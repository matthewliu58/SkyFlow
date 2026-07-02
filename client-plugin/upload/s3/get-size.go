package s3

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type GetSize struct {
	bucketName   string // S3 bucket name
	region       string // AWS region
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // Empty = AWS official
	usePathStyle bool
}

// NewGetSize initializes AWS S3 GetSize instance (aligned with GCP NewGetSize)
func NewGetSize(
	bucketName, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string,
	logger *slog.Logger,
) *GetSize {
	gs := &GetSize{
		bucketName:   bucketName,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	// Same logging logic as GCP
	logger.Info("NewGetSize", slog.String("pre", pre), slog.Any("GetSize", *gs))
	return gs
}

// GetFileSize gets AWS S3 object size (aligned with GCP GetFileSize interface)
// Returns: file size (bytes), error
func (g *GetSize) GetFileSize(ctx context.Context, filename string, pre string, logger *slog.Logger) (int64, error) {
	objectName := filename

	// 1. Listen for ctx cancellation signal, terminate early
	select {
	case <-ctx.Done():
		err := fmt.Errorf("get s3 file size canceled: %w", ctx.Err())
		logger.Error("GetS3ObjectSize canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return 0, err
	default:
	}

	// 2. Initialize S3 client (with timeout control)
	s3Client, err := g.initS3Client(ctx)
	if err != nil {
		logger.Error("创建 S3 客户端失败", slog.String("pre", pre),
			slog.String("bucketName", g.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		return 0, fmt.Errorf("create s3 client failed: %w", err)
	}

	// 3. Build HeadObject request (get metadata only, no file content)
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(g.bucketName),
		Key:    aws.String(objectName),
	}

	// 4. Send HeadObject request to get metadata (core: read ContentLength)
	headResp, err := s3Client.HeadObject(ctx, headInput)
	if err != nil {
		logger.Error("获取 S3 Object 元数据失败", slog.String("pre", pre),
			slog.String("bucketName", g.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))

		// Distinguish common error types (aligned with GCP's ErrObjectNotExist)
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			// S3 object not found error code
			if apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey" {
				return 0, fmt.Errorf("object %s/%s not found: %w", g.bucketName, objectName, err)
			}
			// Permission denied error
			if apiErr.ErrorCode() == "AccessDenied" {
				return 0, fmt.Errorf("access denied for object %s/%s: %w", g.bucketName, objectName, err)
			}
		}
		return 0, fmt.Errorf("s3.HeadObject failed: %w", err)
	}

	// 5. Extract file size (ContentLength corresponds to bytes)
	fileSize := headResp.ContentLength

	// 6. Log result (same log fields as GCP)
	logger.Info("Successfully retrieved S3 Object size", slog.String("pre", pre),
		slog.String("bucketName", g.bucketName),
		slog.String("objectName", objectName),
		slog.Int64("file_size_bytes", *fileSize),
		slog.String("file_size_human", formatBytes(*fileSize))) // Reuse GCP's format function

	// 7. Return file size (bytes)
	return *fileSize, nil
}

// initS3Client initializes S3 client (with timeout config)
func (u *GetSize) initS3Client(ctx context.Context) (*s3.Client, error) {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
		},
	}

	// Basic config
	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(u.region),
		config.WithHTTPClient(httpClient),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     u.accessKey,
				SecretAccessKey: u.secretKey,
				Source:          "custom-config",
			}, nil
		})),
	}

	// If Endpoint is set, override (supports all S3-compatible services)
	if u.endpoint != "" {
		endpointResolver := aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           u.endpoint,
					SigningRegion: u.region,
				}, nil
			},
		)
		loadOpts = append(loadOpts, config.WithEndpointResolverWithOptions(endpointResolver))
	}

	// Load config
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}

	// Create client, auto-control PathStyle
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = u.usePathStyle
	}), nil
}

// formatBytes converts bytes to human-readable string
// e.g.: 1024 → 1KB, 1048576 → 1MB
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(bytes)/float64(div),
		"KMGTPE"[exp])
}
