package s3_client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Upload struct {
	localBaseDir string // Local base directory (used in disk mode)
	bucketName   string // S3 bucket name
	region       string // AWS region
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // Empty = AWS official
	usePathStyle bool
}

// NewUpload initializes AWS S3 Upload instance (aligned with GCP style)
func NewUpload(
	localBaseDir, bucketName, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string, // Log prefix (same as GCP)
	logger *slog.Logger, // Logger instance
) *Upload {
	u := &Upload{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	// Same logging logic as GCP
	logger.Info("NewUpload", slog.String("pre", pre), slog.Any("Upload", *u))
	return u
}

func (u *Upload) UploadFile(
	ctx context.Context,
	objectName string,
	contentLength int64,
	hops string, // Compatible with GCP parameter (reserved, not used in AWS client mode)
	rateLimiter *rate.Limiter, // Compatible with GCP parameter (enable if rate limiting needed)
	reader io.ReadCloser,
	inMemory bool, // true=memory mode, false=disk mode
	pre string, // Log prefix (key tracing field)
	logger *slog.Logger,
) error {

	logger.Info("UploadToS3byClient", slog.String("pre", pre))

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToS3byClient canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	if inMemory {
		logger.Info("Uploading data to S3 (in-memory mode, no local file)",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	} else {
		logger.Info("Uploading file to S3 (disk mode)",
			slog.String("pre", pre),
			slog.String("LocalBaseDir", u.localBaseDir),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	}

	s3Client, err := u.initS3Client(ctx)
	if err != nil {
		logger.Error("Failed to create S3 client",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	var uploadBody io.Reader
	//var localFile *os.File // Local file handle for disk mode

	// inMemory=true → memory streaming upload
	if inMemory {
		if reader == nil {
			err := fmt.Errorf("in-memory mode requires non-nil dataReader")
			logger.Error("In-memory mode invalid", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
		defer reader.Close() // Close input Reader in memory mode

		// Optional: enable rate limiting (to keep consistent with GCP)
		if rateLimiter != nil {
			//uploadBody = NewRateLimitedReader(ctx, reader, rateLimiter)
		} else {
			uploadBody = reader
		}

		// inMemory=false → local file upload
	} else {
		localFilePath := filepath.Join(u.localBaseDir, objectName)
		// Open local file (aligned with GCP error log format)
		f, err := os.Open(localFilePath)
		if err != nil {
			logger.Error("Failed to open local file",
				slog.String("pre", pre),
				slog.String("localFilePath", localFilePath),
				slog.Any("err", err))
			return fmt.Errorf("failed to open local file: %w", err)
		}
		//localFile = f
		defer f.Close() // Ensure local file is closed

		// Optional: enable rate limiting
		if rateLimiter != nil {
			//uploadBody = NewRateLimitedReader(ctx, f, rateLimiter)
		} else {
			uploadBody = f
		}
	}

	// Build S3 upload request
	putInput := &s3.PutObjectInput{
		Bucket:        aws.String(u.bucketName),
		Key:           aws.String(objectName),
		Body:          uploadBody,
		ContentType:   aws.String("application/octet-stream"), // Same as GCP
		ContentLength: aws.Int64(contentLength),
		//StorageClass: types.StorageClassStandard,
	}

	// Execute upload (pass outer ctx, support mid-upload cancellation)
	_, err = s3Client.PutObject(ctx, putInput)
	if err != nil {
		logger.Error("Failed to upload to S3 bucket",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		return fmt.Errorf("failed to upload to S3 bucket: %w", err)
	}

	if inMemory {
		logger.Info("In-memory upload success",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	} else {
		logger.Info("Local file upload success",
			slog.String("pre", pre),
			slog.String("localFilePath", filepath.Join(u.localBaseDir, objectName)),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	}

	return nil
}

// initS3Client initializes S3 client (with timeout config)
func (u *Upload) initS3Client(ctx context.Context) (*s3.Client, error) {
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
