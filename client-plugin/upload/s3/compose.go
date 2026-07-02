package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Core struct
type Compose struct {
	bucket       string // S3 bucket name
	region       string // AWS region
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // Empty = AWS official
	usePathStyle bool
}

// NewCompose initializes AWS S3 Compose instance
func NewCompose(
	bucket, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string,
	logger *slog.Logger,
) *Compose {
	c := &Compose{
		bucket:       bucket,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	logger.Info("NewCompose", slog.String("pre", pre), slog.Any("Compose", *c))
	return c
}

func (c *Compose) ComposeFile(
	ctx context.Context,
	objectName string, // Final composed filename
	parts []string, // Chunk file list
	pre string,
	logger *slog.Logger,
) error {
	// Check context timeout
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled: %w", ctx.Err())
		logger.Error("AWS Compose canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// Initialize S3 client
	s3Client, err := c.initS3Client(ctx)
	if err != nil {
		logger.Error("create S3 client failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("new s3 client failed: %w", err)
	}

	// 1. Single file scenario: copy + delete source file
	if len(parts) == 1 {
		partName := parts[0]
		// Skip if same name
		if partName == objectName {
			logger.Info("single file name matches final name, skip compose",
				slog.String("pre", pre),
				slog.String("object", objectName))
			return nil
		}

		// Copy file
		logger.Info("start copy single file to final location",
			slog.String("pre", pre),
			slog.String("from", partName),
			slog.String("to", objectName))

		copyInput := &s3.CopyObjectInput{
			Bucket:     aws.String(c.bucket),
			CopySource: aws.String(fmt.Sprintf("/%s/%s", c.bucket, partName)),
			Key:        aws.String(objectName),
		}
		_, err := s3Client.CopyObject(ctx, copyInput)
		if err != nil {
			logger.Error("copy single file failed",
				slog.String("pre", pre),
				slog.String("from", partName),
				slog.String("to", objectName),
				slog.Any("err", err))
			return fmt.Errorf("copy single file failed: %w", err)
		}
		logger.Info("copy single file success",
			slog.String("pre", pre),
			slog.String("from", partName),
			slog.String("to", objectName))

		// Delete source file (fault-tolerant: warn only on failure)
		delInput := &s3.DeleteObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(partName),
		}
		if _, delErr := s3Client.DeleteObject(ctx, delInput); delErr != nil {
			var apiErr smithy.APIError
			if errors.As(delErr, &apiErr) {
				logger.Warn("delete single source file failed (copy success)",
					slog.String("pre", pre),
					slog.String("partName", partName),
					slog.String("code", apiErr.ErrorCode()),
					slog.Any("err", delErr))
			} else {
				logger.Warn("delete single source file failed (copy success)",
					slog.String("pre", pre),
					slog.String("partName", partName),
					slog.Any("err", delErr))
			}
		} else {
			logger.Info("delete single source file success",
				slog.String("pre", pre),
				slog.String("partName", partName))
		}

		logger.Info("single file process completed",
			slog.String("pre", pre),
			slog.String("finalObject", objectName))
		return nil
	}

	current := parts
	level := 0
	var tempObjects []string // Track temporary composed files

	// Tree composition: merge up to 1000 chunks per round (S3 max per request)
	for len(current) > 1 {
		var next []string

		for i := 0; i < len(current); i += 1000 {
			end := i + 1000
			if end > len(current) {
				end = len(current)
			}
			group := current[i:end]
			tmpObjectName := fmt.Sprintf("%s.compose.%d.%d", objectName, level, i)

			// Merge current group of chunks to temp file
			if err := c.mergePartsToTempFile(ctx, s3Client, group, tmpObjectName, pre, logger); err != nil {
				logger.Error("merge temp object failed",
					slog.String("pre", pre),
					slog.String("tmpObjectName", tmpObjectName),
					slog.Int("level", level),
					slog.Any("group", group),
					slog.Any("err", err))
				return fmt.Errorf("merge temp object %s failed: %w", tmpObjectName, err)
			}

			next = append(next, tmpObjectName)
			tempObjects = append(tempObjects, tmpObjectName)
			logger.Info("merge temp object success",
				slog.String("pre", pre),
				slog.String("name", tmpObjectName),
				slog.Int("level", level),
				slog.Any("from", group))
		}

		current = next
		level++
	}

	// 3. Final composition: temp file → final file
	logger.Info("start finalize object",
		slog.String("pre", pre),
		slog.String("from", current[0]),
		slog.String("to", objectName))

	// Copy temp file to final location
	finalCopyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(c.bucket),
		CopySource: aws.String(fmt.Sprintf("/%s/%s", c.bucket, current[0])),
		Key:        aws.String(objectName),
	}
	_, err = s3Client.CopyObject(ctx, finalCopyInput)
	if err != nil {
		logger.Error("finalize object copy failed",
			slog.String("pre", pre),
			slog.String("from", current[0]),
			slog.String("to", objectName),
			slog.Any("err", err))
		return fmt.Errorf("copy temp to final failed: %w", err)
	}

	// 4. Cleanup temp files and chunks
	// 4.1 Delete intermediate temp files
	for _, tmp := range tempObjects {
		if tmp != current[0] {
			delInput := &s3.DeleteObjectInput{
				Bucket: aws.String(c.bucket),
				Key:    aws.String(tmp),
			}
			if _, delErr := s3Client.DeleteObject(ctx, delInput); delErr != nil {
				var apiErr smithy.APIError
				if errors.As(delErr, &apiErr) {
					logger.Warn("delete temp object failed",
						slog.String("pre", pre),
						slog.String("tmp", tmp),
						slog.String("code", apiErr.ErrorCode()),
						slog.Any("err", delErr))
				} else {
					logger.Warn("delete temp object failed",
						slog.String("pre", pre),
						slog.String("tmp", tmp),
						slog.Any("err", delErr))
				}
			}
		}
	}

	// 4.2 Delete final temp file
	delFinalTempInput := &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(current[0]),
	}
	if _, delErr := s3Client.DeleteObject(ctx, delFinalTempInput); delErr != nil {
		logger.Warn("delete final temp object failed",
			slog.String("pre", pre),
			slog.String("tmp", current[0]),
			slog.Any("err", delErr))
	}

	// 4.3 Delete original chunk files
	for _, p := range parts {
		delInput := &s3.DeleteObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(p),
		}
		if _, delErr := s3Client.DeleteObject(ctx, delInput); delErr != nil {
			var apiErr smithy.APIError
			if errors.As(delErr, &apiErr) {
				logger.Warn("delete part object failed",
					slog.String("pre", pre),
					slog.String("part", p),
					slog.String("code", apiErr.ErrorCode()),
					slog.Any("err", delErr))
			} else {
				logger.Warn("delete part object failed",
					slog.String("pre", pre),
					slog.String("part", p),
					slog.Any("err", delErr))
			}
		}
	}

	logger.Info("multi file compose success",
		slog.String("pre", pre),
		slog.String("finalObject", objectName))
	return nil
}

// initS3Client initializes S3 client (with timeout config)
func (u *Compose) initS3Client(ctx context.Context) (*s3.Client, error) {
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

// mergePartsToTempFile merges chunks to temp file (S3-compatible compose logic)
func (c *Compose) mergePartsToTempFile(
	ctx context.Context,
	client *s3.Client,
	parts []string,
	tempName string,
	pre string,
	logger *slog.Logger,
) error {
	// Create temp local file
	tempFile, err := os.CreateTemp("", "s3-compose-*")
	if err != nil {
		return fmt.Errorf("create temp local file failed: %w", err)
	}
	tempFilePath := tempFile.Name()
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFilePath) // Clean up local temp file
	}()

	// Download and merge all chunks
	for _, part := range parts {
		select {
		case <-ctx.Done():
			return fmt.Errorf("merge canceled: %w", ctx.Err())
		default:
		}

		// Download chunk
		getInput := &s3.GetObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(part),
		}
		resp, err := client.GetObject(ctx, getInput)
		if err != nil {
			return fmt.Errorf("download part %s failed: %w", part, err)
		}

		// Write to temp file
		_, err = io.Copy(tempFile, resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return fmt.Errorf("write part %s to temp file failed: %w", part, err)
		}

		logger.Debug("merge part to temp file",
			slog.String("pre", pre),
			slog.String("part", part),
			slog.String("temp", tempName))
	}

	// Reset file pointer to beginning
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek temp file failed: %w", err)
	}

	// Upload merged temp file to S3
	putInput := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(tempName),
		Body:   tempFile,
	}
	_, err = client.PutObject(ctx, putInput)
	if err != nil {
		return fmt.Errorf("upload temp file %s to s3 failed: %w", tempName, err)
	}

	return nil
}
