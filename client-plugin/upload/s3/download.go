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
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type Download struct {
	localBaseDir string // Local base directory (used in disk mode)
	bucketName   string // S3 bucket name
	region       string // AWS region
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // Empty = AWS official
	usePathStyle bool
}

// NewDownload initializes AWS S3 Download instance
func NewDownload(
	localBaseDir, bucketName, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string,
	logger *slog.Logger,
) *Download {
	d := &Download{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	logger.Info("NewDownload", slog.String("pre", pre), slog.Any("Download", *d))
	return d
}

// DownloadFile downloads file from AWS S3
func (d *Download) DownloadFile(
	ctx context.Context,
	filename string, // S3 object name
	newFilename string, // Local filename (used in disk mode)
	start int64, // Chunk start byte
	length int64, // Chunk length (<=0 means full download)
	bs string, // Compatible with GCP parameter (reserved)
	inMemory bool, // true=memory mode, false=disk mode
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, error) { // Returns io.ReadCloser (compatible with both modes)
	objectName := filename

	// Check context timeout
	select {
	case <-ctx.Done():
		err := fmt.Errorf("download canceled before start: %w", ctx.Err())
		logger.Error("DownloadFromS3 canceled", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	default:
	}

	// Log mode + full/range read
	if inMemory {
		if length <= 0 {
			logger.Info("Reading full file from S3 (in-memory mode, no disk write)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName))
		} else {
			logger.Info("Reading file range from S3 (in-memory mode, no disk write)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.Int64("start_byte", start),
				slog.Int64("length_byte", length))
		}
	} else {
		if length <= 0 {
			logger.Info("Downloading full file from S3 (disk mode)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.String("LocalBaseDir", d.localBaseDir))
		} else {
			logger.Info("Downloading file range from S3 (disk mode)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.String("newFileName", newFilename),
				slog.String("LocalBaseDir", d.localBaseDir),
				slog.Int64("start_byte", start),
				slog.Int64("length_byte", length))
		}
	}

	// Initialize S3 client
	s3Client, err := d.initS3Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("create s3 client failed: %w", err)
	}

	// Build download request (full/range)
	getInput := &s3.GetObjectInput{
		Bucket: aws.String(d.bucketName),
		Key:    aws.String(objectName),
	}
	// Range download: set Range header
	if length > 0 {
		endByte := start + length - 1
		getInput.Range = aws.String(fmt.Sprintf("bytes=%d-%d", start, endByte))
	}

	// Send download request
	resp, err := s3Client.GetObject(ctx, getInput)
	if err != nil {
		// Handle S3 errors (e.g., file not found, permission denied)
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			logger.Error("S3 GetObject API error",
				slog.String("pre", pre),
				slog.String("code", apiErr.ErrorCode()),
				slog.String("message", apiErr.ErrorMessage()))
		}
		return nil, fmt.Errorf("s3 get object failed: %w", err)
	}

	// Mode 1: inMemory=true → return streaming Reader (no disk write)
	if inMemory {
		return &s3ReaderWrapper{
			ReadCloser: resp.Body, // Fix: struct field is ReadCloser, not Reader
			client:     s3Client,
		}, nil
	}

	// Mode 2: inMemory=false → write to local file
	defer resp.Body.Close()

	// Concatenate local file path
	localFilePath := filepath.Join(d.localBaseDir, newFilename)
	// Create local directory
	if err := os.MkdirAll(d.localBaseDir, 0755); err != nil {
		return nil, fmt.Errorf("create local dir failed: %w", err)
	}

	// Create local file
	f, err := os.Create(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("create local file failed: %w", err)
	}
	defer f.Close()

	// Write to local file
	if _, err := io.Copy(f, resp.Body); err != nil {
		return nil, fmt.Errorf("copy to local file failed: %w", err)
	}

	// Disk mode: return local file Reader
	localFileReader, err := os.Open(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("open local file failed: %w", err)
	}

	// Log result
	if length <= 0 {
		logger.Info("Full file download success (disk mode)",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath))
	} else {
		logger.Info("File range download success (disk mode)",
			slog.String("pre", pre),
			slog.String("objectName", objectName),
			slog.String("localFilePath", localFilePath),
			slog.Int64("start_byte", start),
			slog.Int64("length_byte", length))
	}

	return localFileReader, nil
}

// initS3Client initializes S3 client (with timeout config)
func (u *Download) initS3Client(ctx context.Context) (*s3.Client, error) {
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

// s3ReaderWrapper wraps S3 response body ReadCloser + resource cleanup logic (for memory mode)
type s3ReaderWrapper struct {
	io.ReadCloser            // Correct: resp.Body is io.ReadCloser type
	client        *s3.Client // Keep client reference (for extended cleanup if needed)
}

// Close closes all associated resources (caller must call)
func (w *s3ReaderWrapper) Close() error {
	var errStr []string
	if err := w.ReadCloser.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("reader close failed: %v", err))
	}
	// S3 client doesn't need manual close (SDK manages automatically)
	if len(errStr) > 0 {
		return fmt.Errorf(strings.Join(errStr, "; "))
	}
	return nil
}
