package gcs_client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"golang.org/x/time/rate"
)

type Upload struct {
	localBaseDir string
	bucketName   string
	credFile     string
}

// NewUpload Initialize new Upload struct following unified style
func NewUpload(
	localBaseDir, bucketName, credFile string,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *Upload {
	u := &Upload{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		credFile:     credFile,
	}
	// Same log printing logic as other init functions
	logger.Info("NewUpload", slog.String("pre", pre), slog.Any("Upload", *u))
	return u
}

func (u *Upload) UploadFile(
	ctx context.Context,
	objectName string,
	contentLength int64,
	hops string,
	rateLimiter *rate.Limiter,
	reader io.ReadCloser,
	inMemory bool, // New: true=in-memory streaming upload, false=local file upload
	pre string, // Added: log prefix (key tracing field)
	logger *slog.Logger,
) error {

	logger.Info("UploadToGCSbyClient", slog.String("pre", pre), slog.String("objectName", objectName))
	// Check ctx cancellation only before upload starts (avoid invalid upload)
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToGCSbyClient canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// Log to distinguish upload mode (add pre prefix)
	if inMemory {
		logger.Info("Uploading data to GCS (in-memory mode, no local file)",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	} else {
		logger.Info("Uploading file to GCS (disk mode)",
			slog.String("pre", pre),
			slog.String("LocalBaseDir", u.localBaseDir),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
	}

	// Set GCS credentials
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", u.credFile)

	// Create GCS client (pass ctx, support cancel during client creation)
	//ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute) // Avoid hanging
	//defer cancel()
	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Error("Failed to create storage client",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close() // Ensure client closes

	// Get bucket handle
	bucket := client.Bucket(u.bucketName)

	// Initialize GCS Writer (pass outer ctx, don't recreate timeout)
	wc := bucket.Object(objectName).NewWriter(ctx)
	wc.StorageClass = "STANDARD"
	wc.ContentType = "application/octet-stream"
	defer func() {
		// Ensure Writer closes, capture close error
		if err := wc.Close(); err != nil {
			logger.Error("Failed to close GCS writer",
				slog.String("pre", pre),
				slog.Any("err", err))
		}
	}()

	// Mode 1: inMemory=true -> stream upload from passed Reader
	if inMemory {
		if reader == nil {
			err := fmt.Errorf("in-memory mode requires non-nil dataReader")
			logger.Error("In-memory mode invalid", slog.String("pre", pre), slog.Any("err", err))
			return err
		}

		// Keep original blocking io.Copy, wait for copy completion
		if _, err := io.Copy(wc, reader); err != nil {
			logger.Error("Failed to copy in-memory data to bucket",
				slog.String("pre", pre),
				slog.Any("err", err))
			return fmt.Errorf("failed to copy in-memory data to bucket: %w", err)
		}

		logger.Info("In-memory upload success",
			slog.String("pre", pre),
			slog.String("bucketName", u.bucketName),
			slog.String("objectName", objectName))
		return nil
	}

	// Mode 2: inMemory=false -> upload from local file
	localFilePath := filepath.Join(u.localBaseDir, objectName)

	// Open local file
	f, err := os.Open(localFilePath)
	if err != nil {
		logger.Error("Failed to open local file",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath),
			slog.Any("err", err))
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer f.Close()

	// Keep original blocking io.Copy, wait for copy completion
	if _, err := io.Copy(wc, f); err != nil {
		logger.Error("Failed to copy local file to bucket",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath),
			slog.Any("err", err))
		return fmt.Errorf("failed to copy local file to bucket: %w", err)
	}

	logger.Info("Local file upload success",
		slog.String("pre", pre),
		slog.String("localFilePath", localFilePath),
		slog.String("bucketName", u.bucketName),
		slog.String("objectName", objectName))

	return nil
}
