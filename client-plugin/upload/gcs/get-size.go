package gcs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/storage"
)

type GetSize struct {
	bucketName string
	credFile   string
}

func NewGetSize(
	bucketName, credFile string,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *GetSize {
	gs := &GetSize{
		bucketName: bucketName,
		credFile:   credFile,
	}
	// Same log printing logic as NewDownload/NewUpload
	logger.Info("NewGetSize", slog.String("pre", pre), slog.Any("GetSize", *gs))
	return gs
}

func (g *GetSize) GetFileSize(ctx context.Context, filename string, pre string, logger *slog.Logger) (int64, error) {

	objectName := filename

	// Listen for ctx cancel signal, terminate early
	select {
	case <-ctx.Done():
		err := fmt.Errorf("get csg file size canceled: %w", ctx.Err())
		logger.Error("GetGCSObjectSize canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return 0, err
	default:
	}

	// Set GCS credential env var
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", g.credFile)

	// Create GCS client (with timeout control)
	ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute) // Avoid hanging
	defer cancel()

	client, err := storage.NewClient(ctx_)
	if err != nil {
		logger.Error("create GCS client failed", slog.String("pre", pre),
			slog.String("bucketName", g.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		return 0, fmt.Errorf("storage.NewClient failed: %w", err)
	}
	defer client.Close() // Ensure client closes, release resources

	// Get Bucket and Object instances
	bucket := client.Bucket(g.bucketName)
	obj := bucket.Object(objectName)

	// Get Object metadata (core: read Size from Attrs)
	attrs, err := obj.Attrs(ctx_)
	if err != nil {
		logger.Error("get GCS Object metadata failed", slog.String("pre", pre),
			slog.String("bucketName", g.bucketName),
			slog.String("objectName", objectName),
			slog.Any("err", err))
		// Distinguish common error types, return friendlier message
		if errors.Is(err, storage.ErrObjectNotExist) {
			return 0, fmt.Errorf("object %s/%s does not exist: %w", g.bucketName, objectName, err)
		}
		return 0, fmt.Errorf("obj.Attrs failed: %w", err)
	}

	// Log result
	logger.Info("successfully get GCS Object size", slog.String("pre", pre),
		slog.String("bucketName", g.bucketName),
		slog.String("objectName", objectName),
		slog.Int64("file_size_bytes", attrs.Size),
		slog.String("file_size_human", formatBytes(attrs.Size))) // Optional: human-readable size

	// Return file size (bytes)
	return attrs.Size, nil
}

// formatBytes Convert bytes to human-readable string (e.g., 1024 -> 1KB, 1048576 -> 1MB)
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
