package gcs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
)

type Download struct {
	localBaseDir string
	bucketName   string
	credFile     string
}

func NewDownload(
	localBaseDir, bucketName, credFile string, pre string, logger *slog.Logger,
) *Download {
	d := &Download{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		credFile:     credFile,
	}
	logger.Info("NewDownload", slog.String("pre", pre), slog.Any("Download", *d))
	return d
}

func (d *Download) DownloadFile(
	ctx context.Context,
	filename string,
	newFilename string,
	start int64,
	length int64,
	bs string,
	inMemory bool, // New: true=return Reader without disk write, false=write to disk
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, error) { // Return io.ReadCloser (supports both modes)

	objectName := filename

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("DownloadFromGCSbyClient canceled", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	default:
	}

	// Log to distinguish mode + full/range read
	if inMemory {
		if length <= 0 {
			logger.Info("Reading full file from GCS (in-memory mode, no disk write)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName))
		} else {
			logger.Info("Reading file range from GCS (in-memory mode, no disk write)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.Int64("start_byte", start),
				slog.Int64("length_byte", length))
		}
	} else {
		if length <= 0 {
			logger.Info("Downloading full file from GCS (disk mode)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.String("LocalBaseDir", d.localBaseDir))
		} else {
			logger.Info("Downloading file range from GCS (disk mode)",
				slog.String("pre", pre),
				slog.String("bucketName", d.bucketName),
				slog.String("objectName", objectName),
				slog.String("newFileName", newFilename),
				slog.String("LocalBaseDir", d.localBaseDir),
				slog.Int64("start_byte", start),
				slog.Int64("length_byte", length))
		}
	}

	// Set GCS credentials
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", d.credFile)

	// Create GCS client

	// Create context with timeout
	//ctx_, cancel := context.WithTimeout(ctx, 1*time.Minute)
	//defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create storage client failed: %w", err)
	}

	// Get Bucket and Object
	bucket := client.Bucket(d.bucketName)
	obj := bucket.Object(objectName)

	// Create Reader (full/range read)
	var rc *storage.Reader
	if length <= 0 {
		rc, err = obj.NewReader(ctx)
	} else {
		rc, err = obj.NewRangeReader(ctx, start, length)
	}
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("create object reader failed: %w", err)
	}

	// Mode 1: inMemory=true -> return streaming Reader (no disk write)
	if inMemory {
		return &gcsReaderWrapper{
			Reader: rc,
			//cancel: cancel,
			client: client,
		}, nil
	}

	// Mode 2: inMemory=false -> write to local file
	defer func() {
		rc.Close()
		//cancel()
		client.Close()
	}()

	// Build local file path
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
	if _, err := io.Copy(f, rc); err != nil {
		return nil, fmt.Errorf("copy to local file failed: %w", err)
	}

	// Disk mode returns Reader of local file (convenient for caller)
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

// gcsReaderWrapper Wraps storage.Reader + resource cleanup (for memory mode)
type gcsReaderWrapper struct {
	*storage.Reader
	//cancel context.CancelFunc
	client *storage.Client
}

// Close Close all associated resources (caller must call)
func (w *gcsReaderWrapper) Close() error {
	var errStr []string
	if err := w.Reader.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("reader close failed: %v", err))
	}
	//w.cancel()
	if err := w.client.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("client close failed: %v", err))
	}
	if len(errStr) > 0 {
		return fmt.Errorf(strings.Join(errStr, "; "))
	}
	return nil
}
