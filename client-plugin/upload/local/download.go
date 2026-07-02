package local

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

type Download struct {
	localDir string
}

func NewDownload(
	localDir string,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *Download {
	d := &Download{
		localDir: localDir,
	}
	// Same log printing logic as other init functions
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
	inMemory bool, // New parameter at the end, doesn't affect existing calls
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, error) {

	inMemory = true
	localDir := d.localDir
	//filename := d.filename
	//start := d.start
	//length := d.length

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("LocalReadRangeChunk canceled", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	default:
	}

	// 1. Build full local file path
	localFilePath := filepath.Join(localDir, filename)
	localFilePath = filepath.Clean(localFilePath) // Normalize path (handle multiple slashes/relative paths)

	logger.Info("Start range reading of local file",
		slog.String("pre", pre),
		slog.String("localFilePath", localFilePath),
		slog.Int64("start", start),
		slog.Int64("length", length))

	// 2. Basic validation: file existence/is file
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file does not exist: %s", localFilePath)
		}
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	if fileInfo.IsDir() {
		return nil, fmt.Errorf("%s is a directory, not a file", localFilePath)
	}
	fileTotalSize := fileInfo.Size()
	if fileTotalSize == 0 {
		return nil, fmt.Errorf("file %s is empty", localFilePath)
	}

	// 3. Handle read range (support full read/range read)
	var actualStart, actualLength int64
	if length <= 0 {
		// Read entire file
		actualStart = 0
		actualLength = fileTotalSize
		logger.Info("Read entire file",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath),
			slog.Int64("fileTotalSize", fileTotalSize))
	} else {
		// Range read: validate start position and length
		if start < 0 {
			return nil, fmt.Errorf("start position cannot be negative: %d", start)
		}
		if start >= fileTotalSize {
			return nil, fmt.Errorf("start position %d exceeds file size %d", start, fileTotalSize)
		}

		actualStart = start
		// Adjust length: if start+length exceeds file size, read only to end
		if start+length > fileTotalSize {
			actualLength = fileTotalSize - start
			logger.Warn("Read length exceeds file size, automatically truncated",
				slog.String("pre", pre),
				slog.Int64("requestedLength", length),
				slog.Int64("actualLength", actualLength),
				slog.Int64("fileTotalSize", fileTotalSize))
		} else {
			actualLength = length
		}
		logger.Info("Read specified range of the file",
			slog.String("pre", pre),
			slog.Int64("actualStart", actualStart),
			slog.Int64("actualLength", actualLength))
	}

	// 4. Open file (read-only mode)
	file, err := os.Open(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// 5. Seek to start position
	if _, err := file.Seek(actualStart, io.SeekStart); err != nil {
		file.Close() // Close file handle on failure
		return nil, fmt.Errorf("failed to seek to start position %d: %w", actualStart, err)
	}

	// 6. Wrap Reader: limit read length + implement ReadCloser (unified resource release)
	// LimitReader limits read length, MultiReader ensures EOF after specified length
	limitedReader := io.LimitReader(file, actualLength)
	// Wrap as ReadCloser, close underlying file handle on Close
	readerWrapper := &localFileReaderWrapper{
		reader: limitedReader,
		file:   file,
		pre:    pre,
		logger: logger,
		path:   localFilePath,
	}

	logger.Info("Local file reader created successfully",
		slog.String("pre", pre),
		slog.String("localFilePath", localFilePath),
		slog.Int64("readStart", actualStart),
		slog.Int64("readLength", actualLength))

	return readerWrapper, nil
}

// localFileReaderWrapper Wraps local file Reader, implements io.ReadCloser interface
// Core: unified resource release (close underlying file handle on Close)
type localFileReaderWrapper struct {
	reader io.Reader    // Length-limited Reader
	file   *os.File     // Underlying file handle (for Close)
	pre    string       // Log prefix
	logger *slog.Logger // Log object
	path   string       // File path (for logging)
	closed bool         // Whether closed (avoid double close)
}

// Read Implements io.Reader interface: stream read specified range
func (w *localFileReaderWrapper) Read(p []byte) (n int, err error) {
	if w.closed {
		return 0, fmt.Errorf("reader is closed, cannot read")
	}
	return w.reader.Read(p)
}

// Close Implements io.ReadCloser interface: release file handle
func (w *localFileReaderWrapper) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.file.Close(); err != nil {
		w.logger.Error("Failed to close local file reader",
			slog.String("pre", w.pre),
			slog.String("filePath", w.path),
			slog.Any("err", err))
		return fmt.Errorf("failed to close file: %w", err)
	}
	w.logger.Info("Local file reader closed successfully",
		slog.String("pre", w.pre),
		slog.String("filePath", w.path))
	return nil
}
