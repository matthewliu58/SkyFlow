package local

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

type GetSize struct {
	dir string
}

func NewGetSize(
	dir string,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *GetSize {
	gs := &GetSize{
		dir: dir,
	}
	// Same log printing logic as other init functions
	logger.Info("NewGetSize", slog.String("pre", pre), slog.Any("GetSize", *gs))
	return gs
}

func (g *GetSize) GetFileSize(ctx context.Context, filename string, pre string, logger *slog.Logger) (int64, error) {

	fileName := filename

	select {
	case <-ctx.Done():
		err := fmt.Errorf("get local file size canceled: %w", ctx.Err())
		logger.Error("GetLocalFileSize canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return 0, err
	default:
	}

	// 1. Build full file path
	filePath := filepath.Join(g.dir, fileName)

	// 2. Get file info
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		// Log error and return original error
		logger.Error("GetLocalFileSize failed",
			slog.String("pre", pre),
			slog.String("filePath", filePath),
			slog.String("error", err.Error()))
		return 0, err
	}

	// 3. Validate is file (exclude directory)
	if fileInfo.IsDir() {
		err = os.ErrInvalid // Mark as invalid file (directory)
		logger.Error("GetLocalFileSize failed: path is directory",
			slog.String("pre", pre),
			slog.String("filePath", filePath))
		return 0, err
	}

	// 4. Return file size on success
	logger.Info("GetLocalFileSize success",
		slog.String("pre", pre),
		slog.String("filePath", filePath),
		slog.Int64("fileSize", fileInfo.Size()))
	return fileInfo.Size(), nil
}
