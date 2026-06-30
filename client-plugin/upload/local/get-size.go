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
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *GetSize {
	gs := &GetSize{
		dir: dir,
	}
	// 和其他初始化函数完全一致的日志打印逻辑
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

	// 1. 拼接文件完整路径
	filePath := filepath.Join(g.dir, fileName)

	// 2. 获取文件信息
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		// 记录错误日志，直接返回原错误
		logger.Error("GetLocalFileSize failed",
			slog.String("pre", pre),
			slog.String("filePath", filePath),
			slog.String("error", err.Error()))
		return 0, err
	}

	// 3. 校验是否为文件（排除目录）
	if fileInfo.IsDir() {
		err = os.ErrInvalid // 标记为无效文件（目录）
		logger.Error("GetLocalFileSize failed: path is directory",
			slog.String("pre", pre),
			slog.String("filePath", filePath))
		return 0, err
	}

	// 4. 成功返回文件大小
	logger.Info("GetLocalFileSize success",
		slog.String("pre", pre),
		slog.String("filePath", filePath),
		slog.Int64("fileSize", fileInfo.Size()))
	return fileInfo.Size(), nil
}
