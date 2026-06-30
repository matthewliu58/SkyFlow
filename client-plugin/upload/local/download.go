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
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *Download {
	d := &Download{
		localDir: localDir,
	}
	// 和其他初始化函数完全一致的日志打印逻辑
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
	inMemory bool, // 新增参数放在最后，不影响原有调用
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

	// 1. 拼接本地文件完整路径
	localFilePath := filepath.Join(localDir, filename)
	localFilePath = filepath.Clean(localFilePath) // 标准化路径（处理多斜杠/相对路径）

	logger.Info("Start range reading of local file",
		slog.String("pre", pre),
		slog.String("localFilePath", localFilePath),
		slog.Int64("start", start),
		slog.Int64("length", length))

	// 2. 基础校验：文件是否存在/是否为文件
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

	// 3. 处理读取范围（兼容完整读取/范围读取）
	var actualStart, actualLength int64
	if length <= 0 {
		// 读取整个文件
		actualStart = 0
		actualLength = fileTotalSize
		logger.Info("Read entire file",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath),
			slog.Int64("fileTotalSize", fileTotalSize))
	} else {
		// 范围读取：校验起始位置和长度合法性
		if start < 0 {
			return nil, fmt.Errorf("起始位置start不能为负: %d", start)
		}
		if start >= fileTotalSize {
			return nil, fmt.Errorf("起始位置%d超出文件总大小%d", start, fileTotalSize)
		}

		actualStart = start
		// 修正长度：如果start+length超出文件大小，只读取到文件末尾
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

	// 4. 打开文件（只读模式）
	file, err := os.Open(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// 5. 定位到读取起始位置
	if _, err := file.Seek(actualStart, io.SeekStart); err != nil {
		file.Close() // 失败时关闭文件句柄
		return nil, fmt.Errorf("failed to seek to start position %d: %w", actualStart, err)
	}

	// 6. 封装Reader：限制读取长度 + 实现ReadCloser（统一资源释放）
	// LimitReader限制读取长度，MultiReader确保读取完指定长度后返回EOF
	limitedReader := io.LimitReader(file, actualLength)
	// 封装为ReadCloser，Close时关闭底层文件句柄
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

// localFileReaderWrapper 封装本地文件Reader，实现ReadCloser接口
// 核心：统一资源释放（Close时关闭底层文件句柄）
type localFileReaderWrapper struct {
	reader io.Reader    // 限制长度后的Reader
	file   *os.File     // 底层文件句柄（用于Close）
	pre    string       // 日志前缀
	logger *slog.Logger // 日志对象
	path   string       // 文件路径（用于日志）
	closed bool         // 是否已关闭（避免重复关闭）
}

// Read 实现io.Reader接口：流式读取指定范围的内容
func (w *localFileReaderWrapper) Read(p []byte) (n int, err error) {
	if w.closed {
		return 0, fmt.Errorf("reader已关闭，无法读取")
	}
	return w.reader.Read(p)
}

// Close 实现io.ReadCloser接口：释放文件句柄
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
