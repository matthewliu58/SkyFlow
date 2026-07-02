package remote_client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/util"
	"time"

	"golang.org/x/time/rate"
)

type Upload struct {
	serverURL    string // API address (e.g., http://127.0.0.1:8080/api/v1/chunk/upload)
	localBaseDir string // Chunk file directory
}

// NewUpload Initialize new Upload struct following unified style
func NewUpload(
	serverURL, localBaseDir string,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *Upload {
	u := &Upload{
		serverURL:    serverURL,
		localBaseDir: localBaseDir,
	}
	// Same log printing logic as other init functions
	logger.Info("NewUpload", slog.String("pre", pre), slog.Any("Upload", *u))
	return u
}

// UploadFileChunk Upload single chunk file to ChunkUploadHandler API (supports local file/in-memory streaming dual mode)
// Core change: add inMemory and dataReader params, keep rest unchanged
// Parameter description (original param order unchanged, 2 new params added at the end):
//
//	req: Chunk upload request params
//	pre: Log prefix
//	logger: Log object
//	inMemory: NEW! true=in-memory streaming upload (read from dataReader), false=local file upload (original logic)
//	dataReader: NEW! Data source Reader when inMemory=true (e.g., SSH/GCS Reader)
func (u *Upload) UploadFile(
	ctx context.Context,
	objectName string,
	contentLength int64,
	hops string,
	rateLimiter *rate.Limiter,
	reader io.ReadCloser,
	inMemory bool, // New: in-memory mode switch
	pre string,
	logger *slog.Logger,
) error {

	logger.Info("UploadFileChunk", slog.String("pre", pre), slog.Bool("inMemory", inMemory))

	finalFileName := objectName // 最终合并后的文件名（对应 X-File-Name）
	chunkName := objectName     // 当前分片名称（对应 X-Chunk-Name）
	dataReader := reader

	// Check ctx cancellation only before upload starts (avoid invalid upload)
	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToGCSbyClient canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	// 2. Mode detection: inMemory=true -> in-memory streaming; false -> local file upload (original logic)
	var fileReader io.Reader
	var err error

	if inMemory {
		// In-memory mode: use passed reader directly
		if dataReader == nil {
			return fmt.Errorf("dataReader cannot be nil in in-memory mode")
		}
		fileReader = dataReader
		logger.Info("Upload chunk using in-memory streaming", slog.String("pre", pre), slog.String("ChunkName", chunkName))
	} else {
		// Local file mode: preserve original logic completely
		// 2.1 Build full chunk file path (handle Linux path separator)
		chunkFilePath := filepath.Join(u.localBaseDir, chunkName)
		chunkFilePath = filepath.Clean(chunkFilePath)

		// 2.2 Validate LocalBaseDir (create if not exists)
		if err := os.MkdirAll(u.localBaseDir, 0755); err != nil {
			return fmt.Errorf("failed to create chunk storage directory %s: %w", u.localBaseDir, err)
		}

		// 2.3 Validate chunk file exists and is readable
		fileInfo, err := os.Stat(chunkFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("chunk file does not exist: %s", chunkFilePath)
			}
			return fmt.Errorf("failed to get chunk file info: %w", err)
		}
		if fileInfo.IsDir() {
			return fmt.Errorf("%s is a directory, not a chunk file", chunkFilePath)
		}
		if fileInfo.Size() == 0 {
			return fmt.Errorf("chunk file %s is empty", chunkFilePath)
		}

		// 2.4 Open local chunk file (read-only mode)
		file, err := os.Open(chunkFilePath)
		if err != nil {
			return fmt.Errorf("failed to open chunk file %s (check file permissions): %w", chunkFilePath, err)
		}
		defer file.Close() // Close file in local mode
		fileReader = file

		logger.Info("Upload chunk using local file", slog.String("pre", pre), slog.String("chunkFilePath", chunkFilePath))
	}

	// 3. Build multipart/form-data request body (support both modes)
	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)

	// 3.1 Add file field (field name must be "file", match server)
	fileWriter, err := bodyWriter.CreateFormFile("file", chunkName)
	if err != nil {
		return fmt.Errorf("failed to create file form field: %w", err)
	}

	// 3.2 Stream write file content (shared by both modes, avoid loading large files to memory)
	if _, err := io.Copy(fileWriter, fileReader); err != nil {
		return fmt.Errorf("failed to write file content: %w", err)
	}

	// 3.3 Close multipart writer (generate end boundary)
	if err := bodyWriter.Close(); err != nil {
		return fmt.Errorf("failed to close request body writer: %w", err)
	}

	// 4. Build HTTP POST request (preserve original logic)
	httpReq, err := http.NewRequest("POST", u.serverURL, bodyBuf)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// 5. Set request headers (match server requirements, preserve original logic)
	httpReq.Header.Set("Content-Type", bodyWriter.FormDataContentType())
	httpReq.Header.Set(util.HeaderFileName, finalFileName)
	httpReq.Header.Set(util.HeaderChunkName, chunkName)

	// 6. Configure HTTP client (adapt to Linux keep-alive/timeout, preserve original logic)
	client := &http.Client{Timeout: 1 * time.Minute}

	// 7. Send request (preserve original logic)
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request to %s: %w", u.serverURL, err)
	}

	// 8. Validate response status code (preserve original logic, enhanced error troubleshooting)
	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte("failed to read error response")
		}
		resp.Body.Close() // Must close response body
		return fmt.Errorf("API returned error: status code %d, content: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
