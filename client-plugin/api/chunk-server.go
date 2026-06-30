package api

import (
	"github.com/gin-gonic/gin"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"rigel-client/util"
)

// Core header constants
const (
	HeaderChunkName  = "X-Chunk-Name"  // Custom name for a single chunk
	HeaderChunkNames = "X-Chunk-Names" // Comma-separated chunk name list (merge order)
)

// ChunkMergeConfig chunk merge configuration (adapted to sender-specified rules)
type ChunkMergeConfig struct {
	BaseDir       string   // Chunk storage directory
	FinalFileName string   // Final merged file name
	ChunkNames    []string // Sender-specified chunk name list (in merge order)
	DeleteChunks  bool     // Whether to delete chunks after merge
}

// ChunkUploadHandler chunk upload handler (receives sender-named chunks)
func ChunkUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)
		logger.Info("ChunkUploadHandler start", slog.String("pre", pre))

		// 1. Get header parameters
		finalFileName := c.GetHeader(util.HeaderFileName)
		chunkName := c.GetHeader(HeaderChunkName)
		if finalFileName == "" || chunkName == "" {
			logger.Error("ChunkUploadHandler missing header", slog.String("pre", pre),
				slog.String("finalFileName", finalFileName),
				slog.String("chunkName", chunkName))
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Missing required headers: " + util.HeaderFileName + "/" + HeaderChunkName,
			})
			return
		}

		// 2. Get uploaded chunk file
		file, _, err := c.Request.FormFile("file")
		if err != nil {
			logger.Error("ChunkUploadHandler get chunk file failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Get chunk file failed: " + err.Error()})
			return
		}
		defer file.Close()

		// 3. Ensure local directory exists
		if err := os.MkdirAll(LocalBaseDir, 0755); err != nil {
			logger.Error("ChunkUploadHandler create base dir failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Create local dir failed: " + err.Error()})
			return
		}

		// 4. Generate chunk save path (using sender-specified chunk name)
		chunkPath := filepath.Join(LocalBaseDir, chunkName)

		// 5. Save chunk file
		if err := SaveFileChunk(file, chunkPath, pre, logger); err != nil {
			logger.Error("ChunkUploadHandler save chunk failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Save chunk failed: " + err.Error()})
			return
		}

		// 6. Return success response
		logger.Info("ChunkUploadHandler success", slog.String("pre", pre),
			slog.String("finalFileName", finalFileName),
			slog.String("chunkName", chunkName),
			slog.String("chunkPath", chunkPath))
		c.JSON(http.StatusOK, gin.H{
			"code":       200,
			"message":    "chunk upload success",
			"final_file": finalFileName,
			"chunk_name": chunkName,
			"save_path":  chunkPath,
		})
	}
}

// ChunkMergeHandler chunk merge handler (merge in sender-specified order)
func ChunkMergeHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)
		logger.Info("ChunkMergeHandler start", slog.String("pre", pre))

		var req util.ChunkMergeRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			logger.Error("ChunkMergeHandler bind json failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Bind json failed: " + err.Error()})
			return
		}

		// 1. Get header parameters
		finalFileName := c.GetHeader(util.HeaderFileName)
		if finalFileName == "" || len(req.ChunkNames) <= 0 {
			logger.Error("ChunkMergeHandler missing header", slog.String("pre", pre), slog.Any("req", req))
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Missing required headers: " + util.HeaderFileName + "/" + HeaderChunkNames,
			})
			return
		}

		// 2. Parse chunk name list (in sender-specified order)
		chunkNames := req.ChunkNames
		if len(chunkNames) == 0 {
			logger.Error("ChunkMergeHandler empty chunk list", slog.String("pre", pre))
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid " + HeaderChunkNames + ": empty chunk list"})
			return
		}

		// 3. Whether to delete chunks after merge
		deleteChunks := req.DeleteChunks

		// 4. Build merge config
		mergeCfg := ChunkMergeConfig{
			BaseDir:       LocalBaseDir,
			FinalFileName: finalFileName,
			ChunkNames:    chunkNames,
			DeleteChunks:  deleteChunks,
		}

		// 5. Execute chunk merge
		if err := MergeFileChunks(mergeCfg, pre, logger); err != nil {
			logger.Error("ChunkMergeHandler merge failed", slog.String("pre", pre), slog.Any("err", err))
			c.JSON(http.StatusInternalServerError, gin.H{
				"code":  500,
				"error": "Merge chunks failed: " + err.Error(),
			})
			return
		}

		// 6. Return success response
		finalPath := filepath.Join(LocalBaseDir, finalFileName)
		logger.Info("ChunkMergeHandler success", slog.String("pre", pre),
			slog.String("finalFileName", finalFileName),
			slog.String("finalPath", finalPath),
			slog.Any("mergedChunks", chunkNames))
		c.JSON(http.StatusOK, gin.H{
			"code":          200,
			"message":       "file merge success (by sender order)",
			"final_file":    finalFileName,
			"final_path":    finalPath,
			"merged_chunks": chunkNames,
		})
	}
}

// SaveFileChunk saves a single chunk file (receives sender-named chunk)
func SaveFileChunk(chunkFile io.Reader, chunkPath string, pre string, logger *slog.Logger) error {
	// Create chunk directory if not exists
	chunkDir := filepath.Dir(chunkPath)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		logger.Error("SaveFileChunk create dir failed", slog.String("pre", pre),
			slog.String("chunkDir", chunkDir), slog.Any("err", err))
		return err
	}

	// Create chunk file and write content
	chunkOutFile, err := os.Create(chunkPath)
	if err != nil {
		logger.Error("SaveFileChunk create chunk file failed", slog.String("pre", pre),
			slog.String("chunkPath", chunkPath), slog.Any("err", err))
		return err
	}
	defer chunkOutFile.Close()

	// Stream write (supports large files)
	if _, err = io.Copy(chunkOutFile, chunkFile); err != nil {
		logger.Error("SaveFileChunk write chunk failed", slog.String("pre", pre),
			slog.String("chunkPath", chunkPath), slog.Any("err", err))
		return err
	}

	logger.Info("SaveFileChunk success", slog.String("pre", pre), slog.String("chunkPath", chunkPath))
	return nil
}

// MergeFileChunks merges chunks in sender-specified order (fixes single chunk overwrite, simplifies rename)
func MergeFileChunks(cfg ChunkMergeConfig, pre string, logger *slog.Logger) error {
	// 1. Validate parameters
	if cfg.BaseDir == "" || cfg.FinalFileName == "" || len(cfg.ChunkNames) == 0 {
		logger.Error("MergeFileChunks invalid config", slog.String("pre", pre), slog.Any("cfg", cfg))
		return os.ErrInvalid
	}

	// 2. Build final file path
	finalPath := filepath.Join(cfg.BaseDir, cfg.FinalFileName)

	// 3. Handle single chunk special case (core fix)
	if len(cfg.ChunkNames) == 1 {
		chunkName := cfg.ChunkNames[0]
		chunkPath := filepath.Join(cfg.BaseDir, chunkName)

		// Check if chunk exists
		if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
			logger.Error("MergeFileChunks chunk not exist", slog.String("pre", pre),
				slog.Int("mergeOrder", 0),
				slog.String("chunkName", chunkName),
				slog.String("chunkPath", chunkPath))
			return err
		}

		// If chunk name matches final file name → no copy needed, return directly
		if chunkName == cfg.FinalFileName {
			logger.Info("MergeFileChunks single chunk match final name, skip merge",
				slog.String("pre", pre), slog.String("finalPath", finalPath))
			return nil
		}

		// If chunk name differs from final file name → rename directly (only warn on failure, no fallback to copy)
		if err := os.Rename(chunkPath, finalPath); err != nil {
			logger.Error("MergeFileChunks rename single chunk failed",
				slog.String("pre", pre),
				slog.String("from", chunkPath),
				slog.String("to", finalPath),
				slog.Any("err", err))
			return err // Rename failed, return error directly, no further processing
		}

		// Rename succeeded and delete chunks configured → no extra deletion needed (original file no longer exists after rename)
		logger.Info("MergeFileChunks rename single chunk success",
			slog.String("pre", pre), slog.String("from", chunkPath), slog.String("to", finalPath))
		logger.Info("MergeFileChunks success (single chunk)", slog.String("pre", pre), slog.String("finalPath", finalPath))
		return nil
	}

	// 4. Multi-chunk scenario (keep existing logic)
	finalFile, err := os.Create(finalPath)
	if err != nil {
		logger.Error("MergeFileChunks create final file failed", slog.String("pre", pre),
			slog.String("finalPath", finalPath), slog.Any("err", err))
		return err
	}
	defer finalFile.Close()

	for idx, chunkName := range cfg.ChunkNames {
		chunkPath := filepath.Join(cfg.BaseDir, chunkName)

		if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
			logger.Error("MergeFileChunks chunk not exist", slog.String("pre", pre),
				slog.Int("mergeOrder", idx),
				slog.String("chunkName", chunkName),
				slog.String("chunkPath", chunkPath))
			return err
		}

		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			logger.Error("MergeFileChunks open chunk failed", slog.String("pre", pre),
				slog.Int("mergeOrder", idx),
				slog.String("chunkName", chunkName),
				slog.Any("err", err))
			return err
		}

		if _, err := io.Copy(finalFile, chunkFile); err != nil {
			chunkFile.Close()
			logger.Error("MergeFileChunks copy chunk failed", slog.String("pre", pre),
				slog.Int("mergeOrder", idx),
				slog.String("chunkName", chunkName),
				slog.Any("err", err))
			return err
		}
		chunkFile.Close()

		if cfg.DeleteChunks {
			if err := os.Remove(chunkPath); err != nil {
				logger.Warn("MergeFileChunks delete chunk failed", slog.String("pre", pre),
					slog.String("chunkName", chunkName), slog.Any("err", err))
			}
		}

		logger.Info("MergeFileChunks processed chunk", slog.String("pre", pre),
			slog.Int("mergeOrder", idx),
			slog.String("chunkName", chunkName))
	}

	logger.Info("MergeFileChunks success", slog.String("pre", pre), slog.String("finalPath", finalPath))
	return nil
}
