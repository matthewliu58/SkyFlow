package upload

import (
	"fmt"
	"log/slog"
	"rigel-client/upload/base"
	"rigel-client/upload/split"
	"time"

	"golang.org/x/time/rate"
)

func RedirectImp(fo base.FileOperateInterfaces, task ChunkTask, hops string,
	rateLimiter *rate.Limiter, inMemory bool, pre string, logger *slog.Logger) error {

	//todo internally trigger re-routing

	logger.Info("UploadRedirectImp", slog.String("pre", pre), slog.Any("task", task))

	// First get the basic info of current chunk (avoid null pointer)
	chunkVal, ok := task.Chunks.Get(task.Index)
	if !ok {
		err := fmt.Errorf("chunk index %s not found in Chunks map", task.Index)
		logger.Error("get chunk failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	chunk, _ := chunkVal.(*split.ChunkState)

	startTime := time.Now()
	chunk.LastSend = startTime
	chunk.Acked = int(ChunkStatusTransferring) // 1=transferring
	task.Chunks.Set(task.Index, chunk)
	logger.Info("set chunk initial state", slog.String("pre", pre), slog.String("index", task.Index),
		slog.Int("acked", 1))

	// Define defer function: set Acked to failure on error (fallback)
	var finalErr error
	defer func() {
		if finalErr != nil {
			chunk.LastSend = startTime
			chunk.Acked = int(ChunkStatusTransferFailed)
			task.Chunks.Set(task.Index, chunk)
			logger.Error("chunk transfer failed, set acked=0", slog.String("pre", pre),
				slog.String("index", task.Index), slog.Any("err", finalErr))
		}
	}()

	// Get Reader (read source file)
	ctx := task.Ctx
	// Core fix 1: check if ctx is canceled first, avoid invalid operations
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before get reader: %w", ctx.Err())
		logger.Error("UploadRedirectImp canceled", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	upload := task.Upload
	start := chunk.Offset
	length := chunk.Size
	reader, err := GetTransferReader(ctx, fo, upload, start, length, task.ObjectName, inMemory, pre, logger)
	if err != nil {
		return err
	}
	defer func() {
		if reader != nil {
			_ = reader.Close() // Ensure Reader is closed regardless of success/failure
		}
	}()

	// Upload to target
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before upload: %w", ctx.Err())
		logger.Error("UploadRedirectImp canceled", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	if fo.UploadFile == nil {
		logger.Error("UploadFile is nil", slog.String("pre", pre))
		return fmt.Errorf("%w: UploadFile is nil", ErrInterfaceNotImplemented)
	}

	logger.Info("Download success, _Time", slog.String("pre", pre), slog.String("objectName", task.ObjectName))
	err = fo.UploadFile.UploadFile(ctx, task.ObjectName, length, hops, rateLimiter, reader, inMemory, pre, logger)
	if err != nil {
		logger.Error("UploadFile failed, _Time", slog.String("pre", pre),
			slog.String("objectName", task.ObjectName), slog.Any("err", err))
		return err
	}
	logger.Info("UploadFile success, _Time", slog.String("pre", pre), slog.String("objectName", task.ObjectName))

	// Final check before new status
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before update success state: %w", ctx.Err())
		logger.Error("UploadDirectImp canceled", slog.String("pre", pre), slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	chunk.LastSend = startTime
	chunk.Acked = int(ChunkStatusCompleted)
	task.Chunks.Set(task.Index, chunk)
	logger.Info("Chunk transfer success", slog.String("pre", pre), slog.String("index", task.Index))
	logger.Info("UploadRedirectImp success", slog.String("pre", pre), slog.String("index", task.Index))
	return nil
}
