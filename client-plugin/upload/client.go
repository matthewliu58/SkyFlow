package upload

import (
	"fmt"
	"log/slog"
	"rigel-client/upload/base"
	"rigel-client/upload/split"
	"time"

	"golang.org/x/time/rate"
)

func DirectImp(fo base.FileOperateInterfaces, task ChunkTask, hops string,
	rateLimiter *rate.Limiter, inMemory bool, pre string, logger *slog.Logger) error {

	logger.Info("UploadDirectImp", slog.String("pre", pre), slog.String("index", task.Index)) // Optimization: only print index, avoid large task serialization

	// First get the basic info of current chunk (avoid null pointer)
	chunkVal, ok := task.Chunks.Get(task.Index)
	if !ok {
		err := fmt.Errorf("chunk index %s not found in Chunks map", task.Index) // Fix: Index is string, not int
		logger.Error("get chunk failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	chunk, ok := chunkVal.(*split.ChunkState)
	if !ok {
		err := fmt.Errorf("chunk index %s type is not *split.ChunkState", task.Index)
		logger.Error("chunk type assert failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	// Initial state: mark as transferring (Acked=1)
	startTime := time.Now()
	chunk.LastSend = startTime
	chunk.Acked = int(ChunkStatusTransferring) // 1=transferring
	task.Chunks.Set(task.Index, chunk)
	logger.Info("set chunk initial state", slog.String("pre", pre),
		slog.String("index", task.Index), slog.Int("acked", 1))

	// Define defer function: set Acked to failure on error (fallback)
	var finalErr error
	defer func() {
		if finalErr != nil {
			// On error: update status to failed
			chunk.LastSend = startTime
			chunk.Acked = int(ChunkStatusTransferFailed)
			task.Chunks.Set(task.Index, chunk)
			logger.Error("chunk transfer failed, set acked=0", slog.String("pre", pre),
				slog.String("index", task.Index), slog.Any("err", finalErr))
		}
	}()

	// Get Reader
	ctx := task.Ctx

	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before get reader: %w", ctx.Err())
		logger.Error("UploadDirectImp canceled", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	upload := task.Upload
	start := chunk.Offset
	length := chunk.Size
	reader, err := GetTransferReader(ctx, fo, upload, start, length, task.ObjectName, inMemory, pre, logger)
	if err != nil {
		finalErr = err
		return finalErr
	}
	defer func() {
		if reader != nil {
			_ = reader.Close() // Ensure Reader is closed regardless of success/failure
		}
	}()

	logger.Info("download object success_Time", slog.String("pre", pre),
		slog.String("objectName", task.ObjectName), slog.Time("time", time.Now()))

	// Upload to target
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before upload: %w", ctx.Err())
		logger.Error("UploadDirectImp canceled", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	if fo.UploadFile == nil {
		logger.Error("UploadFile is nil", slog.String("pre", pre))
		return fmt.Errorf("%w: UploadFile is nil", ErrInterfaceNotImplemented)
	}
	err = fo.UploadFile.UploadFile(ctx, task.ObjectName, length, hops, rateLimiter, reader, inMemory, pre, logger)
	if err != nil {
		logger.Error("UploadFile failed", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", err))
		return err
	} else {
		logger.Info("UploadFile success_Time", slog.String("pre", pre),
			slog.String("index", task.Index))
	}

	// Success status update (Acked=2)
	select {
	case <-ctx.Done():
		finalErr = fmt.Errorf("ctx canceled before update success state: %w", ctx.Err())
		logger.Error("UploadDirectImp canceled", slog.String("pre", pre),
			slog.String("index", task.Index), slog.Any("err", finalErr))
		return finalErr
	default:
	}

	chunk.LastSend = startTime
	chunk.Acked = int(ChunkStatusCompleted)
	task.Chunks.Set(task.Index, chunk)
	logger.Info("chunk transfer success, set acked=2", slog.String("pre", pre),
		slog.String("index", task.Index))

	logger.Info("UploadDirectImp success", slog.String("pre", pre), slog.String("index", task.Index))
	return nil
}
