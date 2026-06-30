package upload

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"log/slog"
	"math/rand"
	"rigel-client/upload/base"
	"rigel-client/upload/split"
	"rigel-client/util"
	"time"
)

var (
	ErrFileSizeFailed          = errors.New("get file size failed")
	ErrChunkSplitFailed        = errors.New("split file failed")
	ErrUploadTimeout           = errors.New("upload timeout")
	ErrInterfaceNotImplemented = errors.New("implementation is nil")
)

type (
	ChunkStatus int
)

const (
	ChunkStatusInit           ChunkStatus = 0
	ChunkStatusTransferring   ChunkStatus = 1
	ChunkStatusTransferFailed ChunkStatus = 2
	ChunkStatusCompleted      ChunkStatus = 3
)

const (
	MaxConcurrency          = 10
	QueueBufferSize         = 100
	CheckInterval           = 2 * time.Second // Chunk timeout check interval
	ChunkExpireTime         = 2 * time.Minute // Chunk expire retransmission threshold
	UploadTimeout           = 5 * time.Minute // Overall upload timeout duration
	TaskSubmitRetryInterval = 1 * time.Second
	ChunkSubmitDelay        = 100 * time.Millisecond
	ChunkSizeInMemory       = 512 * 1024 * 1024 // 512MB
)

type ChunkEventType int

const (
	ChunkExpired ChunkEventType = iota
	ChunkFinished
)

type ChunkEvent struct {
	Type    ChunkEventType               // Event type
	Indexes map[string]*split.ChunkState // Expired chunk indexes
}

type ChunkTask struct {
	Ctx        context.Context // Context with request ID
	Index      string
	Chunks     *util.SafeMap
	ObjectName string
	Upload     base.UploadStruct
	Pre        string // Preserve original pre parameter, fully compatible
}

type PathInfo struct {
	Hops string `json:"hops"`
	Rate int64  `json:"rate"`
	//Weight int64  `json:"weight"`
}

type RoutingInfo struct {
	Routing []PathInfo `json:"routing"`
}

type WorkerPool struct {
	TaskCh chan ChunkTask
	cancel func() // New: goroutine pool exit signal
}

// -------------------------- 6. Core functions --------------------------

// StartChunkTimeoutChecker preserves pre parameter, uses context propagation, adds global timeout control
// New parameter:
//
//	globalTimeout - overall checker timeout duration (0 means no limit, controlled only by ctx)
func StartChunkTimeoutChecker(
	ctx context.Context,
	s *util.SafeMap,
	interval time.Duration,
	expire time.Duration,
	globalTimeout time.Duration, // New: overall checker timeout duration
	events chan<- ChunkEvent,
	pre string, // Preserve original pre parameter
	logger *slog.Logger,
) {

	// 2. Build context with global timeout (core fix: add timeout control)
	var cancel func()
	if globalTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, globalTimeout)
		defer cancel() // Ensure resource release on timeout/exit
	}

	// 3. Initialize ticker
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("StartChunkTimeoutChecker",
		slog.String("pre", pre),
		slog.Duration("interval", interval),
		slog.Duration("expire", expire),
		slog.Duration("global_timeout", globalTimeout))

	// 4. Loop logic: add global timeout exit branch
	for {
		select {
		case <-ticker.C:
			expired, finished, hasInitSendingChunk := CollectExpiredChunks(s, expire, pre, logger)

			// Original business logic: check chunk status and send events
			if !hasInitSendingChunk {
				if finished {
					events <- ChunkEvent{Type: ChunkFinished}
					logger.Info("Chunk checker exit: all chunks finished", slog.String("pre", pre))
					return // Normal exit
				}
				if len(expired) > 0 {
					events <- ChunkEvent{Type: ChunkExpired, Indexes: expired}
					logger.Warn("Chunk checker detect expired chunks",
						slog.String("pre", pre),
						slog.Any("expired_indexes", expired))
				}
			}

		case <-ctx.Done():
			// Context ended (timeout/manual cancel), exit loop
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				logger.Warn("Chunk checker exit: global timeout reached",
					slog.String("pre", pre),
					slog.Duration("timeout", globalTimeout))
			} else {
				logger.Info("Chunk checker exit: context canceled",
					slog.String("pre", pre),
					slog.Any("err", err))
			}
			return // Timeout/cancel exit
		}
	}
}

// CollectExpiredChunks preserves pre parameter, replaces Acked with status enum
func CollectExpiredChunks(
	s *util.SafeMap,
	expire time.Duration,
	pre string, // Preserve pre parameter
	logger *slog.Logger,
) (expired map[string]*split.ChunkState, finished, hasInitSendingChunk bool) {
	now := time.Now()
	expired = make(map[string]*split.ChunkState)
	finished = true // Assume all are acked initially

	logger.Info("CollectExpiredChunks", slog.String("pre", pre),
		slog.Any("now", now), slog.Any("expire", expire))
	chunks := s.GetAll()

	for _, v := range chunks {
		v_, ok := v.(*split.ChunkState)
		if !ok {
			continue
		}

		// Core refactor: use enum instead of raw Acked numeric check
		status := ChunkStatus(v_.Acked)

		// Cannot resubmit before initial transmission completes
		if status == ChunkStatusInit {
			logger.Warn("Resubmit rejected, initial transmission is still in progress",
				slog.String("pre", pre), slog.String("index", v_.Index))
			return expired, false, true
		}

		if status == ChunkStatusTransferring || status == ChunkStatusTransferFailed {
			finished = false // If any is not acked, it's not finished
			if !v_.LastSend.IsZero() && now.Sub(v_.LastSend) > expire {
				expired[v_.Index] = v_
			}
		}
	}

	return expired, finished, false
}

// NewWorkerPool preserves pre parameter, context propagation + new cancel logic
func NewWorkerPool(
	fo base.FileOperateInterfaces,
	queueSize int,
	routingInfo RoutingInfo,
	handler func(base.FileOperateInterfaces, ChunkTask, string, *rate.Limiter, bool, string, *slog.Logger) error,
	inMemory bool,
	pre string, // Preserve pre parameter
	logger *slog.Logger,
) *WorkerPool {
	taskCh := make(chan ChunkTask, queueSize)
	// New: create cancel context for stopping worker pool
	ctx, cancel := context.WithCancel(context.Background())

	p := &WorkerPool{
		TaskCh: taskCh,
		cancel: cancel, // Save cancel function
	}
	logger.Info("NewWorkerPool", slog.String("pre", pre), "queueSize", queueSize)

	// Extract common worker execution function, eliminate duplicate code
	runWorker := func(workerID int, hops string, limiter *rate.Limiter, workerType string) {
		logger.Info(fmt.Sprintf("Worker for %s init", workerType), slog.String("pre", pre), "worker", workerID,
			slog.String("hops", hops), slog.Any("limiter", limiter))

		for {
			select {
			case <-ctx.Done(): // Listen for cancel signal
				logger.Info("Worker exit: context canceled", slog.String("pre", pre), "worker", workerID)
				return
			case task, ok := <-taskCh: // Listen on task channel (ok=false when closed)
				if !ok {
					logger.Info("Worker exit: task channel closed", slog.String("pre", pre), "worker", workerID)
					return
				}

				err := handler(
					fo,
					task,
					hops,
					limiter,
					inMemory,
					pre, // Pass pre parameter
					logger,
				)

				if err != nil {
					logger.Error("handle task", slog.String("pre", pre), "worker", workerID, "err", err)
				} else {
					logger.Info("handle task", slog.String("pre", pre), "worker", workerID, "task", task.Index)
				}
			}
		}
	}

	workerNum := len(routingInfo.Routing)
	if workerNum <= 0 {
		// Direct branch: hops is empty, limiter is nil
		for i := 0; i < MaxConcurrency; i++ {
			go runWorker(i, "", nil, "direct")
		}
	} else {
		// Redirect branch: initialize limiter and hops
		rand.Seed(time.Now().UnixNano())
		for i := 0; i < MaxConcurrency; i++ {
			index := rand.Intn(workerNum)
			pathInfo := routingInfo.Routing[index]
			rate_ := pathInfo.Rate
			bytesPerSec := rate_ * 1024 * 1024 / 8 // Mbps → bytes/sec
			limiter := rate.NewLimiter(rate.Limit(bytesPerSec), int(bytesPerSec))
			go runWorker(i, pathInfo.Hops, limiter, "redirect")
		}
	}
	return p
}

// Stop is new: terminates the worker pool (close task channel + cancel context)
func (p *WorkerPool) Stop() {
	if p.cancel != nil {
		p.cancel() // Trigger ctx.Done() for all workers
	}
	close(p.TaskCh) // Close task channel
}

// ChunkEventLoop preserves pre parameter, status enum replaces Acked + listen for cancel signal
func ChunkEventLoop(ctx context.Context, fo base.FileOperateInterfaces, upload base.UploadStruct,
	chunks *util.SafeMap, workerPool *WorkerPool, events <-chan ChunkEvent, done chan struct{}, inMemory bool,
	pre string, logger *slog.Logger) {

	logger.Info("ChunkEventLoop", slog.String("pre", pre))

	for {
		select {
		case ev, ok := <-events: // Listen on events channel (ok=false when closed)
			if !ok {
				logger.Info("ChunkEventLoop exit: events channel closed", slog.String("pre", pre))
				close(done)
				return
			}
			switch ev.Type {
			case ChunkExpired:
				logger.Warn("ChunkExpired", slog.String("pre", pre), "indexes", ev.Indexes)
				StartChunkSubmitLoop(ctx, chunks, workerPool, upload, true, ev.Indexes, pre, logger)

			case ChunkFinished:
				logger.Info("ChunkFinished_Time", slog.String("pre", pre),
					slog.String("fileName", upload.File.NewFileName), slog.Time("time", time.Now()))

				var parts []string
				chunks_ := chunks.GetAll()
				for _, v := range chunks_ {
					v_, ok := v.(*split.ChunkState)
					if !ok {
						continue
					}

					// Use enum to check status
					if ChunkStatus(v_.Acked) != ChunkStatusCompleted {
						logger.Error("upload failed", slog.String("pre", pre),
							slog.String("fileName", upload.File.NewFileName), "index", v_.Index)
						close(done)
						return
					}
					logger.Info("upload success", slog.String("pre", pre), slog.String("fileName",
						upload.File.NewFileName), "index", v_.Index, "ObjectName", v_.ObjectName)
					parts = append(parts, v_.ObjectName)
				}

				parts = util.SortPartStrings(parts)

				var err error
				if fo.ComposeFile == nil {
					logger.Error("ComposeFile is nil", slog.String("pre", pre))
					return
				}
				err = fo.ComposeFile.ComposeFile(ctx, upload.File.NewFileName, parts, pre, logger)
				if err != nil {
					logger.Error("Compose failed", slog.String("pre", pre),
						slog.String("fileName", upload.File.NewFileName), slog.Any("err", err))
				}
				close(done)

				if !inMemory {
					_ = util.DeleteFilesInDir(upload.Proxy.LocalDir, parts, pre, logger)
				}

				return
			}

		case <-ctx.Done(): // Listen for main context cancel signal (timeout/manual termination)
			logger.Info("ChunkEventLoop exit: context canceled", slog.String("pre", pre), "err", ctx.Err())
			close(done)
			return
		}
	}
}

// Submit preserves original logic, compatible with pre
func (p *WorkerPool) Submit(task ChunkTask) bool {
	select {
	case p.TaskCh <- task:
		return true
	default:
		// Queue is full, can choose to drop / log / count
		return false
	}
}

// StartChunkSubmitLoop preserves pre parameter, status enum check + listen for cancel signal
func StartChunkSubmitLoop(
	ctx context.Context,
	chunks *util.SafeMap,
	workerPool *WorkerPool,
	uploadInfo base.UploadStruct,
	resubmit bool,
	resubmitIndexes map[string]*split.ChunkState,
	pre string, // Preserve pre parameter
	logger *slog.Logger,
) {
	logger.Info("StartChunkSubmitLoop", slog.String("pre", pre), "fileName", uploadInfo.File.NewFileName)
	chunks_ := chunks.GetAll()

	for _, v := range chunks_ {
		// Check context cancellation on each loop iteration, avoid invalid submissions
		select {
		case <-ctx.Done():
			logger.Info("StartChunkSubmitLoop exit: context canceled", slog.String("pre", pre))
			return
		default:
			time.Sleep(ChunkSubmitDelay)
		}

		v_, ok := v.(*split.ChunkState)
		if !ok {
			continue
		}

		// Use enum to check status
		status := ChunkStatus(v_.Acked)
		if resubmit {
			if _, ok := resubmitIndexes[v_.Index]; !ok || status == ChunkStatusCompleted {
				continue
			}
		} else {
			if status != ChunkStatusInit {
				continue
			}
		}

		task := ChunkTask{
			Ctx:        ctx,
			Index:      v_.Index,
			Chunks:     chunks,
			ObjectName: v_.ObjectName,
			Upload:     uploadInfo,
			Pre:        pre, // Assign pre field
		}

		if !workerPool.Submit(task) {
			logger.Warn("workerPool full", slog.String("pre", pre))
			time.Sleep(TaskSubmitRetryInterval)
			break
		}
	}
}

func UploadFunc_(
	clientB bool,
	us base.UploadStruct,
	pre string, // Preserve original pre parameter
	logger *slog.Logger) base.FileOperateInterfaces {

	logger.Info("UploadFunc_", slog.String("pre", pre), slog.Any("us", us))
	fo := base.InitInterface(clientB, us, pre, logger)

	return fo
}

// Upload is the core entry point: preserves pre parameter, context propagation + unified cancel all goroutines
func UploadFunc(
	clientB bool,
	fileSize int64,
	us base.UploadStruct,
	handler func(base.FileOperateInterfaces, ChunkTask, string, *rate.Limiter, bool, string, *slog.Logger) error,
	routing RoutingInfo,
	noSplitB bool,
	pre string, // Preserve original pre parameter
	logger *slog.Logger) error {

	logger.Info("UploadFunc", slog.String("pre", pre), slog.Any("us", us))

	// Core fix 1: create cancelable context with global timeout (manage all goroutines)
	ctx, cancel := context.WithTimeout(context.Background(), UploadTimeout)
	defer func() {
		cancel() // Cancel context regardless of normal/abnormal exit
		logger.Info("Upload context canceled", slog.String("pre", pre))
	}()

	// 1. Initialize interface
	fo := base.InitInterface(clientB, us, pre, logger)

	// 3. Get actual file length
	var err error
	if fileSize <= 0 {
		if fo.GetFileSize == nil {
			logger.Info("GetFileSize is nil, use default implementation")
			return fmt.Errorf("%w: GetFileSize is nil", ErrInterfaceNotImplemented)
		}
		fileSize, err = fo.GetFileSize.GetFileSize(ctx, us.File.FileName, pre, logger)
		if err != nil {
			logger.Error("Get file size failed", slog.String("pre", pre), slog.Any("err", err))
			return fmt.Errorf("%w: %s", ErrFileSizeFailed, err.Error())
		}
	}
	logger.Info("Get file size success", slog.String("pre", pre),
		slog.Int64("size", fileSize))

	// 4. Split file into chunks
	chunks := util.NewSafeMap()
	_, err = split.SplitFilebyRange(fileSize, us.File.FileStart, us.File.FileLength,
		us.File.FileName, us.File.NewFileName, noSplitB, chunks, pre, logger)
	if err != nil {
		logger.Error("Split file failed", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("%w: %s", ErrChunkSplitFailed, err.Error())
	} else {
		logger.Info("Split file success", slog.String("pre", pre), slog.Int64("size", fileSize))
	}

	//inMemory := false
	//if chunkSize >= ChunkSizeInMemory || uploadInfo.Source.SourceType == util.LocalDisk {
	//	inMemory = true
	//}
	//In-memory mode is enabled by default
	inMemory := true

	// Start periodic retransmission & check transfer completion
	done := make(chan struct{})
	events := make(chan ChunkEvent, QueueBufferSize)
	// Start timeout checker (pass ctx with timeout)
	go StartChunkTimeoutChecker(ctx, chunks, CheckInterval, ChunkExpireTime, UploadTimeout, events, pre, logger)

	// Start consumer, default one HTTP concurrency
	workerPool := NewWorkerPool(fo, QueueBufferSize, routing, handler, inMemory, pre, logger)
	defer workerPool.Stop() // Core fix 2: terminate worker pool on exit

	// Events consumer (pass ctx with timeout)
	go ChunkEventLoop(ctx, fo, us, chunks, workerPool, events, done, inMemory, pre, logger)

	// 4. Start chunk upload (pass ctx with timeout)
	go StartChunkSubmitLoop(ctx, chunks, workerPool, us, false, nil, pre, logger)

	newFileName := us.File.NewFileName
	select {
	case <-done:
		logger.Info("Function finished", slog.String("pre", pre), slog.String("newFileName", newFileName))
	case <-ctx.Done():
		// Timeout/cancel triggered
		err := ctx.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			logger.Warn("Timeout exit", slog.String("pre", pre), slog.String("newFileName", newFileName))
			return fmt.Errorf("%w: %s", ErrUploadTimeout, newFileName)
		}
		logger.Info("Upload canceled", slog.String("pre", pre), slog.Any("err", err))
		return fmt.Errorf("upload canceled: %w", err)
	}

	logger.Info("UploadFunc finished", slog.String("pre", pre), slog.String("newFileName", newFileName))
	return nil
}

// GetTransferReader preserves pre parameter, context propagation
func GetTransferReader(
	ctx context.Context,
	fo base.FileOperateInterfaces,
	upload base.UploadStruct,
	start, length int64,
	objectName string,
	inMemory bool,
	pre string, // Preserve pre parameter
	logger *slog.Logger,
) (io.ReadCloser, error) {

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("GetTransferReader canceled", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	default:
	}

	var reader io.ReadCloser
	var err error
	if fo.DownloadFile == nil {
		logger.Error("DownloadFile is nil", slog.String("pre", pre))
		return nil, fmt.Errorf("%w: DownloadFile is nil", ErrInterfaceNotImplemented)
	}
	reader, err = fo.DownloadFile.DownloadFile(ctx, upload.File.FileName, objectName,
		start, length, "", inMemory, pre, logger)
	if err != nil {
		logger.Error("GetTransferReader failed", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	}

	logger.Info("GetTransferReader success",
		slog.String("pre", pre),
		slog.String("fileName", upload.File.FileName),
		slog.Int64("start", start),
		slog.Int64("length", length),
		slog.String("objectName", objectName))

	return reader, nil
}
