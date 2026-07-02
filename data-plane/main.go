package main

import (
	"context"
	"data-plane/probing"
	"data-plane/telemetry"
	"data-plane/util"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Custom Handler: Fix slog.Context to context.Context for Go 1.21+ compatibility
type SourceHandler struct {
	handler slog.Handler
}

// Handle: Core fix - change slog.Context to context.Context
func (h *SourceHandler) Handle(ctx context.Context, r slog.Record) error {
	// Collect log call location (skip current handler's stack frame, get real business code location)
	fs := runtime.CallersFrames([]uintptr{r.PC})
	frame, _ := fs.Next()

	// Keep only filename (remove full path)
	fileName := filepath.Base(frame.File)

	// Add source location fields to log record
	r.AddAttrs(
		slog.String("file", fileName),          // File name
		slog.Int("line", frame.Line),           // Line number
		slog.String("func", frame.Func.Name()), // Function name (optional)
	)

	// Pass to underlying TextHandler for output
	return h.handler.Handle(ctx, r)
}

// Below are default implementations of slog.Handler interface (all fixed to context.Context)
func (h *SourceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *SourceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &SourceHandler{handler: h.handler.WithAttrs(attrs)}
}

func (h *SourceHandler) WithGroup(name string) slog.Handler {
	return &SourceHandler{handler: h.handler.WithGroup(name)}
}

var (
	// Lock and status variables
	statusLock sync.Mutex
	status     string = "on" // Default status is "on"
)

func main() {

	// Create log directory (same level as pkg)
	logDir := filepath.Join(".", "log")
	if err := os.MkdirAll(logDir, os.ModePerm); err != nil {
		panic("Failed to create log directory: " + err.Error())
	}
	logFilePath := filepath.Join(logDir, "app.log")
	//logFilePath1 := filepath.Join(logDir, "envoy.log")
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file: " + err.Error())
	}
	//logFile1, err := os.OpenFile(logFilePath1, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if err != nil {
	//	panic("无法打开日志文件: " + err.Error())
	//}

	// Initialize logger, output to log/app.log
	//logger := slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{
	//	Level: slog.LevelInfo,
	//}))
	//logger1 := slog.New(slog.NewTextHandler(logFile1, &slog.HandlerOptions{
	//	Level: slog.LevelInfo,
	//}))

	// 2. Configure base TextHandler (preserve existing Level configuration)
	baseHandler := slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level:     slog.LevelInfo, // Log level
		AddSource: true,           // Must be enabled! Otherwise cannot get filename/line number
	})

	// 3. Wrap with custom SourceHandler (add filename, line number, function name)
	logger := slog.New(&SourceHandler{handler: baseHandler})

	// 4. Set as global logger (optional, affects entire project)
	slog.SetDefault(logger)

	logPre := "init"

	util.Config_, err = util.ReadYamlConfig(logger)
	if err != nil {
		logger.Error("read config failed", slog.String("pre", logPre), slog.Any("err", err))
		return
	} else {
		b, _ := json.Marshal(util.Config_)
		logger.Info("Config file read successfully", slog.String("pre", logPre),
			slog.String("config", string(b)))
	}
	//envoy_manager.EnvoyPath = util.Config_.EnvoyPath
	//envoy_manager.DefaultConfig = util.Config_.DefaultConfig
	//envoy_manager.EnvoyLog = util.Config_.EnvoyLog

	// 2. Initialize Gin router
	router := gin.Default()

	router.GET("/healthStateChange", func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)

		logger.Info("healthStateChange", slog.String("pre", pre))

		// Get query parameter "set"
		set := c.DefaultQuery("set", "on") // Default value is "on", returns 200

		logger.Info("get switch val", slog.String("pre", pre))

		// Lock status modification for concurrency safety
		statusLock.Lock()
		defer statusLock.Unlock()

		// Determine status and return code based on set parameter
		if set == "off" {
			// set is "off", change status to "off", return 500
			status = "off"
		} else {
			// Default or set is "on", change status to "on", return 200
			status = "on"
		}
		c.JSON(http.StatusOK, "success")
	})

	router.GET("/health", func(c *gin.Context) {

		pre := util.GenerateRandomLetters(5)

		// Lock status modification for concurrency safety
		statusLock.Lock()
		defer statusLock.Unlock()

		logger.Info("health", slog.String("pre", pre), slog.String("status", status))

		if status == "off" {
			c.JSON(http.StatusInternalServerError, "error")
			return
		}
		c.JSON(http.StatusOK, "success")
	})

	// 3. Initialize reporter
	go telemetry.ReportCycle(util.Config_.ControlHost, logPre, logger)

	// Start probing logic
	cfg := probing.Config{
		Concurrency: 4,
		Timeout:     2 * time.Second,
		Interval:    5 * time.Second,
		Attempts:    5, // Number of attempts per round
	}
	ctx := context.Background()
	probing.StartProbePeriodically(ctx, util.Config_.ControlHost, cfg, logPre, logger)
	//logger.Info("", "probe result", probingResult)

	//InitEnvoy(logger, logger1)

	// 4. Start API server
	logger.Info("API server starting", slog.String("pre", logPre), slog.String("addr", ":8082"))
	if err := router.Run(":8082"); err != nil {
		logger.Error("API server startup failed", slog.String("pre", logPre), slog.Any("err", err))
		return
	}
}
