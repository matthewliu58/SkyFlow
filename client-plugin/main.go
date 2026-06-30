package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/api"
	"rigel-client/config"
	"rigel-client/util"
	"runtime"
	"time"
)

type SourceHandler struct {
	handler slog.Handler
}

func (h *SourceHandler) Handle(ctx context.Context, r slog.Record) error {
	fs := runtime.CallersFrames([]uintptr{r.PC})
	frame, _ := fs.Next()
	fileName := filepath.Base(frame.File)
	r.AddAttrs(
		slog.String("file", fileName),
		slog.Int("line", frame.Line),
		slog.String("func", frame.Func.Name()),
	)
	return h.handler.Handle(ctx, r)
}

func (h *SourceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *SourceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &SourceHandler{handler: h.handler.WithAttrs(attrs)}
}

func (h *SourceHandler) WithGroup(name string) slog.Handler {
	return &SourceHandler{handler: h.handler.WithGroup(name)}
}

func AccessLogMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		latency := time.Since(start)
		logger.Info("access",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", latency.Milliseconds(),
			"remote", c.ClientIP(),
			"content_length", c.Request.ContentLength,
			"user_agent", c.Request.UserAgent(),
		)
	}
}

func main() {

	//logging
	logDir := "log"
	_ = os.MkdirAll(logDir, 0755)
	logFile, err := os.OpenFile(logDir+"/app.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()
	baseHandler := slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})
	logger := slog.New(&SourceHandler{handler: baseHandler})
	slog.SetDefault(logger)

	pre := util.GenerateRandomLetters(5)
	logger.Info("rigel client", slog.String("pre", pre))

	//config
	config.Config_, err = config.ReadYamlConfig(logger)
	if err != nil {
		logger.Error("read config error", slog.String("pre", pre), slog.Any("error", err))
		return
	}
	api.LocalBaseDir = config.Config_.LocalBaseDir
	logger.Info("config data", slog.String("pre", pre), slog.Any("data", config.Config_))

	//gin
	router := gin.New()
	router.Use(AccessLogMiddleware(logger))
	router.Use(gin.Recovery())
	router.POST("/api/v1/proxy/upload", api.V1ProxyUploadHandler(logger))
	router.POST("/api/v1/proxy/largefile/upload", api.V1ProxyLargeUploadHandler(logger))
	router.POST("/api/v1/client/upload", api.V1ClientUploadHandler(logger))
	router.POST("/api/v1/client/largefile/upload", api.V1ClientLargeUploadHandler(logger))
	router.POST("/api/v1/chunk/upload", api.ChunkUploadHandler(logger))
	router.POST("/api/v1/chunk/merge", api.ChunkMergeHandler(logger))

	port := "8080"
	logger.Info("gin run success", slog.String("pre", pre), slog.String("port", port))
	if err = router.Run(":" + port); err != nil {
		logger.Error("gin run failed", slog.String("pre", pre), slog.Any("err", err))
		return
	}
}
