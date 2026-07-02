package queue

import (
	"data-proxy/util"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/v3/process"
)

const (
	BufferSize            = 64
	WarningLevelforBuffer = 0.6
	//CriticalLevelforBuffer = 0.8
)

var (
	ActiveTransfers int64
)

type ProxyStatus struct {
	ActiveConnections int64   `json:"active_connections"` // Current active connection count
	TotalMem          int64   `json:"total_mem"`          // Total machine memory (bytes)
	ProcessMem        int64   `json:"process_mem"`        // Current process memory usage (bytes)
	AvgCachePerConn   float64 `json:"avg_cache_per_conn"` // Average cache size per connection (bytes)
	CacheUsageRatio   float64 `json:"cache_usage_ratio"`  // Cache usage ratio [0,1]
}

func CheckQueue(allBufferSize int, pre string, logger *slog.Logger) ProxyStatus {

	s := ProxyStatus{}

	p, _ := process.NewProcess(int32(os.Getpid()))
	memInfo, _ := p.MemoryInfo()
	proxyMem := int64(memInfo.RSS)
	s.ProcessMem = proxyMem

	perConnCache := allBufferSize * 1024 // 128 KB per connection
	active := atomic.LoadInt64(&ActiveTransfers)
	if active <= 0 {
		return s
	}
	s.ActiveConnections = active

	avgCache := float64(proxyMem) / float64(active)
	logger.Info("Proxy avg cache", slog.String("pre", pre), slog.Int64("proxyMem", proxyMem),
		slog.Int64("active", active), slog.Float64("avgCache", avgCache))

	s.AvgCachePerConn = avgCache
	s.CacheUsageRatio = avgCache / float64(perConnCache)

	if s.CacheUsageRatio > WarningLevelforBuffer {
		logger.Warn("Potential congestion: average per-connection buffer near 128KB",
			slog.String("pre", pre), slog.Float64("warning_level", WarningLevelforBuffer))
	}

	logger.Info("Proxy status", slog.String("pre", pre), slog.Any("ProxyStatus", s))
	return s
}

// GetCongestionInfo get congestion status /getCongestionInfo
func GetQueueInfo(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)
		info := CheckQueue(2*BufferSize, pre, logger)
		c.JSON(http.StatusOK, info)
	}
}
