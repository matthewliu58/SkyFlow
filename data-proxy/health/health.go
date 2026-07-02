package health

import (
	"data-proxy/util"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

var (
	// Lock and status variables
	statusLock sync.Mutex
	status     string = "on" // Default status is "on"
)

// HealthStateChange modify health status switch /healthStateChange?set=on/off
func HealthStateChange(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)
		logger.Info("healthStateChange", slog.String("pre", pre))

		// Get parameter
		set := c.DefaultQuery("set", "on")
		logger.Info("get switch val", slog.String("pre", pre), slog.String("set", set))

		// Lock and modify status
		statusLock.Lock()
		defer statusLock.Unlock()

		if set == "off" {
			status = "off"
		} else {
			status = "on"
		}

		c.JSON(http.StatusOK, "success")
	}
}

// Health health check /health
func Health(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		pre := util.GenerateRandomLetters(5)

		statusLock.Lock()
		defer statusLock.Unlock()

		logger.Info("health", slog.String("pre", pre), slog.String("status", status))

		if status == "off" {
			c.JSON(http.StatusInternalServerError, "error")
			return
		}
		c.JSON(http.StatusOK, "success")
	}
}
