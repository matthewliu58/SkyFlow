package telemetry

import (
	"bytes"
	"data-plane/telemetry/collector"
	"data-plane/telemetry/model"
	"data-plane/util"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Constants (hardcoded)
const (
	//ControlHost    = "http://34.69.185.247:8081"
	ReportURL      = "/api/v1/vm/receive"
	ReportInterval = 10 * time.Second
)

// HTTPReporter HTTP reporter
type HTTPReporter struct {
	client *http.Client
}

// NewHTTPReporter initialize reporter
func NewHTTPReporter() *HTTPReporter {
	return &HTTPReporter{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Report reports VM info (wrapped in ApiResponse format)
func (r *HTTPReporter) Report(controlHost, pre string, vmReport *model.VMReport) error {
	// 1. Fill ReportID (if empty)
	if vmReport.ReportID == "" {
		vmReport.ReportID = uuid.NewString()
	}

	// 2. Build outer ApiResponse request body
	reqBody := model.ApiResponse{
		Code: 200,
		Msg:  "VM info report request",
		Data: vmReport,
	}

	// 3. Serialize to JSON
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	// 4. Send POST request
	resp, err := r.client.Post(controlHost+ReportURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 5. Parse response (optional, verify report result)
	var respBody model.ApiResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return err
	}

	if respBody.Code != 200 {
		return fmt.Errorf("report failed: %s", respBody.Msg)
	}

	return nil
}

// ReportCycle starts the periodic VM info reporting loop
func ReportCycle(controlHost, pre string, logger *slog.Logger) {
	// 1. Initialize collector and reporter
	vmCollector := collector.NewVMCollector()
	httpReporter := NewHTTPReporter()

	// 2. Start periodic reporting
	ticker := time.NewTicker(ReportInterval)
	defer ticker.Stop()

	logger.Info(
		"Data plane started, beginning periodic reporting", slog.String("pre", pre),
		slog.Duration("report_interval", ReportInterval),
		slog.String("report_url", controlHost+ReportURL),
	)

	// 3. Execute once immediately then on interval
	//reportOnce(vmCollector, httpReporter, logger)

	for range ticker.C {
		reportOnce(controlHost, vmCollector, httpReporter, logger)
	}
}

// reportOnce single report execution
func reportOnce(controlHost string, collector *collector.VMCollector, reporter *HTTPReporter, logger *slog.Logger) {

	pre := util.GenerateRandomLetters(5)

	// 1. Collect info
	logger.Info("Collecting VM info...", slog.String("pre", pre))
	vmReport, err := collector.Collect(pre, logger)
	if err != nil {
		logger.Error("Collection failed", slog.String("pre", pre), slog.Any("err", err))
		return
	}

	// 2. Report info
	b, _ := json.Marshal(vmReport)
	logger.Info("Reporting VM info", slog.String("pre", pre), slog.String("data", string(b)))

	err = reporter.Report(controlHost, pre, vmReport)
	if err != nil {
		logger.Error("Report failed", slog.String("pre", pre), slog.Any("err", err))
		return
	}

	logger.Info("Report success", slog.String("pre", pre), slog.String("ReportID", vmReport.ReportID))
}
