package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log/slog"
	"net/http"
	"rigel-client/upload"
	"rigel-client/upload/base"
	"rigel-client/util"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	LargeFileUploadTimeout = 5 * time.Minute
)

type VM struct {
	IP     string `json:"ip" form:"ip"`
	Weight int64  `json:"weight" form:"weight"`
}

type LargeFile struct {
	VMs    []VM              `json:"vms" form:"vms"`
	Upload base.UploadStruct `json:"upload" form:"upload"`
}

func V1ProxyLargeUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Generate unique request ID for log tracking
		pre := util.GenerateRandomLetters(5)
		logger.Info("V1ProxyLargeUploadHandler start", slog.String("pre", pre))

		processUploadLogic(c, false, pre, logger)

		logger.Info("V1ProxyLargeUploadHandler end", slog.String("pre", pre))
	}
}

func V1ClientLargeUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Generate unique request ID for log tracking
		pre := util.GenerateRandomLetters(5)
		logger.Info("V1ClientLargeUploadHandler start", slog.String("pre", pre))

		processUploadLogic(c, true, pre, logger)

		logger.Info("V1ClientLargeUploadHandler end", slog.String("pre", pre))
	}
}

func processUploadLogic(c *gin.Context, clientB bool, pre string, logger *slog.Logger) {

	logger.Info("Start processing upload logic", slog.String("pre", pre), slog.Any("client", clientB))

	// 1. Parse request headers and body, build basic upload info
	largeFile, err := ParseHeadersAndBuildUploadInfo_(c, pre, logger)
	if err != nil {
		return // Error already handled inside ParseHeadersAndBuildUploadInfo with response returned
	}
	fo := upload.UploadFunc_(clientB, largeFile.Upload, pre, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		cancel() // Cancel context on both normal and abnormal exit
		logger.Info("Large upload context canceled", slog.String("pre", pre))
	}()

	up := largeFile.Upload
	if fo.GetFileSize == nil {
		err := fmt.Errorf("GetFileSize is nil")
		logger.Error("GetFileSize is nil", slog.String("pre", pre), slog.String("error", err.Error()))
		handleError(c, logger, pre, http.StatusInternalServerError, "GetFileSize is nil", err)
		return // Added return, original code was missing return which would continue execution
	}
	l, err := fo.GetFileSize.GetFileSize(ctx, up.File.FileName, pre, logger)
	if err != nil {
		handleError(c, logger, pre, http.StatusInternalServerError, "GetFileSize failed", err)
		return
	} else {
		logger.Info("GetFileSize success", slog.String("pre", pre), slog.Int64("size", l))
	}

	list, err := ProcessLargeFileUpload(clientB, largeFile, l, pre, logger)
	if err != nil {
		handleError(c, logger, pre, http.StatusInternalServerError, "ProcessLargeFileUpload failed", err)
		return
	} else {
		logger.Info("ProcessLargeFileUpload success", slog.String("pre", pre), slog.Any("list", list))
	}

	if len(list) > 1 {
		list = util.SortPartStrings(list)
		if fo.ComposeFile == nil {
			err := fmt.Errorf("ComposeFile is nil")
			logger.Error("ComposeFile is nil", slog.String("pre", pre), slog.String("error", err.Error()))
			handleError(c, logger, pre, http.StatusInternalServerError, "ComposeFile is nil", err)
			return // Added return, original code was missing return which would continue execution
		}
		err := fo.ComposeFile.ComposeFile(ctx, up.File.FileName, list, pre, logger)
		if err != nil {
			handleError(c, logger, pre, http.StatusInternalServerError, "ComposeFile failed", err)
			return
		}
	}
	logger.Info("processUploadLogic finished", slog.String("pre", pre))
	c.JSON(http.StatusOK, gin.H{"status": "success", "data": list})
}

func ParseHeadersAndBuildUploadInfo_(c *gin.Context, pre string, logger *slog.Logger) (LargeFile, error) {

	logger.Info("Start parsing upload info", slog.String("pre", pre))

	var req LargeFile
	if err := c.ShouldBindJSON(&req); err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body: %v", err)
		logger.Error(errMsg, slog.String("pre", pre))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return LargeFile{}, fmt.Errorf(errMsg)
	}

	logger.Info("ParseHeadersAndBuildUploadInfo_", slog.String("pre", pre), slog.Any("req", req))
	return req, nil
}

// Function returns: []string (split file name list), error
func ProcessLargeFileUpload(cleintB bool, largeFile LargeFile, size int64, pre string, logger *slog.Logger) ([]string, error) {
	// Store split file name list
	var splitFileNames []string

	// 1. Basic parameter validation
	logger.Info("starting large file chunk upload processing",
		slog.String("pre", pre),
		slog.String("func", "ProcessLargeFileUpload"),
	)

	if len(largeFile.VMs) == 0 {
		err := fmt.Errorf("no available VM nodes")
		logger.Error("parameter validation failed",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	file := largeFile.Upload.File
	if file.FileLength == 0 {
		file.FileLength = size
		logger.Info("file length is 0, using provided size",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.Int64("file_length", file.FileLength),
		)
	}

	// 2. Calculate total weight and bytes per VM
	totalWeight := int64(0)
	for _, vm := range largeFile.VMs {
		totalWeight += vm.Weight
	}
	logger.Info("VM total weight calculation complete",
		slog.String("pre", pre),
		slog.String("func", "ProcessLargeFileUpload"),
		slog.Int64("total_weight", totalWeight),
	)

	if totalWeight == 0 {
		err := fmt.Errorf("total weight of all VMs cannot be 0")
		logger.Error("weight validation failed",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	// 3. Calculate chunks (round to ensure total length consistency)
	var offset int64 = file.FileStart
	vmTasks := make(map[string]base.UploadStruct) // Store upload task per VM

	for i, vm := range largeFile.VMs {
		// Calculate bytes allocated to current VM (rounded)
		allocatedLength := (file.FileLength*vm.Weight + totalWeight/2) / totalWeight

		// Last VM handles remaining bytes (avoid total length inconsistency from rounding)
		if i == len(largeFile.VMs)-1 {
			allocatedLength = file.FileStart + file.FileLength - offset
		}

		// Build current VM's upload struct
		uploadTask := largeFile.Upload
		uploadTask.File.FileStart = offset
		uploadTask.File.FileLength = allocatedLength

		// Generate chunk file name
		splitFileName := fmt.Sprintf("%s_%d", file.NewFileName, i)
		uploadTask.File.NewFileName = splitFileName
		// Add chunk file name to list
		splitFileNames = append(splitFileNames, splitFileName)

		vmTasks[vm.IP] = uploadTask
		logger.Info("VM chunk calculation complete",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.String("vm_ip", vm.IP),
			slog.String("new_file_name", splitFileName),
		)

		offset += allocatedLength
	}

	// 4. Send requests concurrently and wait for all results
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	logger.Info("starting concurrent upload requests",
		slog.String("pre", pre),
		slog.String("func", "ProcessLargeFileUpload"),
		slog.Int("task_count", len(vmTasks)),
		slog.Duration("timeout", LargeFileUploadTimeout),
	)

	for ip, task := range vmTasks {
		wg.Add(1)
		go func(ip string, task base.UploadStruct) {
			defer wg.Done()

			// Construct request URL
			url := fmt.Sprintf("http://%s:8080/api/v1/proxy/upload", ip)
			if cleintB {
				url = fmt.Sprintf("http://%s:8080/api/v1/client/upload", ip)
			}

			// Serialize upload struct to JSON
			jsonData, err := json.Marshal(task)
			if err != nil {
				mu.Lock()
				finalErr := fmt.Errorf("failed to serialize task (%s): %v", ip, err)
				errs = append(errs, finalErr)
				mu.Unlock()
				logger.Error("task serialization failed",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.String("error", finalErr.Error()),
				)
				return
			}

			req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
			if err != nil {
				mu.Lock()
				finalErr := fmt.Errorf("failed to create request (%s): %v", ip, err)
				errs = append(errs, finalErr)
				mu.Unlock()
				logger.Error("create request failed",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.String("error", finalErr.Error()),
				)
				return
			}

			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Pre", pre)
			//token, err := util.GetGCPShortToken(context.Background(), config.Config_.GCPServiceAccount, pre, logger)
			//if err != nil {
			//	mu.Lock()
			//	finalErr := fmt.Errorf("failed to get GCP short token (%s): %v", ip, err)
			//	errs = append(errs, finalErr)
			//	return
			//}
			//req.Header.Set("Authorization", "Bearer "+token)

			logger.Info("upload request ready to send", slog.String("pre", pre), slog.Time("time", time.Now()))

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				mu.Lock()
				finalErr := fmt.Errorf("failed to send request (%s): %v", ip, err)
				errs = append(errs, finalErr)
				mu.Unlock()
				logger.Error("HTTP request send failed",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.String("error", finalErr.Error()),
				)
				return
			}
			defer resp.Body.Close()

			// Core check: whether HTTP status code is 200 (http.StatusOK)
			if resp.StatusCode != http.StatusOK {
				mu.Lock()
				finalErr := fmt.Errorf("node response status abnormal (%s): %d", ip, resp.StatusCode)
				errs = append(errs, finalErr)
				mu.Unlock()
				logger.Error("VM response status abnormal",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.Int("response_status", resp.StatusCode),
					slog.String("error", finalErr.Error()),
				)
				return
			}

			// Read response body (only handle read failure, don't print content)
			_, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				mu.Lock()
				finalErr := fmt.Errorf("failed to read response (%s): %v", ip, err)
				errs = append(errs, finalErr)
				mu.Unlock()
				logger.Error("response body read failed",
					slog.String("pre", pre),
					slog.String("func", "ProcessLargeFileUpload"),
					slog.String("vm_ip", ip),
					slog.String("error", finalErr.Error()),
				)
				return
			}

			logger.Info("VM upload request processed successfully",
				slog.String("pre", pre),
				slog.String("func", "ProcessLargeFileUpload"),
				slog.String("vm_ip", ip),
			)
		}(ip, task)
	}

	// Wait logic with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed normally
	case <-time.After(LargeFileUploadTimeout):
		// Timeout triggered
		finalErr := fmt.Errorf("upload task timed out (timeout: %v), some VMs may not have completed", LargeFileUploadTimeout)
		errs = append(errs, finalErr)
		logger.Error("upload task timed out",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.Duration("timeout", LargeFileUploadTimeout),
			slog.String("error", finalErr.Error()),
		)
		return nil, finalErr
	}

	if len(errs) <= 0 {
		logger.Info("all VM upload requests completed successfully",
			slog.String("pre", pre),
			slog.String("func", "ProcessLargeFileUpload"),
			slog.Any("split_file_names", splitFileNames),
		)
		return splitFileNames, nil
	}

	logger.Error("partial VM upload requests failed",
		slog.String("pre", pre),
		slog.String("func", "ProcessLargeFileUpload"),
		slog.Any("error", errs),
		slog.Any("split_file_names", splitFileNames),
	)
	return nil, fmt.Errorf("partial VM upload failures: %d nodes failed", len(errs))
}