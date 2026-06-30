package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"log/slog"
	"net/http"
	"rigel-client/config"
	"rigel-client/upload"
	"rigel-client/upload/base"
	"rigel-client/util"
	"strconv"
	"time"
)

const (
	RoutingURL = "/api/v1/routing"
)

var (
	LocalBaseDir string
)

// ---------------------- Core wrapper: Header parsing + full validation + UploadInfo construction ----------------------
// ParseHeadersAndBuildUploadInfo one-stop request header parsing, validation, UploadInfo construction
// Input: Gin context, log prefix, logger
// Output: constructed UploadInfo / whether response has been sent to client / error
func ParseHeadersAndBuildUploadInfo(c *gin.Context, pre string,
	logger *slog.Logger) (base.UploadStruct, int64, error) {

	logger.Info("Start parsing upload info", slog.String("pre", pre))

	//todo file size can be passed via header to avoid calculation
	var fileSize int64
	if sizeStr := c.GetHeader(util.HeaderFileSize); sizeStr != "" {
		if parsedSize, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
			fileSize = parsedSize
		} else {
			logger.Warn("Failed to parse file size, will calculate from source",
				slog.String("pre", pre), slog.String("sizeStr", sizeStr))
		}
	} else {
		logger.Warn("File size not found in header, will calculate from source", slog.String("pre", pre))
	}

	var req base.UploadStruct
	if err := c.ShouldBindJSON(&req); err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body: %v", err)
		logger.Error(errMsg, slog.String("pre", pre))
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return base.UploadStruct{}, 0, fmt.Errorf(errMsg)
	}

	if req.Source.Type == util.LocalDisk {
		req.Proxy.LocalDir = LocalBaseDir
	}

	logger.Info("TransferConfig", slog.String("pre", pre), slog.Any("req", req))
	return req, fileSize, nil
}

// V1ClientUploadHandler V1 client direct upload file handler
// Core flow: parse upload request headers -> directly call client upload logic -> return upload result
// Differs from V1 proxy upload: no need to call service B for routing, uploads file directly
func V1ClientUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Generate 5 random letters as unique request ID for log tracking
		requestID := util.GenerateRandomLetters(5)
		logger.Info("V2ClientUploadHandler start", slog.String("requestID", requestID))

		// 1. Parse request headers to build basic upload info (file name, storage path, client info, etc.)
		// Return values: uploadInfo-upload core info; _ (ignored)-extended field; err-parse error
		uploadInfo, fileSize, err := ParseHeadersAndBuildUploadInfo(c, requestID, logger)
		if err != nil {
			return // Error already handled inside ParseHeadersAndBuildUploadInfo with response returned
		}

		// 2. Call client direct upload to upload file to storage service (service C)
		// UploadDirectImp: client direct upload implementation (differs from V1 proxy forwarding)
		// Parameters: uploadInfo-upload info; UploadDirectImp-direct upload function; true-enable concurrency; requestID-request ID; logger-log instance
		if err := upload.UploadFunc(true, fileSize, uploadInfo, upload.DirectImp,
			upload.RoutingInfo{}, false, requestID, logger); err != nil {
			logger.Error("client direct upload failed", slog.String("requestID", requestID),
				slog.Any("err", err))
			// Return 500 internal error with specific error message
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// 3. Upload success, return standardized response
		logger.Info("V2ClientUploadHandler success", slog.String("requestID", requestID),
			slog.String("fileName", uploadInfo.File.FileName),
			slog.String("objectName", uploadInfo.File.NewFileName))
		c.JSON(http.StatusOK, gin.H{
			"message":    "upload by client success",  // Client direct upload success message
			"file_name":  uploadInfo.File.FileName,    // Original file name
			"objectName": uploadInfo.File.NewFileName, // Stored object name (may be renamed)
		})
	}
}

// V1ProxyUploadHandler proxy upload core handler
// Flow: parse request -> call service B for routing -> upload file to service C -> return response
func V1ProxyUploadHandler(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Generate unique request ID for log tracking
		pre := c.GetHeader("X-Pre")
		if len(pre) <= 0 {
			pre = util.GenerateRandomLetters(5)
		}
		logger.Info("V1ProxyUploadHandler start", slog.String("pre", pre))

		// 1. Parse request headers and body, build basic upload info
		uploadInfo, fileSize, err := ParseHeadersAndBuildUploadInfo(c, pre, logger)
		if err != nil {
			return // Error already handled inside ParseHeadersAndBuildUploadInfo with response returned
		}

		// 2. Call service B to get routing info
		routingInfo, err := getRoutingInfoFromServiceControlPlane(uploadInfo, pre, logger)
		if err != nil {
			handleError(c, logger, pre, http.StatusInternalServerError, "get routing info failed", err)
			return
		}
		if len(routingInfo.Routing) == 0 {
			handleError(c, logger, pre, http.StatusBadRequest, "routing info is empty", nil)
			return
		}

		// 3. Upload file to service C
		if err := upload.UploadFunc(false, fileSize, uploadInfo, upload.RedirectImp,
			routingInfo, false, pre, logger); err != nil {
			handleError(c, logger, pre, http.StatusInternalServerError, "upload to service C failed", err)
			return
		}

		// 4. Return success response
		logger.Info("V1ProxyUploadHandler success", slog.String("pre", pre),
			slog.String("fileName", uploadInfo.File.FileName),
			slog.String("objectName", uploadInfo.File.NewFileName))
		c.JSON(http.StatusOK, gin.H{
			"message":    "upload by proxy success",
			"file_name":  uploadInfo.File.FileName,
			"objectName": uploadInfo.File.NewFileName,
		})
	}
}

// getRoutingInfoFromServiceControlPlane calls the control plane to get routing info
func getRoutingInfoFromServiceControlPlane(uploadInfo base.UploadStruct, pre string,
	logger *slog.Logger) (upload.RoutingInfo, error) {

	// Build request to call service B
	reqBodyBytes, err := json.Marshal(uploadInfo.EndPoints)
	if err != nil {
		logger.Error("marshal endpoints failed", slog.String("pre", pre), slog.Any("err", err))
		return upload.RoutingInfo{}, err
	}
	req, err := http.NewRequest("POST", config.Config_.ControlHost+RoutingURL, bytes.NewReader(reqBodyBytes))
	if err != nil {
		logger.Error("build service B request failed", slog.String("pre", pre),
			slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}

	// Set request headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(util.HeaderFileName, uploadInfo.File.NewFileName)
	req.Header.Set("X-Pre", pre)

	// Send request to service B
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("call service B failed", slog.String("pre", pre), slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}
	defer resp.Body.Close()

	// Read service B response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("read service B response failed", slog.String("pre", pre),
			slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}

	type ApiResponse struct {
		Code int         `json:"code"` // 200=success, 400=bad request, 500=server error
		Msg  string      `json:"msg"`  // Message
		Data interface{} `json:"data"` // Business data
	}

	// Parse service B response
	var apiResp ApiResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		logger.Error("unmarshal service B response failed", slog.String("pre", pre),
			slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}

	// Parse routing info
	reqDataBytes, err := json.Marshal(apiResp.Data)
	if err != nil {
		logger.Error("marshal routing data failed", slog.String("pre", pre), slog.Any("err", err))
		return upload.RoutingInfo{}, err
	}
	logger.Info("get service control plane response", slog.String("pre", pre),
		slog.String("responseData", string(reqDataBytes)))
	var routingInfo upload.RoutingInfo
	if err = json.Unmarshal(reqDataBytes, &routingInfo); err != nil {
		logger.Error("unmarshal routing info failed", slog.String("pre", pre),
			slog.String("err", err.Error()))
		return upload.RoutingInfo{}, err
	}

	logger.Info("get routing info success", slog.String("pre", pre), slog.Any("routingInfo", routingInfo))
	return routingInfo, nil
}

// handleError unified error handling: log and return standardized response
func handleError(c *gin.Context, logger *slog.Logger, pre string, statusCode int, msg string, err error) {
	errMsg := msg
	if err != nil {
		errMsg = msg + ": " + err.Error()
	}
	logger.Error(errMsg, slog.String("pre", pre))
	c.JSON(statusCode, gin.H{
		"error": errMsg,
	})
}
