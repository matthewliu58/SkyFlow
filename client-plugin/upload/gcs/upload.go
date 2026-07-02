package gcs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	cfg "rigel-client/config"
	limit_rate "rigel-client/limit-rate"
	"rigel-client/util"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	gcpScopes = "https://www.googleapis.com/auth/devstorage.full_control"
)

type Upload struct {
	localBaseDir string
	bucketName   string
	token        string
	credFile     string
}

func NewUpload(
	localBaseDir, bucketName, token string,
	credFile string,
	pre string, // Log prefix
	logger *slog.Logger,
) *Upload {
	u := &Upload{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		token:        token,
		credFile:     credFile,
	}
	// Same log printing logic as NewDownload
	logger.Info("NewUpload", slog.String("pre", pre), slog.Any("Upload", *u))
	return u
}

func (u *Upload) UploadFile(
	ctx context.Context,
	objectName string,
	contentLength int64,
	hops string,
	rateLimiter *rate.Limiter,
	reader io.ReadCloser,
	inMemory bool,
	pre string,
	logger *slog.Logger,
) error {

	logger.Info("UploadToGCSbyProxy start", slog.String("pre", pre))

	if len(hops) == 0 {
		err := fmt.Errorf("hops is empty")
		logger.Error("invalid hops", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("UploadToGCSbyProxy canceled", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	var proxyReader io.ReadCloser = reader

	// Define resource cleanup defer (release all Readers)
	defer func() {
		if proxyReader != nil && proxyReader != reader {
			_ = proxyReader.Close() // Close local file Reader
		}
		// External reader is closed by caller, don't close here (avoid double close)
	}()

	// ---------------------- 2. Select upload source: memory stream / local file ----------------------
	if !inMemory {
		// Mode 1: inMemory=false -> read from local file
		localFilePath := filepath.Join(u.localBaseDir, objectName)
		localFilePath = filepath.Clean(localFilePath) // Normalize path (prevent multiple slashes)

		logger.Info("prepare to read local file",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath))

		f, err := os.Open(localFilePath)
		if err != nil {
			logger.Error("failed to open local file",
				slog.String("pre", pre),
				slog.String("localFilePath", localFilePath),
				slog.Any("err", err))
			return fmt.Errorf("failed to open local file: %w", err)
		}
		proxyReader = f // 替换为本地文件Reader
		logger.Info("local file opened successfully",
			slog.String("pre", pre),
			slog.String("localFilePath", localFilePath))
	} else {
		// Mode 2: inMemory=true -> use externally provided memory Reader
		if proxyReader == nil {
			err := fmt.Errorf("inMemory=true but reader is nil")
			logger.Error("invalid reader", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
		logger.Info("use in-memory reader for upload", slog.String("pre", pre))
	}

	// ---------------------- 3. Rate limit wrap Reader ----------------------
	rateLimitedBody := limit_rate.NewRateLimitedReader(ctx, proxyReader, rateLimiter)
	logger.Info("rate limiter applied to reader", slog.String("pre", pre))

	// ---------------------- 4. Parse hops and construct URL ----------------------
	hopList := strings.Split(hops, ",")
	if len(hopList) == 0 {
		err := fmt.Errorf("invalid X-Hops: %s (split empty)", hops)
		logger.Error("parse hops failed", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	//If first hop is local IP, change to 127.0.0.1 to bypass public
	firstHop := hopList[0]
	if firstHop == cfg.PublicIp {
		firstHop = "127.0.0.1"
	}

	url := fmt.Sprintf(
		"http://%s/%s/%s",
		firstHop,
		u.bucketName,
		objectName,
	)
	logger.Info("construct upload URL",
		slog.String("pre", pre),
		slog.String("url", url),
		slog.String("firstHop", firstHop))

	// ---------------------- 5. Generate GCP Access Token ----------------------
	//logger.Info("start to generate GCP access token", slog.String("pre", pre))
	//jsonBytes, err := os.ReadFile(u.credFile)
	//if err != nil {
	//	logger.Error("read cred file failed",
	//		slog.String("pre", pre),
	//		slog.String("credFile", u.credFile),
	//		slog.Any("err", err))
	//	return fmt.Errorf("read cred file: %w", err)
	//}
	//
	//reds, err := google.CredentialsFromJSON(ctx, jsonBytes, gcpScopes)
	//if err != nil {
	//	logger.Error("Parse GCP credentials failed",
	//		slog.String("pre", pre),
	//		slog.Any("err", err))
	//	return fmt.Errorf("parse credentials: %w", err)
	//}
	//
	//token, err := reds.TokenSource.Token()
	//if err != nil {
	//	logger.Error("Get GCP token failed",
	//		slog.String("pre", pre),
	//		slog.Any("err", err))
	//	return fmt.Errorf("get token: %w", err)
	//}
	//logger.Info("GCP access token generated successfully", slog.String("pre", pre))

	var token string
	if len(u.token) >= 0 {
		token = u.token
	} else {
		var err error
		token, err = util.GetGCPShortToken(ctx, u.credFile, pre, logger)
		if err != nil {
			logger.Error("Get GCP token failed",
				slog.String("pre", pre),
				slog.Any("err", err))
			return fmt.Errorf("get token: %w", err)
		}
	}

	// ---------------------- 6. Build and send HTTP request ----------------------
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, rateLimitedBody)
	if err != nil {
		logger.Error("Create HTTP request failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return fmt.Errorf("new request: %w", err)
	}

	// 设置请求头
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(util.HeaderXHops, hops)
	req.Header.Set(util.HeaderXChunkIndex, "1")
	req.Header.Set(util.HeaderXRateLimitEnable, "true")
	req.Header.Set(util.HeaderDestType, util.GCSCLoud)
	logger.Info("HTTP request headers set", slog.String("pre", pre))

	// 发送请求
	client := &http.Client{Timeout: 1 * time.Minute}
	logger.Info("Send HTTP request to proxy",
		slog.String("pre", pre),
		slog.String("url", url),
		slog.String("timeout", "5m"))

	resp, err := client.Do(req)
	if err != nil {
		logger.Error("HTTP request failed",
			slog.String("pre", pre),
			slog.String("url", url),
			slog.Any("err", err))
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close() // Ensure response body closes

	// ---------------------- 7. Validate response status ----------------------
	if resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			respBody = []byte("failed to read response body")
			logger.Error("Read error response failed",
				slog.String("pre", pre),
				slog.Any("err", readErr))
		}
		err := fmt.Errorf("upload failed: %d %s", resp.StatusCode, string(respBody))
		logger.Error("Upload to GCS by proxy failed",
			slog.String("pre", pre),
			slog.Int("statusCode", resp.StatusCode),
			slog.String("response", string(respBody)))
		return err
	}

	logger.Info("UploadToGCSbyProxy success",
		slog.String("pre", pre),
		slog.String("objectName", objectName),
		slog.String("url", url))

	return nil
}
