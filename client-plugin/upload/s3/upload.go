package s3

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	limit_rate "rigel-client/limit-rate"
	"rigel-client/util"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	awsService = "s3"
)

type Upload struct {
	localBaseDir string // Local base directory (used in disk mode)
	bucketName   string // S3 bucket name
	region       string // AWS region
	accessKey    string // AWS Access Key ID
	secretKey    string // AWS Secret Access Key
	endpoint     string // Empty = AWS official
	usePathStyle bool
}

// NewUpload initializes AWS S3 Upload instance
func NewUpload(
	localBaseDir, bucketName, region, accessKey, secretKey, endpoint string,
	usePathStyle bool,
	pre string, // Log prefix
	logger *slog.Logger,
) *Upload {
	u := &Upload{
		localBaseDir: localBaseDir,
		bucketName:   bucketName,
		region:       region,
		accessKey:    accessKey,
		secretKey:    secretKey,
		endpoint:     endpoint,
		usePathStyle: usePathStyle,
	}
	// Same logging logic as GCP
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

	logger.Info("UploadToS3byProxy start", slog.String("pre", pre), slog.String("hops", hops))

	if len(hops) == 0 {
		err := fmt.Errorf("hops is empty")
		logger.Error("invalid hops", slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("upload canceled")
	default:
	}

	var proxyReader io.ReadCloser = reader
	defer func() {
		if proxyReader != nil && proxyReader != reader {
			_ = proxyReader.Close()
		}
	}()

	if !inMemory {
		localFilePath := filepath.Join(u.localBaseDir, objectName)
		localFilePath = filepath.Clean(localFilePath)
		f, err := os.Open(localFilePath)
		if err != nil {
			logger.Error("open file err", slog.String("pre", pre), slog.Any("err", err))
			return err
		}
		proxyReader = f
	}

	rateLimitedBody := limit_rate.NewRateLimitedReader(ctx, proxyReader, rateLimiter)

	hopList := strings.Split(hops, ",")
	firstHop := hopList[0]

	url := fmt.Sprintf("http://%s/%s/%s", firstHop, u.bucketName, objectName)
	logger.Info("upload url", slog.String("pre", pre), slog.String("url", url))

	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	// Signing key
	signingKey := getSignatureKey(u.secretKey, dateStamp, u.region, awsService)

	canonicalURI := fmt.Sprintf("/%s/%s", u.bucketName, objectName)
	canonicalQueryString := ""

	lastHop := hopList[len(hopList)-1]
	realHost := strings.Split(lastHop, ":")[0]

	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", realHost, amzDate)
	signedHeaders := "host;x-amz-date"
	payloadHash := "UNSIGNED-PAYLOAD"

	canonicalRequest := fmt.Sprintf(
		"PUT\n%s\n%s\n%s\n%s\n%s",
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, u.region, awsService)
	stringToSign := fmt.Sprintf(
		"AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		credentialScope,
		sha256Hex(canonicalRequest),
	)

	mac := hmac.New(sha256.New, signingKey)
	mac.Write([]byte(stringToSign))
	signature := fmt.Sprintf("%x", mac.Sum(nil))

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		u.accessKey, credentialScope,
		signedHeaders,
		signature,
	)
	logger.Info("authHeader", slog.String("pre", pre), slog.String("authHeader", authHeader))

	// PUT method
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, rateLimitedBody)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", authHeader)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = contentLength

	// Proxy headers
	req.Header.Set("X-Pre", pre)
	req.Header.Set(util.HeaderXHops, hops)
	req.Header.Set(util.HeaderXChunkIndex, "1")
	req.Header.Set(util.HeaderXRateLimitEnable, "true")
	req.Header.Set(util.HeaderDestType, util.S3Cloud)

	// Send request
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload fail %d: %s", resp.StatusCode, string(body))
	}

	logger.Info("upload success", slog.String("pre", pre), slog.String("object", objectName))
	return nil
}

// getSignatureKey generates AWS signing key (AWS4 specification)
func getSignatureKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

// hmacSHA256 calculates HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// sha256Hex calculates SHA256 and returns hex string
func sha256Hex(data string) string {
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)
}
