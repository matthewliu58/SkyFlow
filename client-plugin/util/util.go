package util

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/bits"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
)

func GenerateRandomLetters(length int) string {
	rand.Seed(time.Now().UnixNano())
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var result string
	for i := 0; i < length; i++ {
		result += string(letters[rand.Intn(len(letters))])
	}
	return result
}

// AutoSelectChunkSize Auto-select optimal chunk size: ~10 parts + aligned to power of 2 + min 64MB / max 2GB
func AutoSelectChunkSize(totalSize int64) int64 {
	if totalSize <= 0 {
		return 64 << 20
	}

	// 1. Target ~10 parts, rounded calculation
	const targetParts = 10
	chunkSize := (totalSize + targetParts/2) / targetParts

	// 2. Core: align to nearest power of 2 first
	if chunkSize > 0 {
		chunkSize = 1 << bits.Len64(uint64(chunkSize)-1)
	}

	// 3. Finally limit range: min 64MB, max 2GB
	const minChunk = 64 << 20 // 64MB
	const maxChunk = 2 << 30  // 2GB

	if chunkSize < minChunk {
		return minChunk
	}
	if chunkSize > maxChunk {
		return maxChunk
	}
	return chunkSize
}

// AutoSelectBs Strict match, return standard uppercase format
func AutoSelectBs(totalSize int64) string {
	chunkSize := AutoSelectChunkSize(totalSize)
	switch chunkSize {
	case 2 << 30:
		return "2G"
	case 1 << 30:
		return "1G"
	case 512 << 20:
		return "512M"
	case 256 << 20:
		return "256M"
	case 128 << 20:
		return "128M"
	case 64 << 20:
		return "64M"
	default:
		return "64M"
	}
}

// ParseBsToBytes Case-insensitive parsing
func ParseBsToBytes(bs string) (int64, error) {
	bs = strings.TrimSpace(strings.ToLower(bs))
	if bs == "" {
		return 0, errors.New("bs cannot be empty")
	}
	var numPart string
	var unitPart string
	for i, ch := range bs {
		if ch >= '0' && ch <= '9' {
			numPart += string(ch)
		} else {
			unitPart = bs[i:]
			break
		}
	}
	if numPart == "" {
		return 0, fmt.Errorf("invalid bs format: %s", bs)
	}
	num, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil {
		return 0, err
	}
	switch unitPart {
	case "k", "kb":
		return num * 1024, nil
	case "m", "mb":
		return num << 20, nil
	case "g", "gb":
		return num << 30, nil
	case "t", "tb":
		return num << 40, nil
	default:
		return num, nil
	}
}

// SortPartStrings Sort part strings by the number after "part" in ascending order (e.g. aa.part.4 -> sorted by number 4)
func SortPartStrings(parts []string) []string {
	sortedParts := make([]string, len(parts))
	copy(sortedParts, parts)
	sort.Slice(sortedParts, func(i, j int) bool {
		numI := extractPartNumber(sortedParts[i])
		numJ := extractPartNumber(sortedParts[j])
		return numI < numJ
	})
	return sortedParts
}

// extractPartNumber Extract number from part string (e.g. "aa.part.4" -> 4)
func extractPartNumber(s string) int {
	// Split string by "."
	parts := strings.Split(s, ".")
	// Take the last part as number (supports multi-digit numbers like "aa.part.10")
	if len(parts) < 3 {
		return 0 // Return 0 on format error to ensure sorting doesn't panic
	}
	numStr := parts[len(parts)-1]
	// Convert to number, return 0 on failure
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0
	}
	return num
}

func DeleteFilesInDir(dir string, fileNames []string, pre string, logger *slog.Logger) error {
	// 1. Basic parameter validation
	if dir == "" {
		err := errors.New("directory path cannot be empty")
		logger.Error("DeleteFilesInDir, invalid param",
			slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	if len(fileNames) == 0 {
		err := errors.New("file name array cannot be empty")
		logger.Error("DeleteFilesInDir, invalid param",
			slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	//if logger == nil {
	//	err := errors.New("logger instance cannot be empty")
	//	fmt.Printf("error [%s]: %v\n", pre, err) // fallback log
	//	return err
	//}

	// 2. Check if directory exists
	dirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("directory does not exist: %s", dir)
			logger.Error("DeleteFilesInDir, dir check failed",
				slog.String("pre", pre), slog.Any("err", err))
			return err
		}
		err = fmt.Errorf("failed to get directory info: %w", err)
		logger.Error("DeleteFilesInDir, dir stat failed",
			slog.String("pre", pre), slog.Any("err", err))
		return err
	}
	if !dirInfo.IsDir() {
		err = fmt.Errorf("specified path is not a directory: %s", dir)
		logger.Error("DeleteFilesInDir, not a directory",
			slog.String("pre", pre), slog.Any("err", err))
		return err
	}

	// 3. Iterate and delete each file
	var failedFiles []string
	for _, fileName := range fileNames {
		// Skip empty file names
		if strings.TrimSpace(fileName) == "" {
			logger.Warn("DeleteFilesInDir, skip empty filename", slog.String("pre", pre))
			continue
		}

		// Join full file path (auto handle path separator, cross-platform compatible)
		filePath := filepath.Join(dir, fileName)

		// Delete file
		err = os.Remove(filePath)
		if err != nil {
			// Record failed file but don't interrupt the process
			failedMsg := fmt.Sprintf("%s: %v", filePath, err)
			failedFiles = append(failedFiles, failedMsg)
			logger.Warn("DeleteFilesInDir, delete file failed",
				slog.String("pre", pre),
				slog.String("filePath", filePath),
				slog.Any("err", err))
			continue
		}

		logger.Info("DeleteFilesInDir, delete file success",
			slog.String("pre", pre),
			slog.String("filePath", filePath))
	}

	// 4. Handle failed deletions (if any)
	if len(failedFiles) > 0 {
		err = fmt.Errorf("some files failed to delete: %s", strings.Join(failedFiles, "; "))
		logger.Error("DeleteFilesInDir partial delete failed",
			slog.String("pre", pre),
			slog.Any("err", err))
		return err
	}

	logger.Info("DeleteFilesInDir, all files deleted success", slog.String("pre", pre))
	return nil
}

func ReplaceUploadURLHost(originalUploadURL, firstHop string) (string, error) {
	// 1. Validate input parameters
	if originalUploadURL == "" {
		return "", fmt.Errorf("original upload URL is empty")
	}
	if firstHop == "" {
		return "", fmt.Errorf("first hop is empty")
	}

	// 2. Parse original URL (supports with/without protocol)
	var parsedURL *url.URL
	var err error
	if !strings.Contains(originalUploadURL, "://") {
		// URL without protocol, auto prepend http:// (avoid parse failure)
		parsedURL, err = url.Parse("http://" + originalUploadURL)
	} else {
		parsedURL, err = url.Parse(originalUploadURL)
	}
	if err != nil {
		return "", fmt.Errorf("parse original URL failed: %w", err)
	}

	// 3. Replace Host with first hop
	parsedURL.Host = firstHop

	// 4. Restore URL (remove prepended http:// if original had no protocol)
	resultURL := parsedURL.String()
	if !strings.Contains(originalUploadURL, "://") {
		resultURL = strings.TrimPrefix(resultURL, "http://")
	}

	return resultURL, nil
}

func GetPublicIP() (string, error) {
	resp, err := http.Get("https://icanhazip.com")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	buf := make([]byte, 1024)
	n, err := resp.Body.Read(buf)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(buf[:n])), nil
}

//func GetGCPShortToken_(ctx context.Context, credFile, pre string, logger *slog.Logger) (string, error) {
//
//	jsonBytes, err := os.ReadFile(credFile)
//	if err != nil {
//		logger.Error("read cred file failed",
//			slog.String("pre", pre),
//			slog.String("credFile", credFile),
//			slog.Any("err", err))
//		return "", fmt.Errorf("read cred file: %w", err)
//	}
//
//	reds, err := google.CredentialsFromJSON(ctx, jsonBytes,
//		"https://www.googleapis.com/auth/devstorage.full_control")
//	if err != nil {
//		logger.Error("Parse GCP credentials failed",
//			slog.String("pre", pre),
//			slog.Any("err", err))
//		return "", fmt.Errorf("parse credentials: %w", err)
//	}
//
//	token, err := reds.TokenSource.Token()
//	if err != nil {
//		logger.Error("Get GCP token failed",
//			slog.String("pre", pre),
//			slog.Any("err", err))
//		return "", fmt.Errorf("get token: %w", err)
//	}
//
//	return token.AccessToken, nil
//}

func GetGCPShortToken(ctx context.Context, credFile, pre string, logger *slog.Logger) (string, error) {
	jsonBytes, err := os.ReadFile(credFile)
	if err != nil {
		logger.Error("read cred file failed",
			slog.String("pre", pre),
			slog.String("credFile", credFile),
			slog.Any("err", err))
		return "", fmt.Errorf("read cred file: %w", err)
	}

	// 1. Parse JWT config first
	cfg, err := google.JWTConfigFromJSON(jsonBytes, "https://www.googleapis.com/auth/devstorage.full_control")
	if err != nil {
		logger.Error("parse JWT config failed", slog.String("pre", pre), slog.Any("err", err))
		return "", fmt.Errorf("jwt config: %w", err)
	}

	cfg.Expires = 1 * time.Minute

	// 2. Get token
	token, err := cfg.TokenSource(ctx).Token()
	if err != nil {
		logger.Error("get GCP token failed", slog.String("pre", pre), slog.Any("err", err))
		return "", fmt.Errorf("get token: %w", err)
	}

	return token.AccessToken, nil
}
