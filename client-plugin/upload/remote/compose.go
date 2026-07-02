package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log/slog"
	"net/http"
	"rigel-client/util"
	"time"
)

type Compose struct {
	serverURL    string
	deleteChunks bool
}

func NewCompose(
	serverURL string,
	deleteChunks bool,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *Compose {
	c := &Compose{
		serverURL:    serverURL,
		deleteChunks: deleteChunks,
	}
	// Same log printing logic as other init functions
	logger.Info("NewCompose", slog.String("pre", pre), slog.Any("Compose", *c))
	return c
}

// ChunkMergeClient Chunk merge client (returns raw response)
// Parameter description:
//
//	serverURL: Server API address (e.g., "http://localhost:8080/api/v1/chunk/merge")
//	finalFileName: Final merged file name
//	chunkNames: Chunk names list (in merge order)
//	deleteChunks: Whether to delete chunks after merge
func (c *Compose) ComposeFile(ctx context.Context,
	objectName string,
	parts []string,
	pre string, logger *slog.Logger) error {

	finalFileName := objectName
	chunkNames := parts

	logger.Info("ChunkMergeClient", slog.String("pre", pre),
		slog.String("finalFileName", finalFileName), slog.Any("chunkNames", chunkNames))

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled: %w", ctx.Err())
		logger.Error("ChunkMergeClient canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return err
	default:
	}

	mergeReq := util.ChunkMergeRequest{
		FinalFileName: finalFileName,
		ChunkNames:    chunkNames,
		DeleteChunks:  c.deleteChunks,
	}
	b, _ := json.Marshal(mergeReq)

	// 2. Build request (chunk merge API doesn't need request body, params in Header)
	req, err := http.NewRequest("POST", c.serverURL, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// 3. Set request headers
	// Set final file name
	req.Header.Set("X-File-Name", finalFileName)
	// Set chunk names list (comma separated)
	//req.Header.Set(HeaderChunkNames, strings.Join(chunkNames, ","))
	// Set whether to delete chunks
	//req.Header.Set(HeaderDeleteChunks, fmt.Sprintf("%t", deleteChunks))
	// Set Content-Type (recommended even without body)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	// 4. Create HTTP client and send request
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// 5. Read response body (raw string)
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	logger.Info("ChunkMergeClient", slog.String("pre", pre),
		"respBody", string(respBody), "StatusCode", resp.StatusCode)

	// Return raw response content, HTTP status code
	return nil
}
