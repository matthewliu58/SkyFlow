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
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *Compose {
	c := &Compose{
		serverURL:    serverURL,
		deleteChunks: deleteChunks,
	}
	// 和其他初始化函数完全一致的日志打印逻辑
	logger.Info("NewCompose", slog.String("pre", pre), slog.Any("Compose", *c))
	return c
}

// ChunkMergeClient 分片合并客户端（直接返回原始响应内容）
// 参数说明：
//
//	serverURL: 服务端接口地址（如 "http://localhost:8080/api/v1/chunk/merge"）
//	finalFileName: 合并后的最终文件名
//	chunkNames: 分片名列表（按合并顺序）
//	deleteChunks: 合并后是否删除分片
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

	// 2. 构造请求（分片合并接口不需要请求体，参数都在Header中）
	req, err := http.NewRequest("POST", c.serverURL, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("创建请求失败: %v", err)
	}

	// 3. 设置请求Header
	// 设置最终文件名
	req.Header.Set("X-File-Name", finalFileName)
	// 设置分片名列表（逗号分隔）
	//req.Header.Set(HeaderChunkNames, strings.Join(chunkNames, ","))
	// 设置是否删除分片
	//req.Header.Set(HeaderDeleteChunks, fmt.Sprintf("%t", deleteChunks))
	// 设置Content-Type（虽然没有请求体，但建议设置）
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	// 4. 创建HTTP客户端并发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 5. 读取响应内容（原始字符串）
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %v", err)
	}

	logger.Info("ChunkMergeClient", slog.String("pre", pre),
		"respBody", string(respBody), "StatusCode", resp.StatusCode)

	// 返回原始响应内容、HTTP状态码
	return nil
}
