package probing

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	ProbingTaskURL = "/api/v1/probe/tasks"
)

type ProbeTask struct {
	TargetType string `json:"TargetType"`
	Provider   string `json:"Provider"`
	IP         string `json:"IP"`
	Port       int    `json:"Port"`
	Region     string `json:"Region"`
	ID         string `json:"ID"`
}

func GetProbeTasks(pre, controlHost string) ([]ProbeTask, error) {

	url := controlHost + ProbingTaskURL

	// 1. Send HTTP GET request
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// 2. Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	// 3. Define server response structure
	var serverResp struct {
		Code int         `json:"code"`
		Msg  string      `json:"msg"`
		Data []ProbeTask `json:"data"`
	}

	// 4. Parse JSON
	if err := json.Unmarshal(body, &serverResp); err != nil {
		return nil, fmt.Errorf("json parse failed: %w", err)
	}

	// 5. Check response code
	if serverResp.Code != 200 {
		return nil, fmt.Errorf("api error: %d %s", serverResp.Code, serverResp.Msg)
	}

	// 6. Return node list
	return serverResp.Data, nil
}
