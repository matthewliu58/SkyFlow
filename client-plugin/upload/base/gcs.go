package base

import (
	"encoding/json"
	"log/slog"
)

type GCP struct {
	BucketName string `json:"bucket_name" form:"bucket_name"` // GCP bucket
	Token      string `json:"token" form:"token"`             // GCP access token
	//CredFile   string `json:"cred_file" form:"cred_file"`     // GCP credential file
}

func ExtractGCPFromInterface(obj interface{}, pre string, logger *slog.Logger) *GCP {
	// 1. Convert obj to JSON bytes first
	data, err := json.Marshal(obj)
	if err != nil {
		logger.Error("marshal interface failed", slog.Any("pre", pre), slog.Any("err", err))
		return nil
	}

	// 2. Then deserialize to GCP
	var gcp GCP
	if err := json.Unmarshal(data, &gcp); err != nil {
		logger.Error("unmarshal to GCP failed", slog.Any("pre", pre), slog.Any("err", err))
		return nil
	}

	return &gcp
}
