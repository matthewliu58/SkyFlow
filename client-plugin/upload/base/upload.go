package base

import (
	"encoding/json"
	"log/slog"
)

type Chunk struct {
	Upload string `json:"upload" form:"upload"` // Upload API endpoint
	Merge  string `json:"merge" form:"merge"`   // Merge API endpoint
}

func ExtractChunkFromInterface(obj interface{}, pre string, logger *slog.Logger) *Chunk {
	if obj == nil {
		logger.Error("Chunk interface is nil", slog.String("pre", pre))
		return nil
	}

	// Convert to JSON first
	data, err := json.Marshal(obj)
	if err != nil {
		logger.Error("marshal chunk interface failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	// Then deserialize to Chunk
	var ck Chunk
	if err := json.Unmarshal(data, &ck); err != nil {
		logger.Error("unmarshal to Chunk failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	return &ck
}
