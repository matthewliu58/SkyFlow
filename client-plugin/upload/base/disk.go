package base

import (
	"encoding/json"
	"log/slog"
)

type SourceDisk struct {
	User      string `json:"ssh_user" form:"ssh_user"`             // SSH username
	Host      string `json:"ssh_host" form:"ssh_host"`             // SSH host IP
	Port      string `json:"ssh_port" form:"ssh_port"`             // SSH port
	Password  string `json:"ssh_password" form:"ssh_password"`     // SSH password
	RemoteDir string `json:"ssh_remote_dir" form:"ssh_remote_dir"` // Remote file directory
}

func ExtractSourceDiskFromInterface(obj interface{}, pre string, logger *slog.Logger) *SourceDisk {
	if obj == nil {
		logger.Error("SourceDisk interface is nil", slog.String("pre", pre))
		return nil
	}

	// 1. Serialize interface{} to JSON
	data, err := json.Marshal(obj)
	if err != nil {
		logger.Error("marshal SourceDisk interface failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	// 2. Deserialize to struct
	var sd SourceDisk
	if err := json.Unmarshal(data, &sd); err != nil {
		logger.Error("unmarshal to SourceDisk failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	return &sd
}
