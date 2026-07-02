package base

import (
	"encoding/json"
	"log/slog"
)

type AWSConfig struct {
	BucketName   string `json:"bucketName"`   // S3 bucket name
	Region       string `json:"region"`       // AWS region
	AccessKey    string `json:"accessKey"`    // Access Key
	SecretKey    string `json:"secretKey"`    // Secret Key
	Endpoint     string `json:"endpoint"`     // Leave empty for AWS official
	UsePathStyle bool   `json:"usePathStyle"` // Must enable for S3-compatible storage
}

// ExtractAWSFromInterface Extract AWS config from interface (implemented similar to ExtractGCPFromInterface)
func ExtractAWSFromInterface(iface interface{}, pre string, logger *slog.Logger) *AWSConfig {
	if iface == nil {
		logger.Error("AWS interface is nil", slog.String("pre", pre))
		return nil
	}

	// 1. Serialize to JSON first
	data, err := json.Marshal(iface)
	if err != nil {
		logger.Error("marshal aws interface failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	// 2. Deserialize to AWSConfig
	var awsCfg AWSConfig
	if err := json.Unmarshal(data, &awsCfg); err != nil {
		logger.Error("unmarshal to AWSConfig failed", slog.String("pre", pre), slog.Any("err", err))
		return nil
	}

	// 3. Validate required fields
	if awsCfg.BucketName == "" || awsCfg.Region == "" || awsCfg.AccessKey == "" || awsCfg.SecretKey == "" {
		logger.Error("AWS config missing required fields", slog.String("pre", pre),
			slog.String("bucket", awsCfg.BucketName),
			slog.String("region", awsCfg.Region))
		return nil
	}

	return &awsCfg
}
