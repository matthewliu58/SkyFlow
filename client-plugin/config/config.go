package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/util"
)

var (
	Config_  *Config
	PublicIp string
)

// Config is the top-level struct, corresponding to yaml configuration
type Config struct {
	Port              string `yaml:"port"`           // Service port
	ControlHost       string `yaml:"control_host"`   // Control plane API address
	LocalBaseDir      string `yaml:"local_base_dir"` // Local upload directory
	GCPServiceAccount string `yaml:"gcp_service_account"`
}

// ReadYamlConfig reads the config.yaml file in the same directory
func ReadYamlConfig(logger *slog.Logger) (*Config, error) {
	// 1. Get the directory of the current executable (ensures same level as config.yaml)
	exePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to get executable path: %w", err)
	}
	exeDir := filepath.Dir(exePath)                    // Executable directory
	configPath := filepath.Join(exeDir, "config.yaml") // Join config.yaml path at the same level

	// 2. Check if config file exists
	if _, err = os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s (please ensure config.yaml is in the same directory as the executable)", configPath)
	}

	// 3. Read config file content
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 4. Parse yaml into struct
	var config Config
	if err = yaml.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	PublicIp, err = util.GetPublicIP()
	if err != nil {
		logger.Warn("failed to get public IP, will use private IP", slog.Any("err", err))
	}

	return &config, nil
}
