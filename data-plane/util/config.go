package util

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

var Config_ *Config

// Config top-level struct, corresponds to yaml flat configuration
type Config struct {
	EnvoyPath     string `yaml:"envoy_path"`
	ControlHost   string `yaml:"control_host"`
	DefaultConfig string `yaml:"default_config"`
	EnvoyLog      string `yaml:"envoy_log"`
}

// ReadYamlConfig reads config.yaml from the same directory
func ReadYamlConfig(logger *slog.Logger) (*Config, error) {
	// 1. Get current executable directory (ensure same level as config.yaml)
	exePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("get executable path failed: %w", err)
	}
	exeDir := filepath.Dir(exePath)                    // Executable directory
	configPath := filepath.Join(exeDir, "config.yaml") // Join path to config.yaml at same level

	// 2. Verify config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s (ensure config.yaml is in same directory)", configPath)
	}

	// 3. Read config file content
	content, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read config file failed: %w", err)
	}

	// 4. Parse yaml to struct
	var config Config
	if err := yaml.Unmarshal(content, &config); err != nil {
		return nil, fmt.Errorf("yaml parse failed: %w", err)
	}

	return &config, nil
}
