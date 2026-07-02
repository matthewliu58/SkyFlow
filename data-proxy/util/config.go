package util

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

var (
	Config_ *Config
)

// Config top-level struct, corresponding to yaml top-level configuration
type Config struct {
	Port string `yaml:"port"`
	//Mem  int64  `yaml:"mem"`
}

// ReadYamlConfig read config.yaml from the same directory
func ReadYamlConfig(logger *slog.Logger) (*Config, error) {
	// 1. Get current executable directory (ensure it's at the same level as config.yaml)
	exePath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to get executable path: %w", err)
	}
	exeDir := filepath.Dir(exePath)                    // Executable directory
	configPath := filepath.Join(exeDir, "config.yaml") // Join path to config.yaml at the same level

	// 2. Check if config file exists
	if _, err = os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file does not exist: %s (ensure config.yaml is in the same directory as the executable)", configPath)
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

	return &config, nil
}
