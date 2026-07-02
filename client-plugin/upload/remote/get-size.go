package remote

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

type GetSize struct {
	user      string // Username
	hostPort  string // Host IP:port (e.g., 192.168.1.20:22)
	password  string // Password (or use key authentication)
	remoteDir string
}

func NewGetSize(
	user, hostPort, password, remoteDir string,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *GetSize {
	gs := &GetSize{
		user:      user,
		hostPort:  hostPort,
		password:  password,
		remoteDir: remoteDir,
	}
	// Same log printing logic as other init functions
	logger.Info("NewGetSize", slog.String("pre", pre), slog.Any("GetSize", *gs))
	return gs
}

func (g *GetSize) GetFileSize(ctx context.Context, filename string, pre string, logger *slog.Logger) (int64, error) {

	remoteFile := filepath.Join(g.remoteDir, filename)
	logger.Info("Start get remote file size",
		slog.String("pre", pre),
		slog.String("remoteFile", remoteFile),
		slog.String("host", g.hostPort))

	// 2. Listen for ctx cancel signal, terminate early
	select {
	case <-ctx.Done():
		err := fmt.Errorf("get remote file size canceled: %w", ctx.Err())
		logger.Error("GetRemoteFileSize canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return 0, err
	default:
	}

	// 1. Initialize SSH config
	sshConfig := &ssh.ClientConfig{
		User:            g.user,
		Auth:            []ssh.AuthMethod{ssh.Password(g.password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Recommend secure HostKey verification in production
		Timeout:         5 * time.Second,
	}

	// 2. Establish SSH connection (global connection, reusable)
	client, err := ssh.Dial("tcp", g.hostPort, sshConfig)
	if err != nil {
		logger.Error("SSH dial failed", slog.String("pre", pre), slog.Any("err", err))
		return 0, fmt.Errorf("SSH connection failed: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Warn("Close SSH client failed", slog.String("pre", pre), slog.Any("err", err))
		}
	}()

	// 3. Detect remote system type first (avoid duplicate command execution)
	systemType, err := getRemoteSystemType(client, pre, logger)
	if err != nil {
		logger.Warn("Get remote system type failed, use default stat command",
			slog.String("pre", pre), slog.Any("err", err))
	}

	// 4. Execute corresponding stat command based on system type (new independent Session)
	var cmd string
	switch systemType {
	case "darwin": // macOS
		cmd = fmt.Sprintf("stat -f %%z '%s'", remoteFile)
	default: // Linux (centos/ubuntu)
		cmd = fmt.Sprintf("stat -c %%s '%s'", remoteFile)
	}

	// New Session for command execution (core fix: use new Session per command)
	fileSize, err := executeSSHCommand(client, cmd, pre, logger)
	if err != nil {
		// Fallback retry: if system type detection failed, try alternative command (still use new Session)
		retryCmd := ""
		if systemType == "darwin" {
			retryCmd = fmt.Sprintf("stat -c %%s '%s'", remoteFile)
		} else {
			retryCmd = fmt.Sprintf("stat -f %%z '%s'", remoteFile)
		}
		logger.Warn("First stat command failed, retry with fallback cmd",
			slog.String("pre", pre),
			slog.String("originCmd", cmd),
			slog.String("retryCmd", retryCmd),
			slog.Any("err", err))

		fileSize, err = executeSSHCommand(client, retryCmd, pre, logger)
		if err != nil {
			logger.Error("All stat command failed", slog.String("pre", pre), slog.Any("err", err))
			return 0, fmt.Errorf("failed to execute stat command: %w", err)
		}
	}

	logger.Info("Get remote file size success",
		slog.String("pre", pre),
		slog.String("remoteFile", remoteFile),
		slog.Int64("fileSize", fileSize))

	return fileSize, nil
}

// getRemoteSystemType Get remote system type (linux/darwin)
func getRemoteSystemType(client *ssh.Client, pre string, logger *slog.Logger) (string, error) {
	cmd := "uname -s"
	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("create session failed: %w", err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			logger.Warn("Close system type session failed", slog.String("pre", pre), slog.Any("err", err))
		}
	}()

	output, err := session.CombinedOutput(cmd)
	if err != nil {
		return "", fmt.Errorf("execute uname command failed: %w, output: %s", err, string(output))
	}

	systemType := strings.ToLower(strings.TrimSpace(string(output)))
	return systemType, nil
}

// executeSSHCommand Execute SSH command (new Session per call, avoid Stdout conflict)
func executeSSHCommand(client *ssh.Client, cmd string, pre string, logger *slog.Logger) (int64, error) {
	// Create new independent Session
	session, err := client.NewSession()
	if err != nil {
		return 0, fmt.Errorf("failed to create SSH session: %w", err)
	}
	// Ensure Session closes (even on failure)
	defer func() {
		if err := session.Close(); err != nil {
			logger.Warn("Close command session failed", slog.String("pre", pre), slog.Any("err", err))
		}
	}()

	// Execute command (CombinedOutput auto binds Stdout/Stderr, executes once only)
	output, err := session.CombinedOutput(cmd)
	if err != nil {
		return 0, fmt.Errorf("command execution failed: %w, output: %s", err, string(output))
	}

	// Parse file size
	sizeStr := strings.TrimSpace(string(output))
	fileSize, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse file size: %w, raw output: %s", err, sizeStr)
	}

	return fileSize, nil
}
