package remote

import (
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type GetSize struct {
	user      string // 用户名
	hostPort  string // 主机IP:端口（如192.168.1.20:22）
	password  string // 密码（或用密钥认证）
	remoteDir string
}

func NewGetSize(
	user, hostPort, password, remoteDir string,
	pre string, // 日志前缀（和之前保持一致）
	logger *slog.Logger, // 日志实例（和之前保持一致）
) *GetSize {
	gs := &GetSize{
		user:      user,
		hostPort:  hostPort,
		password:  password,
		remoteDir: remoteDir,
	}
	// 和其他初始化函数完全一致的日志打印逻辑
	logger.Info("NewGetSize", slog.String("pre", pre), slog.Any("GetSize", *gs))
	return gs
}

func (g *GetSize) GetFileSize(ctx context.Context, filename string, pre string, logger *slog.Logger) (int64, error) {

	remoteFile := filepath.Join(g.remoteDir, filename)
	logger.Info("Start get remote file size",
		slog.String("pre", pre),
		slog.String("remoteFile", remoteFile),
		slog.String("host", g.hostPort))

	// 2. 监听ctx取消信号，提前终止操作
	select {
	case <-ctx.Done():
		err := fmt.Errorf("get remote file size canceled: %w", ctx.Err())
		logger.Error("GetRemoteFileSize canceled before connect", slog.String("pre", pre), slog.Any("err", err))
		return 0, err
	default:
	}

	// 1. 初始化SSH配置
	sshConfig := &ssh.ClientConfig{
		User:            g.user,
		Auth:            []ssh.AuthMethod{ssh.Password(g.password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 生产环境建议替换为安全的HostKey校验
		Timeout:         5 * time.Second,
	}

	// 2. 建立SSH连接（全局连接，可复用）
	client, err := ssh.Dial("tcp", g.hostPort, sshConfig)
	if err != nil {
		logger.Error("SSH dial failed", slog.String("pre", pre), slog.Any("err", err))
		return 0, fmt.Errorf("SSH连接失败：%w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Warn("Close SSH client failed", slog.String("pre", pre), slog.Any("err", err))
		}
	}()

	// 3. 先判断远端系统类型（避免重复执行命令）
	systemType, err := getRemoteSystemType(client, pre, logger)
	if err != nil {
		logger.Warn("Get remote system type failed, use default stat command",
			slog.String("pre", pre), slog.Any("err", err))
	}

	// 4. 根据系统类型执行对应stat命令（新建独立Session）
	var cmd string
	switch systemType {
	case "darwin": // macOS
		cmd = fmt.Sprintf("stat -f %%z '%s'", remoteFile)
	default: // Linux (centos/ubuntu)
		cmd = fmt.Sprintf("stat -c %%s '%s'", remoteFile)
	}

	// 新建Session执行命令（核心修复：每次命令用新Session）
	fileSize, err := executeSSHCommand(client, cmd, pre, logger)
	if err != nil {
		// 降级重试：如果系统类型判断错误，尝试另一种命令（仍用新Session）
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
			return 0, fmt.Errorf("执行stat命令失败：%w", err)
		}
	}

	logger.Info("Get remote file size success",
		slog.String("pre", pre),
		slog.String("remoteFile", remoteFile),
		slog.Int64("fileSize", fileSize))

	return fileSize, nil
}

// getRemoteSystemType 获取远端系统类型（linux/darwin）
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

// executeSSHCommand 执行SSH命令（每次新建Session，避免Stdout冲突）
func executeSSHCommand(client *ssh.Client, cmd string, pre string, logger *slog.Logger) (int64, error) {
	// 新建独立Session
	session, err := client.NewSession()
	if err != nil {
		return 0, fmt.Errorf("创建SSH会话失败：%w", err)
	}
	// 确保Session关闭（即使执行失败）
	defer func() {
		if err := session.Close(); err != nil {
			logger.Warn("Close command session failed", slog.String("pre", pre), slog.Any("err", err))
		}
	}()

	// 执行命令（CombinedOutput会自动绑定Stdout/Stderr，且仅执行一次）
	output, err := session.CombinedOutput(cmd)
	if err != nil {
		return 0, fmt.Errorf("命令执行失败：%w，输出：%s", err, string(output))
	}

	// 解析文件大小
	sizeStr := strings.TrimSpace(string(output))
	fileSize, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析文件大小失败：%w，原始输出：%s", err, sizeStr)
	}

	return fileSize, nil
}
