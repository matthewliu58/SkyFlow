package remote

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"rigel-client/util"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

type Download struct {
	user      string // Username
	hostPort  string // Host IP:port (e.g., 192.168.1.20:22)
	password  string // Password (or use key authentication)
	remoteDir string
	localDir  string
}

func NewDownload(
	user, hostPort, password, remoteDir, localDir string,
	pre string, // Log prefix (keep consistent with previous)
	logger *slog.Logger, // Log instance (keep consistent with previous)
) *Download {
	d := &Download{
		user:      user,
		hostPort:  hostPort,
		password:  password,
		remoteDir: remoteDir,
		localDir:  localDir,
	}
	// Same log printing logic as other init functions
	logger.Info("NewDownload", slog.String("pre", pre), slog.Any("Download", *d))
	return d
}

func (d *Download) DownloadFile(
	ctx context.Context,
	filename string,
	newFilename string,
	start int64,
	length int64,
	bs string,
	inMemory bool, // New parameter at the end, doesn't affect existing calls
	pre string,
	logger *slog.Logger,
) (io.ReadCloser, error) {

	select {
	case <-ctx.Done():
		err := fmt.Errorf("upload canceled before start: %w", ctx.Err())
		logger.Error("SSHDDReadRangeChunk canceled", slog.String("pre", pre), slog.Any("err", err))
		return nil, err
	default:
	}

	// 1. Build full remote file path
	remoteFile := filepath.Join(d.remoteDir, filename)

	// 2. Handle length <= 0 (read entire file)
	var actualStart, actualLength int64
	if length <= 0 {
		// Get remote file total size

		gs := NewGetSize(d.user, d.hostPort, d.password, d.remoteDir, pre, logger)
		fileSize, err := gs.GetFileSize(ctx, filename, pre, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to get file size: %w", err)
		}
		actualStart = 0         // Read from beginning
		actualLength = fileSize // Read full file size
		logger.Info("Read entire file",
			slog.String("pre", pre),
			slog.String("remoteFile", remoteFile),
			slog.Int64("fileTotalSize(bytes)", actualLength))
	} else {
		// Validate start (start cannot be negative when length > 0)
		if start < 0 {
			return nil, fmt.Errorf("start cannot be negative (when length > 0)")
		}
		actualStart = start
		actualLength = length
		logger.Info("Read specified range",
			slog.String("pre", pre),
			slog.String("remoteFile", remoteFile),
			slog.Int64("startPosition(bytes)", actualStart),
			slog.Int64("readLength(bytes)", actualLength))
	}

	// 3. Auto select optimal block size (if bs is empty)
	if strings.TrimSpace(bs) == "" {
		bs = util.AutoSelectBs(actualLength)
		//logger.Info("Auto select block size",
		//	slog.String("pre", pre),
		//	slog.String("blockSize", bs))
	}

	// 4. Parse block size to bytes
	bsBytes, err := util.ParseBsToBytes(bs)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block size: %w", err)
	}

	// 5. Calculate dd command params (skip=blocks to skip, count=blocks to read)
	skip := actualStart / bsBytes                   // Blocks to skip for start position
	count := (actualLength + bsBytes - 1) / bsBytes // Blocks to read (rounded up)
	ddCmd := fmt.Sprintf("dd if=%s bs=%s skip=%d count=%d", remoteFile, bs, skip, count)

	// 6. Initialize SSH client config
	sshConfig := &ssh.ClientConfig{
		User:            d.user,
		Auth:            []ssh.AuthMethod{ssh.Password(d.password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Recommend secure key verification in production
		Timeout:         1 * time.Minute,             // Extended timeout for large file reads
	}

	// 7. Establish SSH connection
	client, err := ssh.Dial("tcp", d.hostPort, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("SSH connection failed: %w", err)
	}

	// 8. Create SSH session
	session, err := client.NewSession()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create SSH session: %w", err)
	}

	// 9. Get stdout pipe for dd command
	stdout, err := session.StdoutPipe()
	if err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	// 10. Start dd command
	logger.Info(fmt.Sprintf("Execute dd command (%s mode)", map[bool]string{true: "in-memory stream", false: "local disk"}[inMemory]),
		slog.String("pre", pre),
		slog.String("command", ddCmd))
	if err := session.Start(ddCmd); err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("failed to start dd command: %w, command: %s", err, ddCmd)
	}

	// ==================== Mode 1: inMemory=true -> In-memory stream (no disk write) ====================
	if inMemory {
		logger.Info("Read remote file (in-memory streaming mode, no disk write)",
			slog.String("pre", pre),
			slog.String("remoteFile", remoteFile))

		// Wrap Reader and SSH resources (caller Close releases)
		readerWrapper := &sshReaderWrapper{
			reader:       stdout,
			session:      session,
			client:       client,
			pre:          pre,
			logger:       logger,
			ddCmd:        ddCmd,
			actualLength: actualLength,
			readDone:     false,
		}
		return readerWrapper, nil
	}

	// ==================== Mode 2: inMemory=false -> Local disk write (fully compatible with original logic) ====================
	// 11. Ensure local directory exists
	if err := os.MkdirAll(d.localDir, 0755); err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("failed to create local directory: %w", err)
	}

	// 12. Create local file (overwrite existing)
	localFilePath := filepath.Join(d.localDir, newFilename)
	localFd, err := os.Create(localFilePath)
	if err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("failed to create local file: %w", err)
	}
	defer localFd.Close()

	// 13. Execute dd command and write output to local file
	logger.Info("Write dd output to local file",
		slog.String("pre", pre),
		slog.String("outputFile", localFilePath))

	// Copy dd output to local file at once (original logic)
	_, err = io.Copy(localFd, stdout)
	if err != nil {
		session.Close()
		client.Close()
		return nil, fmt.Errorf("failed to write to local file: %w", err)
	}

	// Wait for dd command to complete
	if err := session.Wait(); err != nil {
		logger.Warn("dd command completed with non-zero exit status",
			slog.String("pre", pre),
			slog.String("error", err.Error()))
	}

	// Close SSH resources (resources consumed in disk mode)
	session.Close()
	client.Close()

	// Verify local file size (original logic)
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		logger.Warn("Failed to get local file info",
			slog.String("pre", pre),
			slog.String("file", localFilePath),
			slog.String("error", err.Error()))
	} else {
		logger.Info("File read completed",
			slog.String("pre", pre),
			slog.String("localFile", localFilePath),
			slog.Int64("fileSize(bytes)", fileInfo.Size()),
			slog.String("expectedSize(bytes)", fmt.Sprintf("%d", actualLength)))
	}

	// Open local file and return Reader (convenient for caller)
	localFileReader, err := os.Open(localFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file: %w", err)
	}

	// Compatible with original return: return local file name (newFilename) and path
	return localFileReader, nil
}

// sshReaderWrapper Wraps stdout Reader + SSH resource cleanup logic (for memory mode)
type sshReaderWrapper struct {
	reader       io.Reader    // dd command stdout pipe (no Close)
	session      *ssh.Session // SSH session
	client       *ssh.Client  // SSH client
	pre          string       // Log prefix
	logger       *slog.Logger // Log object
	ddCmd        string       // dd command
	actualLength int64        // Expected read length
	readDone     bool         // Whether read completed
}

// Read Implements io.Reader interface
func (w *sshReaderWrapper) Read(p []byte) (n int, err error) {
	n, err = w.reader.Read(p)
	if err == io.EOF {
		w.readDone = true
	}
	return n, err
}

// Close 实现io.ReadCloser接口（释放SSH资源）
func (w *sshReaderWrapper) Close() error {
	var errStr []string

	// Wait for dd command to complete
	if !w.readDone {
		if err := w.session.Wait(); err != nil {
			w.logger.Warn("dd command completed with non-zero exit status",
				slog.String("pre", w.pre),
				slog.String("command", w.ddCmd),
				slog.String("error", err.Error()))
		}
	}

	// Close SSH session
	if err := w.session.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("failed to close SSH session: %v", err))
	}

	// Close SSH client
	if err := w.client.Close(); err != nil {
		errStr = append(errStr, fmt.Sprintf("failed to close SSH client: %v", err))
	}

	w.logger.Info("SSH streaming read resources released",
		slog.String("pre", w.pre),
		slog.String("ddCommand", w.ddCmd))

	if len(errStr) > 0 {
		return fmt.Errorf(strings.Join(errStr, "; "))
	}
	return nil
}
