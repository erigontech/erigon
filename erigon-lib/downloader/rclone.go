package downloader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/log/v3"
	"github.com/spaolacci/murmur3"
	"golang.org/x/sync/errgroup"
)

type rcloneInfo struct {
	sync.Mutex
	file       string
	snapInfo   *snaptype.FileInfo
	remoteInfo remoteInfo
	localInfo  fs.FileInfo
}

func (i *rcloneInfo) Version() uint8 {
	if i.snapInfo != nil {
		return i.snapInfo.Version
	}

	return 0
}

func (i *rcloneInfo) From() uint64 {
	if i.snapInfo != nil {
		return i.snapInfo.From
	}

	return 0
}

func (i *rcloneInfo) To() uint64 {
	if i.snapInfo != nil {
		return i.snapInfo.To
	}

	return 0
}

func (i *rcloneInfo) Type() snaptype.Type {
	if i.snapInfo != nil {
		return i.snapInfo.T
	}

	return snaptype.Unknown
}

type RCloneClient struct {
	rclone        *exec.Cmd
	rcloneUrl     string
	rcloneSession *http.Client
	logger        log.Logger
}

func (c *RCloneClient) start(logger log.Logger) error {
	c.logger = logger

	rclone, _ := exec.LookPath("rclone")

	if len(rclone) == 0 {
		logger.Warn("[rclone] Uploading disabled: rclone not found in PATH")
		return fmt.Errorf("rclone not found in PATH")
	}

	if p, err := freePort(); err == nil {
		ctx, cancel := context.WithCancel(context.Background())

		addr := fmt.Sprintf("127.0.0.1:%d", p)
		c.rclone = exec.CommandContext(ctx, rclone, "rcd", "--rc-addr", addr, "--rc-no-auth")
		c.rcloneUrl = "http://" + addr
		c.rcloneSession = &http.Client{} // no timeout - we're doing sync calls

		if err := c.rclone.Start(); err != nil {
			cancel()
			logger.Warn("[rclone] Uploading disabled: rclone didn't start", "err", err)
			return fmt.Errorf("rclone didn't start: %w", err)
		} else {
			logger.Info("[rclone] rclone started", "addr", addr)
		}

		go func() {
			signalCh := make(chan os.Signal, 1)
			signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

			switch s := <-signalCh; s {
			case syscall.SIGTERM, syscall.SIGINT:
				cancel()
			}
		}()
	}

	return nil
}

func (c *RCloneClient) ListRemotes(ctx context.Context) ([]string, error) {
	result, err := c.cmd(ctx, "config/listremotes", nil)

	if err != nil {
		return nil, err
	}

	remotes := struct {
		Remotes []string `json:"remotes"`
	}{}

	err = json.Unmarshal(result, &remotes)

	if err != nil {
		return nil, err
	}

	return remotes.Remotes, nil
}

func (u *RCloneClient) sync(ctx context.Context, request *rcloneRequest) error {
	_, err := u.cmd(ctx, "sync/sync", request)
	return err
}

/*
return retryConnects(ctx, func(ctx context.Context) error {
	return client.CallContext(ctx, result, string(method), args...)
})
}
*/

func isConnectionError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial"
	}
	return false
}

const connectionTimeout = time.Second * 5

func retry(ctx context.Context, op func(context.Context) error, isRecoverableError func(error) bool, delay time.Duration, lastErr error) error {
	err := op(ctx)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) && lastErr != nil {
		return lastErr
	}
	if !isRecoverableError(err) {
		return err
	}

	delayTimer := time.NewTimer(delay)
	select {
	case <-delayTimer.C:
		return retry(ctx, op, isRecoverableError, delay, err)
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return err
		}
		return ctx.Err()
	}
}

func (u *RCloneClient) cmd(ctx context.Context, path string, args interface{}) ([]byte, error) {
	requestBody, err := json.Marshal(args)

	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		u.rcloneUrl+"/"+path, bytes.NewBuffer(requestBody))

	if err != nil {
		return nil, err
	}

	request.Header.Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(ctx, connectionTimeout)
	defer cancel()

	var response *http.Response

	err = retry(ctx, func(ctx context.Context) error {
		response, err = u.rcloneSession.Do(request) //nolint:bodyclose
		return err
	}, isConnectionError, time.Millisecond*200, nil)

	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		responseBody := struct {
			Error string `json:"error"`
		}{}

		if err := json.NewDecoder(response.Body).Decode(&responseBody); err == nil && len(responseBody.Error) > 0 {
			u.logger.Warn("[rclone] cmd failed", "path", path, "status", response.Status, "err", responseBody.Error)
			return nil, fmt.Errorf("cmd: %s failed: %s: %s", path, response.Status, responseBody.Error)
		} else {
			u.logger.Warn("[rclone] cmd failed", "path", path, "status", response.Status)
			return nil, fmt.Errorf("cmd: %s failed: %s", path, response.Status)
		}
	}

	return io.ReadAll(response.Body)
}

type RCloneSession struct {
	*RCloneClient
	sync.Mutex
	files           map[string]*rcloneInfo
	oplock          sync.Mutex
	remoteFs        string
	localFs         string
	syncQueue       chan syncRequest
	syncScheduled   atomic.Bool
	activeSyncCount atomic.Int32
	cancel          context.CancelFunc
}

var rcClient RCloneClient
var rcClientStart sync.Once

func NewRCloneClient(logger log.Logger) (*RCloneClient, error) {
	var err error

	rcClientStart.Do(func() {
		err = rcClient.start(logger)
	})

	if err != nil {
		return nil, err
	}

	return &rcClient, nil
}

func freePort() (port int, err error) {
	if a, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0"); err != nil {
		return 0, err
	} else {
		if l, err := net.ListenTCP("tcp", a); err != nil {
			return 0, err
		} else {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
}

func (c *RCloneClient) NewSession(ctx context.Context, localFs string, remoteFs string) (*RCloneSession, error) {
	ctx, cancel := context.WithCancel(ctx)

	session := &RCloneSession{
		RCloneClient: c,
		files:        map[string]*rcloneInfo{},
		remoteFs:     remoteFs,
		localFs:      localFs,
		cancel:       cancel,
		syncQueue:    make(chan syncRequest, 100),
	}

	go func() {
		if _, err := session.ReadRemoteDir(ctx, true); err == nil {
			session.syncFiles(ctx)
		}
	}()

	return session, nil
}

func (c *RCloneSession) RemoteFsRoot() string {
	return c.remoteFs
}

func (c *RCloneSession) LocalFsRoot() string {
	return c.localFs
}

func (c *RCloneSession) Stop() {
	c.cancel()
}

type syncRequest struct {
	ctx       context.Context
	info      map[string]*rcloneInfo
	cerr      chan error
	request   *rcloneRequest
	retryTime time.Duration
}

func (c *RCloneSession) Upload(ctx context.Context, files ...string) error {
	c.Lock()

	reqInfo := map[string]*rcloneInfo{}

	for _, file := range files {
		info, ok := c.files[file]

		if !ok || info.localInfo == nil {
			localInfo, err := os.Stat(filepath.Join(c.localFs, file))

			if err != nil {
				c.Unlock()
				return fmt.Errorf("can't upload: %s: %w", file, err)
			}

			if !localInfo.Mode().IsRegular() || localInfo.Size() == 0 {
				c.Unlock()
				return fmt.Errorf("can't upload: %s: %s", file, "file is not uploadable")
			}

			if ok {
				info.localInfo = localInfo
			} else {
				info := &rcloneInfo{
					file:      file,
					localInfo: localInfo,
				}

				if snapInfo, ok := snaptype.ParseFileName(c.localFs, file); ok {
					info.snapInfo = &snapInfo
				}

				c.files[file] = info
			}
		} else {
			reqInfo[file] = info
		}
	}

	c.Unlock()

	cerr := make(chan error, 1)

	c.syncQueue <- syncRequest{ctx, reqInfo, cerr,
		&rcloneRequest{
			Group: c.Label(),
			SrcFs: c.localFs,
			DstFs: c.remoteFs,
			Filter: rcloneFilter{
				IncludeRule: files,
			}}, 0}

	return <-cerr
}

func (c *RCloneSession) Download(ctx context.Context, files ...string) error {
	c.Lock()

	if len(c.files) == 0 {
		c.Unlock()
		_, err := c.ReadRemoteDir(ctx, false)
		if err != nil {
			return fmt.Errorf("can't download: %s: %w", files, err)
		}
		c.Lock()
	}

	reqInfo := map[string]*rcloneInfo{}

	for _, file := range files {
		info, ok := c.files[file]

		if !ok || info.remoteInfo.Size == 0 {
			c.Unlock()
			return fmt.Errorf("can't download: %s: %w", file, os.ErrNotExist)
		}

		reqInfo[file] = info
	}

	c.Unlock()

	cerr := make(chan error, 1)

	c.syncQueue <- syncRequest{ctx, reqInfo, cerr,
		&rcloneRequest{
			SrcFs: c.remoteFs,
			DstFs: c.localFs,
			Filter: rcloneFilter{
				IncludeRule: files,
			}}, 0}

	return <-cerr
}

func (c *RCloneSession) Cat(ctx context.Context, file string) (io.Reader, error) {
	rclone, err := exec.LookPath("rclone")

	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, rclone, "cat", c.remoteFs+"/"+file)

	stdout, err := cmd.StdoutPipe()

	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return stdout, nil
}

func (c *RCloneSession) ReadLocalDir(ctx context.Context) ([]fs.DirEntry, error) {
	return os.ReadDir(c.localFs)
}

func (c *RCloneSession) Label() string {
	return strconv.FormatUint(murmur3.Sum64([]byte(c.localFs+"<->"+c.remoteFs)), 36)
}

type remoteInfo struct {
	Name    string
	Size    uint64
	ModTime time.Time
}

type SnapInfo interface {
	Version() uint8
	From() uint64
	To() uint64
	Type() snaptype.Type
}

type fileInfo struct {
	*rcloneInfo
}

func (fi *fileInfo) Name() string {
	return fi.file
}

func (fi *fileInfo) Size() int64 {
	return int64(fi.remoteInfo.Size)
}

func (fi *fileInfo) Mode() fs.FileMode {
	return fs.ModeIrregular
}

func (fi *fileInfo) ModTime() time.Time {
	return fi.remoteInfo.ModTime
}

func (fi *fileInfo) IsDir() bool {
	return false
}

func (fi *fileInfo) Sys() any {
	return fi.rcloneInfo
}

type dirEntry struct {
	info *fileInfo
}

func (e dirEntry) Name() string {
	return e.info.Name()
}

func (e dirEntry) IsDir() bool {
	return e.info.IsDir()
}

func (e dirEntry) Type() fs.FileMode {
	return e.info.Mode()
}

func (e dirEntry) Info() (fs.FileInfo, error) {
	return e.info, nil
}

var ErrAccessDenied = errors.New("access denied")

func (c *RCloneSession) ReadRemoteDir(ctx context.Context, refresh bool) ([]fs.DirEntry, error) {
	if len(c.remoteFs) == 0 {
		return nil, fmt.Errorf("remote fs undefined")
	}

	c.oplock.Lock()
	defer c.oplock.Unlock()

	c.Lock()
	fileCount := len(c.files)
	c.Unlock()

	if fileCount == 0 || refresh {
		listBody, err := json.Marshal(struct {
			Fs     string `json:"fs"`
			Remote string `json:"remote"`
		}{
			Fs:     c.remoteFs,
			Remote: "",
		})

		if err != nil {
			return nil, fmt.Errorf("can't marshal list request: %w", err)
		}

		listRequest, err := http.NewRequestWithContext(ctx, http.MethodPost,
			c.rcloneUrl+"/operations/list", bytes.NewBuffer(listBody))

		if err != nil {
			return nil, fmt.Errorf("can't create list request: %w", err)
		}

		listRequest.Header.Set("Content-Type", "application/json")

		var response *http.Response

		for i := 0; i < 10; i++ {
			response, err = c.rcloneSession.Do(listRequest) //nolint:bodyclose
			if err == nil {
				break
			}
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			return nil, fmt.Errorf("can't get remote list: %w", err)
		}

		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(response.Body)
			e := struct {
				Error string `json:"error"`
			}{}

			if err := json.Unmarshal(body, &e); err == nil {
				if strings.Contains(e.Error, "AccessDenied") {
					return nil, fmt.Errorf("can't get remote list: %w", ErrAccessDenied)
				}
			}

			return nil, fmt.Errorf("can't get remote list: %s: %s", response.Status, string(body))
		}

		responseBody := struct {
			List []remoteInfo `json:"list"`
		}{}

		if err := json.NewDecoder(response.Body).Decode(&responseBody); err != nil {
			return nil, fmt.Errorf("can't decode remote list: %w", err)
		}

		for _, fi := range responseBody.List {
			localInfo, _ := os.Stat(filepath.Join(c.localFs, fi.Name))

			c.Lock()
			if rcinfo, ok := c.files[fi.Name]; ok {
				rcinfo.localInfo = localInfo
				rcinfo.remoteInfo = fi

				if snapInfo, ok := snaptype.ParseFileName(c.localFs, fi.Name); ok {
					rcinfo.snapInfo = &snapInfo
				} else {
					rcinfo.snapInfo = nil
				}

			} else {
				info := &rcloneInfo{
					file:       fi.Name,
					localInfo:  localInfo,
					remoteInfo: fi,
				}

				if snapInfo, ok := snaptype.ParseFileName(c.localFs, fi.Name); ok {
					info.snapInfo = &snapInfo
				}

				c.files[fi.Name] = info
			}
			c.Unlock()
		}
	}

	var entries = make([]fs.DirEntry, 0, len(c.files))

	for _, info := range c.files {
		if info.remoteInfo.Size > 0 {
			entries = append(entries, &dirEntry{&fileInfo{info}})
		}
	}

	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	return entries, nil
}

type rcloneFilter struct {
	IncludeRule []string `json:"IncludeRule"`
}

type rcloneRequest struct {
	Async  bool                   `json:"_async,omitempty"`
	Config map[string]interface{} `json:"_config,omitempty"`
	Group  string                 `json:"group"`
	SrcFs  string                 `json:"srcFs"`
	DstFs  string                 `json:"dstFs"`
	Filter rcloneFilter           `json:"_filter"`
}

func (c *RCloneSession) syncFiles(ctx context.Context) {
	if !c.syncScheduled.CompareAndSwap(false, true) {
		return
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)

	minRetryTime := 30 * time.Second
	maxRetryTime := 300 * time.Second

	retry := func(request syncRequest) {
		switch {
		case request.retryTime == 0:
			request.retryTime = minRetryTime
		case request.retryTime < maxRetryTime:
			request.retryTime += request.retryTime
		default:
			request.retryTime = maxRetryTime
		}

		retryTimer := time.NewTicker(request.retryTime)

		select {
		case <-request.ctx.Done():
			request.cerr <- request.ctx.Err()
			return
		case <-retryTimer.C:
		}

		c.Lock()
		syncQueue := c.syncQueue
		c.Unlock()

		if syncQueue != nil {
			syncQueue <- request
		} else {
			request.cerr <- fmt.Errorf("no sync queue available")
		}
	}

	go func() {
		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()

		select {
		case <-gctx.Done():
			if syncCount := int(c.activeSyncCount.Load()) + len(c.syncQueue); syncCount > 0 {
				log.Info("[rclone] Synced files", "processed", fmt.Sprintf("%d/%d", c.activeSyncCount.Load(), syncCount))
			}

			c.Lock()
			syncQueue := c.syncQueue
			c.syncQueue = nil
			c.Unlock()

			if syncQueue != nil {
				close(syncQueue)
			}

			return
		case <-logEvery.C:
			if syncCount := int(c.activeSyncCount.Load()) + len(c.syncQueue); syncCount > 0 {
				log.Info("[rclone] Syncing files", "progress", fmt.Sprintf("%d/%d", c.activeSyncCount.Load(), syncCount))
			}
		}
	}()

	go func() {
		for req := range c.syncQueue {

			if gctx.Err() != nil {
				req.cerr <- gctx.Err()
				continue
			}

			func(req syncRequest) {
				g.Go(func() error {
					c.activeSyncCount.Add(1)

					defer func() {
						c.activeSyncCount.Add(-1)
						if r := recover(); r != nil {
							log.Error("[rclone] snapshot sync failed", "err", r, "stack", dbg.Stack())

							if gctx.Err() != nil {
								req.cerr <- gctx.Err()
							}

							var err error
							var ok bool

							if err, ok = r.(error); ok {
								req.cerr <- fmt.Errorf("snapshot sync failed: %w", err)
							} else {
								req.cerr <- fmt.Errorf("snapshot sync failed: %s", r)
							}

							return
						}
					}()

					if req.ctx.Err() != nil {
						req.cerr <- req.ctx.Err()
						return nil //nolint:nilerr
					}

					if err := c.sync(gctx, req.request); err != nil {

						if gctx.Err() != nil {
							req.cerr <- gctx.Err()
						} else {
							go retry(req)
						}

						return nil //nolint:nilerr
					}

					for _, info := range req.info {
						localInfo, _ := os.Stat(filepath.Join(c.localFs, info.file))

						info.Lock()
						info.localInfo = localInfo
						info.remoteInfo = remoteInfo{
							Name:    info.file,
							Size:    uint64(localInfo.Size()),
							ModTime: localInfo.ModTime(),
						}
						info.Unlock()
					}

					req.cerr <- nil
					return nil
				})
			}(req)
		}

		c.syncScheduled.Store(false)

		if err := g.Wait(); err != nil {
			c.logger.Debug("[rclone] uploading failed", "err", err)
		}
	}()
}
