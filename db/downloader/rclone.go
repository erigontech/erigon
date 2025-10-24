// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/c2h5oh/datasize"
	"github.com/spaolacci/murmur3"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

type rcloneInfo struct {
	sync.Mutex
	file       string
	snapInfo   *snaptype.FileInfo
	remoteInfo remoteInfo
	localInfo  fs.FileInfo
}

func (i *rcloneInfo) Version() snaptype.Version {
	if i.snapInfo != nil {
		return i.snapInfo.Version
	}

	return version.ZeroVersion
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
		return i.snapInfo.Type
	}

	return nil
}

type RCloneClient struct {
	rclone        *exec.Cmd
	rcloneUrl     string
	rcloneSession *http.Client
	logger        log.Logger
	bwLimit       *rate.Limit
	optionsQueue  chan RCloneOptions
}

type RCloneOptions struct {
	BwLimit     string `json:"BwLimit,omitempty"`
	BwLimitFile string `json:"BwLimitFile,omitempty"`
}

func (c *RCloneClient) start(logger log.Logger) error {
	c.logger = logger

	rclone, _ := exec.LookPath("rclone")

	if len(rclone) == 0 {
		return errors.New("rclone not found in PATH")
	}

	logger.Info("[downloader] rclone found in PATH: enhanced upload/download enabled")

	if p, err := freePort(); err == nil {
		ctx, cancel := context.WithCancel(context.Background())

		addr := fmt.Sprintf("127.0.0.1:%d", p)
		c.rclone = exec.CommandContext(ctx, rclone, "rcd", "--rc-addr", addr, "--rc-no-auth", "--multi-thread-streams", "1")
		c.rcloneUrl = "http://" + addr
		c.rcloneSession = &http.Client{} // no timeout - we're doing sync calls
		c.optionsQueue = make(chan RCloneOptions, 100)

		if err := c.rclone.Start(); err != nil {
			cancel()
			logger.Warn("[downloader] Uploading disabled: rclone didn't start", "err", err)
			return fmt.Errorf("rclone didn't start: %w", err)
		} else {
			logger.Info("[downloader] rclone started", "addr", addr)
		}

		go func() {
			signalCh := make(chan os.Signal, 1)
			signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

			for {
				select {
				case s := <-signalCh:
					switch s {
					case syscall.SIGTERM, syscall.SIGINT:
						cancel()
					}
				case o := <-c.optionsQueue:
					c.setOptions(ctx, o)
				}
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

type RCloneTransferStats struct {
	Bytes      uint64  `json:"bytes"`
	Eta        uint    `json:"eta"` // secs
	Group      string  `json:"group"`
	Name       string  `json:"name"`
	Percentage uint    `json:"percentage"`
	Size       uint64  `json:"size"`     //bytes
	Speed      float64 `json:"speed"`    //bytes/sec
	SpeedAvg   float64 `json:"speedAvg"` //bytes/sec
}

type RCloneStats struct {
	Bytes               uint64                `json:"bytes"`
	Checks              uint                  `json:"checks"`
	DeletedDirs         uint                  `json:"deletedDirs"`
	Deletes             uint                  `json:"deletes"`
	ElapsedTime         float64               `json:"elapsedTime"` // seconds
	Errors              uint                  `json:"errors"`
	Eta                 uint                  `json:"eta"` // seconds
	FatalError          bool                  `json:"fatalError"`
	Renames             uint                  `json:"renames"`
	RetryError          bool                  `json:"retryError"`
	ServerSideCopies    uint                  `json:"serverSideCopies"`
	ServerSideCopyBytes uint                  `json:"serverSideCopyBytes"`
	ServerSideMoveBytes uint                  `json:"serverSideMoveBytes"`
	ServerSideMoves     uint                  `json:"serverSideMoves"`
	Speed               float64               `json:"speed"` // bytes/sec
	TotalBytes          uint64                `json:"totalBytes"`
	TotalChecks         uint                  `json:"totalChecks"`
	TotalTransfers      uint                  `json:"totalTransfers"`
	TransferTime        float64               `json:"transferTime"` // seconds
	Transferring        []RCloneTransferStats `json:"transferring"`
	Transfers           uint                  `json:"transfers"`
}

func (c *RCloneClient) Stats(ctx context.Context) (*RCloneStats, error) {
	result, err := c.cmd(ctx, "core/stats", nil)

	if err != nil {
		return nil, err
	}

	var stats RCloneStats

	err = json.Unmarshal(result, &stats)

	if err != nil {
		return nil, err
	}

	return &stats, nil
}

func (c *RCloneClient) GetBwLimit() rate.Limit {
	if c.bwLimit != nil {
		return *c.bwLimit
	}

	return 0
}

func (c *RCloneClient) SetBwLimit(ctx context.Context, limit rate.Limit) {
	if c.bwLimit == nil || limit != *c.bwLimit {
		c.bwLimit = &limit
		bwLimit := datasize.ByteSize(limit).KBytes()
		c.logger.Trace("Setting rclone bw limit", "kbytes", int64(bwLimit))
		c.optionsQueue <- RCloneOptions{
			BwLimit: fmt.Sprintf("%dK", int64(bwLimit)),
		}
	}
}

func (c *RCloneClient) setOptions(ctx context.Context, options RCloneOptions) error {
	_, err := c.cmd(ctx, "options/set", struct {
		Main RCloneOptions `json:"main"`
	}{
		Main: options,
	})

	return err
}

func (u *RCloneClient) sync(ctx context.Context, request *rcloneRequest) error {
	_, err := u.cmd(ctx, "sync/sync", request)
	return err
}

func (u *RCloneClient) copyFile(ctx context.Context, request *rcloneRequest) error {
	_, err := u.cmd(ctx, "operations/copyfile", request)
	return err
}

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
	var requestBodyReader io.Reader

	if args != nil {
		requestBody, err := json.Marshal(args)

		if err != nil {
			return nil, err
		}

		requestBodyReader = bytes.NewBuffer(requestBody)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.rcloneUrl+"/"+path, requestBodyReader)

	if err != nil {
		return nil, err
	}

	if requestBodyReader != nil {
		request.Header.Set("Content-Type", "application/json")
	}

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
			var argsJson string

			if bytes, err := json.Marshal(args); err == nil {
				argsJson = string(bytes)
			}

			u.logger.Warn("[downloader] rclone cmd failed", "path", path, "args", argsJson, "status", response.Status, "err", responseBody.Error)
			return nil, fmt.Errorf("cmd: %s failed: %s: %s", path, response.Status, responseBody.Error)
		}

		var argsJson string

		if bytes, err := json.Marshal(args); err == nil {
			argsJson = string(bytes)
		}

		u.logger.Warn("[downloader] rclone cmd failed", "path", path, "args", argsJson, "status", response.Status)
		return nil, fmt.Errorf("cmd: %s failed: %s", path, response.Status)
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
	headers         http.Header
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

func (c *RCloneClient) NewSession(ctx context.Context, localFs string, remoteFs string, headers http.Header) (*RCloneSession, error) {
	ctx, cancel := context.WithCancel(ctx)

	session := &RCloneSession{
		RCloneClient: c,
		files:        map[string]*rcloneInfo{},
		remoteFs:     remoteFs,
		localFs:      localFs,
		cancel:       cancel,
		syncQueue:    make(chan syncRequest, 100),
		headers:      headers,
	}

	go func() {
		if !strings.HasPrefix(remoteFs, "http") {
			if _, err := session.ReadRemoteDir(ctx, true); err != nil {
				return
			}
		}

		session.syncFiles(ctx)
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
	requests  []*rcloneRequest
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

				if snapInfo, isStateFile, ok := snaptype.ParseFileName(c.localFs, file); ok {
					if isStateFile {
						//TODO
					} else {
						info.snapInfo = &snapInfo
					}
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
		[]*rcloneRequest{{
			Group: c.Label(),
			SrcFs: c.localFs,
			DstFs: c.remoteFs,
			Filter: rcloneFilter{
				IncludeRule: files,
			}}}, 0}

	return <-cerr
}

func (c *RCloneSession) Download(ctx context.Context, files ...string) error {

	reqInfo := map[string]*rcloneInfo{}

	var fileRequests []*rcloneRequest

	if strings.HasPrefix(c.remoteFs, "http") {
		var headers string
		var comma string

		for header, values := range c.headers {
			for _, value := range values {
				headers += fmt.Sprintf("%s%s=%s", comma, header, value)
				comma = ","
			}
		}

		for _, file := range files {
			reqInfo[file] = &rcloneInfo{
				file: file,
			}
			fileRequests = append(fileRequests,
				&rcloneRequest{
					Group: c.remoteFs,
					SrcFs: rcloneFs{
						Type:    "http",
						Url:     c.remoteFs,
						Headers: headers,
					},
					SrcRemote: file,
					DstFs:     c.localFs,
					DstRemote: file,
				})
		}
	} else {
		c.Lock()

		if len(c.files) == 0 {
			c.Unlock()
			_, err := c.ReadRemoteDir(ctx, false)
			if err != nil {
				return fmt.Errorf("can't download: %s: %w", files, err)
			}
			c.Lock()
		}

		for _, file := range files {
			info, ok := c.files[file]

			if !ok || info.remoteInfo.Size == 0 {
				c.Unlock()
				return fmt.Errorf("can't download: %s: %w", file, os.ErrNotExist)
			}

			reqInfo[file] = info
		}

		c.Unlock()

		fileRequests = append(fileRequests, &rcloneRequest{
			Group: c.Label(),
			SrcFs: c.remoteFs,
			DstFs: c.localFs,
			Filter: rcloneFilter{
				IncludeRule: files,
			}})
	}

	cerr := make(chan error, 1)

	c.syncQueue <- syncRequest{ctx, reqInfo, cerr, fileRequests, 0}

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
	return dir.ReadDir(c.localFs)
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
	Version() snaptype.Version
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
		return nil, errors.New("remote fs undefined")
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

				if snapInfo, isStateFile, ok := snaptype.ParseFileName(c.localFs, fi.Name); ok {
					if isStateFile {
						//TODO
					} else {
						rcinfo.snapInfo = &snapInfo
					}
				} else {
					rcinfo.snapInfo = nil
				}

			} else {
				info := &rcloneInfo{
					file:       fi.Name,
					localInfo:  localInfo,
					remoteInfo: fi,
				}

				if snapInfo, isStateFile, ok := snaptype.ParseFileName(c.localFs, fi.Name); ok {
					if isStateFile {
						//TODO
					} else {
						info.snapInfo = &snapInfo
					}
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

type rcloneFs struct {
	Type    string `json:"type"`
	Url     string `json:"url,omitempty"`
	Headers string `json:"headers,omitempty"` //comma separated list of key,value pairs, standard CSV encoding may be used.
}

type rcloneRequest struct {
	Async     bool           `json:"_async,omitempty"`
	Config    *RCloneOptions `json:"_config,omitempty"`
	Group     string         `json:"_group"`
	SrcFs     interface{}    `json:"srcFs"`
	SrcRemote string         `json:"srcRemote,omitempty"`
	DstFs     string         `json:"dstFs"`
	DstRemote string         `json:"dstRemote,omitempty"`

	Filter rcloneFilter `json:"_filter"`
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
			request.cerr <- errors.New("no sync queue available")
		}
	}

	go func() {
		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()

		select {
		case <-gctx.Done():
			if syncCount := int(c.activeSyncCount.Load()) + len(c.syncQueue); syncCount > 0 {
				log.Debug("[rclone] Synced files", "processed", fmt.Sprintf("%d/%d", c.activeSyncCount.Load(), syncCount))
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
				log.Debug("[rclone] Syncing files", "progress", fmt.Sprintf("%d/%d", c.activeSyncCount.Load(), syncCount))
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

					for _, fileReq := range req.requests {
						if _, ok := fileReq.SrcFs.(rcloneFs); ok {
							if err := c.copyFile(gctx, fileReq); err != nil {

								if gctx.Err() != nil {
									req.cerr <- gctx.Err()
								} else {
									go retry(req)
								}

								return nil //nolint:nilerr
							}
						} else {
							if err := c.sync(gctx, fileReq); err != nil {

								if gctx.Err() != nil {
									req.cerr <- gctx.Err()
								} else {
									go retry(req)
								}

								return nil //nolint:nilerr
							}
						}
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
			c.logger.Debug("[rclone] sync failed", "err", err)
		}
	}()
}
