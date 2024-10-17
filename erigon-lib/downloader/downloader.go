// Copyright 2021 The Erigon Authors
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
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/types/infohash"
	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/downloader/downloaderrawdb"
	"github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	prototypes "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
)

// Downloader - component which downloading historical files. Can use BitTorrent, or other protocols
type Downloader struct {
	db                  kv.RwDB
	pieceCompletionDB   storage.PieceCompletion
	torrentClient       *torrent.Client
	webDownloadClient   *RCloneClient
	webDownloadSessions map[string]*RCloneSession

	cfg *downloadercfg.Cfg

	lock  *sync.RWMutex
	stats AggStats

	folder storage.ClientImplCloser

	ctx          context.Context
	stopMainLoop context.CancelFunc
	wg           sync.WaitGroup

	webseeds         *WebSeeds
	webseedsDiscover bool

	logger    log.Logger
	verbosity log.Lvl

	torrentFS       *AtomicTorrentFS
	snapshotLock    *snapshotLock
	webDownloadInfo map[string]webDownloadInfo
	downloading     map[string]*downloadInfo
	downloadLimit   *rate.Limit

	stuckFileDetailedLogs bool

	logPrefix         string
	startTime         time.Time
	onTorrentComplete func(name string, hash *prototypes.H160)
	completedTorrents map[string]completedTorrentInfo
}

type completedTorrentInfo struct {
	path string
	hash *prototypes.H160
}

type downloadInfo struct {
	torrent  *torrent.Torrent
	time     time.Time
	progress float32
}

type webDownloadInfo struct {
	url     *url.URL
	length  int64
	md5     string
	torrent *torrent.Torrent
}

type AggStats struct {
	MetadataReady, FilesTotal int32
	LastMetadataUpdate        *time.Time
	PeersUnique               int32
	ConnectionsTotal          uint64
	Downloading               int32

	Completed bool
	Progress  float32

	BytesCompleted, BytesTotal uint64
	CompletionRate             uint64

	BytesDownload, BytesUpload uint64
	UploadRate, DownloadRate   uint64
	LocalFileHashes            int
	LocalFileHashTime          time.Duration

	BytesHashed, BytesFlushed uint64
	HashRate, FlushRate       uint64

	WebseedActiveTrips    *atomic.Int64
	WebseedMaxActiveTrips *atomic.Int64
	WebseedTripCount      *atomic.Int64
	WebseedDiscardCount   *atomic.Int64
	WebseedServerFails    *atomic.Int64
	WebseedBytesDownload  *atomic.Int64

	lastTorrentStatus time.Time
}

type requestHandler struct {
	http.Transport
	downloader *Downloader
}

var cloudflareHeaders = http.Header{
	"lsjdjwcush6jbnjj3jnjscoscisoc5s": []string{"I%OSJDNFKE783DDHHJD873EFSIVNI7384R78SSJBJBCCJBC32JABBJCBJK45"},
}

func insertCloudflareHeaders(req *http.Request) {
	for key, value := range cloudflareHeaders {
		req.Header[key] = value
	}
}

// retryBackoff performs exponential backoff based on the attempt number and limited
// by the provided minimum and maximum durations.
//
// It also tries to parse Retry-After response header when a http.StatusTooManyRequests
// (HTTP Code 429) is found in the resp parameter. Hence it will return the number of
// seconds the server states it may be ready to process more requests from this client.
func calcBackoff(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	if resp != nil {
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
			if s, ok := resp.Header["Retry-After"]; ok {
				if sleep, err := strconv.ParseInt(s[0], 10, 64); err == nil {
					return time.Second * time.Duration(sleep)
				}
			}
		}
	}

	mult := math.Pow(2, float64(attemptNum)) * float64(min)
	sleep := time.Duration(mult)
	if float64(sleep) != mult || sleep > max {
		sleep = max
	}

	return sleep
}

func (r *requestHandler) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	r.downloader.lock.RLock()
	webseedMaxActiveTrips := r.downloader.stats.WebseedMaxActiveTrips
	webseedActiveTrips := r.downloader.stats.WebseedActiveTrips
	webseedTripCount := r.downloader.stats.WebseedTripCount
	webseedBytesDownload := r.downloader.stats.WebseedBytesDownload
	webseedDiscardCount := r.downloader.stats.WebseedDiscardCount
	WebseedServerFails := r.downloader.stats.WebseedServerFails
	r.downloader.lock.RUnlock()

	activeTrips := webseedActiveTrips.Add(1)
	if activeTrips > webseedMaxActiveTrips.Load() {
		webseedMaxActiveTrips.Store(activeTrips)
	}

	defer func() {
		if r := recover(); r != nil {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
				resp.Body = nil
			}

			err = fmt.Errorf("http client panic: %s", r)
		}

		webseedActiveTrips.Add(-1)
	}()

	insertCloudflareHeaders(req)

	resp, err = r.Transport.RoundTrip(req)

	attempts := 1
	retry := true

	const minDelay = 500 * time.Millisecond
	const maxDelay = 5 * time.Second
	const maxAttempts = 10

	for err == nil && retry {
		webseedTripCount.Add(1)

		switch resp.StatusCode {
		case http.StatusOK:
			if len(req.Header.Get("Range")) > 0 {
				// the torrent lib is expecting http.StatusPartialContent so it will discard this
				// if this count is higher than 0, its likely there is a server side config issue
				// as it implies that the server is not handling range requests correctly and is just
				// returning the whole file - which the torrent lib can't handle
				//
				// TODO: We could count the bytes - probably need to take this from the req though
				// as its not clear the amount of the content which will be read.  This needs
				// further investigation - if required.
				webseedDiscardCount.Add(1)
			}

			webseedBytesDownload.Add(resp.ContentLength)
			retry = false

		// the first two statuses here have been observed from cloudflare
		// during testing.  The remainder are generally understood to be
		// retry-able http responses, calcBackoff will use the Retry-After
		// header if its available
		case http.StatusInternalServerError, http.StatusBadGateway,
			http.StatusRequestTimeout, http.StatusTooEarly,
			http.StatusTooManyRequests, http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:

			WebseedServerFails.Add(1)

			if resp.Body != nil {
				resp.Body.Close()
				resp.Body = nil
			}

			attempts++
			delayTimer := time.NewTimer(calcBackoff(minDelay, maxDelay, attempts, resp))

			select {
			case <-delayTimer.C:
				// Note this assumes the req.Body is nil
				resp, err = r.Transport.RoundTrip(req)
				webseedTripCount.Add(1)

			case <-req.Context().Done():
				err = req.Context().Err()
			}
			retry = attempts < maxAttempts

		default:
			webseedBytesDownload.Add(resp.ContentLength)
			retry = false
		}
	}

	return resp, err
}

func New(ctx context.Context, cfg *downloadercfg.Cfg, logger log.Logger, verbosity log.Lvl, discover bool) (*Downloader, error) {
	requestHandler := &requestHandler{
		Transport: http.Transport{
			Proxy:       cfg.ClientConfig.HTTPProxy,
			DialContext: cfg.ClientConfig.HTTPDialContext,
			// I think this value was observed from some webseeds. It seems reasonable to extend it
			// to other uses of HTTP from the client.
			MaxConnsPerHost: 10,
		}}

	cfg.ClientConfig.WebTransport = requestHandler

	db, c, m, torrentClient, err := openClient(ctx, cfg.Dirs.Downloader, cfg.Dirs.Snap, cfg.ClientConfig, cfg.MdbxWriteMap, logger)
	if err != nil {
		return nil, fmt.Errorf("openClient: %w", err)
	}

	peerID, err := readPeerID(db)
	if err != nil {
		return nil, fmt.Errorf("get peer id: %w", err)
	}
	cfg.ClientConfig.PeerID = string(peerID)
	if len(peerID) == 0 {
		if err = savePeerID(db, torrentClient.PeerID()); err != nil {
			return nil, fmt.Errorf("save peer id: %w", err)
		}
	}

	mutex := &sync.RWMutex{}
	stats := AggStats{
		WebseedActiveTrips:    &atomic.Int64{},
		WebseedMaxActiveTrips: &atomic.Int64{},
		WebseedTripCount:      &atomic.Int64{},
		WebseedBytesDownload:  &atomic.Int64{},
		WebseedDiscardCount:   &atomic.Int64{},
		WebseedServerFails:    &atomic.Int64{},
	}

	snapLock, err := getSnapshotLock(ctx, cfg, db, &stats, mutex, logger)
	if err != nil {
		return nil, fmt.Errorf("can't initialize snapshot snapLock: %w", err)
	}

	d := &Downloader{
		cfg:                 cfg,
		db:                  db,
		pieceCompletionDB:   c,
		folder:              m,
		torrentClient:       torrentClient,
		lock:                mutex,
		stats:               stats,
		webseeds:            NewWebSeeds(cfg.WebSeedUrls, verbosity, logger),
		logger:              logger,
		verbosity:           verbosity,
		torrentFS:           &AtomicTorrentFS{dir: cfg.Dirs.Snap},
		snapshotLock:        snapLock,
		webDownloadInfo:     map[string]webDownloadInfo{},
		webDownloadSessions: map[string]*RCloneSession{},
		downloading:         map[string]*downloadInfo{},
		webseedsDiscover:    discover,
		logPrefix:           "",
		completedTorrents:   make(map[string]completedTorrentInfo),
	}
	d.webseeds.SetTorrent(d.torrentFS, snapLock.Downloads, cfg.DownloadTorrentFilesFromWebseed)

	requestHandler.downloader = d

	if cfg.ClientConfig.DownloadRateLimiter != nil {
		downloadLimit := cfg.ClientConfig.DownloadRateLimiter.Limit()
		d.downloadLimit = &downloadLimit
	}

	d.ctx, d.stopMainLoop = context.WithCancel(ctx)

	if cfg.AddTorrentsFromDisk {
		for _, download := range snapLock.Downloads {
			if info, err := d.torrentInfo(download.Name); err == nil {
				if info.Completed != nil {
					if hash := hex.EncodeToString(info.Hash); download.Hash != hash {
						fileInfo, _, ok := snaptype.ParseFileName(d.SnapDir(), download.Name)

						if !ok {
							d.logger.Debug("[snapshots] Can't parse download filename", "file", download.Name)
							continue
						}

						// this is lazy as it can be expensive for large files
						fileHashBytes, err := fileHashBytes(d.ctx, fileInfo, &d.stats, d.lock)

						if errors.Is(err, os.ErrNotExist) {
							hashBytes, _ := hex.DecodeString(download.Hash)
							if err := d.db.Update(d.ctx, torrentInfoReset(download.Name, hashBytes, 0)); err != nil {
								d.logger.Debug("[snapshots] Can't update torrent info", "file", download.Name, "hash", download.Hash, "err", err)
							}
							continue
						}

						fileHash := hex.EncodeToString(fileHashBytes)

						if fileHash != download.Hash && fileHash != hash {
							d.logger.Debug("[snapshots] download db mismatch", "file", download.Name, "snapshotLock", download.Hash, "db", hash, "disk", fileHash, "downloaded", *info.Completed)
						} else {
							d.logger.Debug("[snapshots] snapshotLock hash does not match completed download", "file", download.Name, "snapshotLock", hash, "download", download.Hash, "downloaded", *info.Completed)
						}
					}
				}
			}
		}

		if err := d.BuildTorrentFilesIfNeed(d.ctx, snapLock.Chain, snapLock.Downloads); err != nil {
			return nil, err
		}

		if err := d.addTorrentFilesFromDisk(false); err != nil {
			return nil, err
		}
	}

	return d, nil
}

const SnapshotsLockFileName = "snapshot-lock.json"

type snapshotLock struct {
	Chain     string              `json:"chain"`
	Downloads snapcfg.Preverified `json:"downloads"`
}

func getSnapshotLock(ctx context.Context, cfg *downloadercfg.Cfg, db kv.RoDB, stats *AggStats, statsLock *sync.RWMutex, logger log.Logger) (*snapshotLock, error) {
	//TODO: snapshots-lock.json must be created after 1-st download done
	//TODO: snapshots-lock.json is not compatible with E3 .kv files - because they are not immutable (merging to infinity)
	return initSnapshotLock(ctx, cfg, db, stats, statsLock, logger)
	/*
		if !cfg.SnapshotLock {
			return initSnapshotLock(ctx, cfg, db, logger)
		}

			snapDir := cfg.Dirs.Snap

			lockPath := filepath.Join(snapDir, SnapshotsLockFileName)

			file, err := os.Open(lockPath)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return nil, err
				}
			}

			var data []byte

			if file != nil {
				defer file.Close()

				data, err = io.ReadAll(file)

				if err != nil {
					return nil, err
				}
			}

			if file == nil || len(data) == 0 {
				f, err := os.Create(lockPath)
				if err != nil {
					return nil, err
				}
				defer f.Close()

				lock, err := initSnapshotLock(ctx, cfg, db, logger)

				if err != nil {
					return nil, err
				}

				data, err := json.Marshal(lock)

				if err != nil {
					return nil, err
				}

				_, err = f.Write(data)

				if err != nil {
					return nil, err
				}

				if err := f.Sync(); err != nil {
					return nil, err
				}

				return lock, nil
			}

			var lock snapshotLock

			if err = json.Unmarshal(data, &lock); err != nil {
				return nil, err
			}

			if lock.Chain != cfg.ChainName {
				return nil, fmt.Errorf("unexpected chain name:%q expecting: %q", lock.Chain, cfg.ChainName)
			}

			prevHashes := map[string]string{}
		prevNames := map[string]string{}

		for _, current := range lock.Downloads {
			if prev, ok := prevHashes[current.Hash]; ok {
				if prev != current.Name {
					return nil, fmt.Errorf("invalid snapshot_lock: %s duplicated at: %s and %s", current.Hash, current.Name, prev)
				}
			}

			if prev, ok := prevNames[current.Name]; ok {
				if prev != current.Hash {
					return nil, fmt.Errorf("invalid snapshot_lock: %s duplicated at: %s and %s", current.Name, current.Hash, prev)
				}
			}

			prevHashes[current.Name] = current.Hash
			prevNames[current.Hash] = current.Name
		}return &lock, nil
	*/
}

func initSnapshotLock(ctx context.Context, cfg *downloadercfg.Cfg, db kv.RoDB, stats *AggStats, statsLock *sync.RWMutex, logger log.Logger) (*snapshotLock, error) {
	lock := &snapshotLock{
		Chain: cfg.ChainName,
	}

	files, err := SeedableFiles(cfg.Dirs, cfg.ChainName, false)
	if err != nil {
		return nil, err
	}

	snapCfg := cfg.SnapshotConfig
	if snapCfg == nil {
		snapCfg = snapcfg.KnownCfg(cfg.ChainName)
	}
	//if len(files) == 0 {
	lock.Downloads = snapCfg.Preverified
	//}

	// if files exist on disk we assume that the lock file has been removed
	// or was never present so compare them against the known config to
	// recreate the lock file
	//
	// if the file is above the ExpectBlocks in the snapCfg we ignore it
	// if the file is the same version of the known file we:
	//   check if its mid upload
	//     - in which case we compare the hash in the db to the known hash
	//       - if they are different we delete the local file and include the
	//         know file in the hash which will force a re-upload
	//   otherwise
	//      - if the file has a different hash to the known file we include
	//        the files hash in the upload to preserve the local copy
	// if the file is a different version - we see if the version for the
	// file is available in know config - and if so we follow the procedure
	// above, but we use the matching version from the known config.  If there
	// is no matching version just use the one discovered for the file

	versionedCfg := map[snaptype.Version]*snapcfg.Cfg{}
	versionedCfgLock := sync.Mutex{}

	snapDir := cfg.Dirs.Snap

	var downloadMap btree.Map[string, snapcfg.PreverifiedItem]
	var downloadsMutex sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(-1) * 4)
	var done atomic.Int32

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	for _, file := range files {
		file := file

		g.Go(func() error {
			defer done.Add(1)
			fileInfo, isStateFile, ok := snaptype.ParseFileName(snapDir, file)

			if !ok {
				return nil
			}

			if isStateFile {
				if preverified, ok := snapCfg.Preverified.Get(file); ok {
					downloadsMutex.Lock()
					defer downloadsMutex.Unlock()
					downloadMap.Set(file, preverified)
				}
				return nil //TODO: we don't create
			}

			if fileInfo.From > snapCfg.ExpectBlocks {
				return nil
			}

			if preverified, ok := snapCfg.Preverified.Get(fileInfo.Name()); ok {
				hashBytes, err := localHashBytes(ctx, fileInfo, db, stats, statsLock)

				if err != nil {
					return fmt.Errorf("localHashBytes: %w", err)
				}

				downloadsMutex.Lock()
				defer downloadsMutex.Unlock()

				if hash := hex.EncodeToString(hashBytes); preverified.Hash == hash {
					downloadMap.Set(fileInfo.Name(), preverified)
				} else {
					logger.Debug("[downloader] local file hash does not match known", "file", fileInfo.Name(), "local", hash, "known", preverified.Hash)
					// TODO: check if it has an index - if not use the known hash and delete the file
					downloadMap.Set(fileInfo.Name(), snapcfg.PreverifiedItem{Name: fileInfo.Name(), Hash: hash})
				}
			} else {
				versioned := func() *snapcfg.Cfg {
					versionedCfgLock.Lock()
					defer versionedCfgLock.Unlock()

					versioned, ok := versionedCfg[fileInfo.Version]

					if !ok {
						versioned = snapcfg.VersionedCfg(cfg.ChainName, fileInfo.Version, fileInfo.Version)
						versionedCfg[fileInfo.Version] = versioned
					}

					return versioned
				}()

				hashBytes, err := localHashBytes(ctx, fileInfo, db, stats, statsLock)

				if err != nil {
					return fmt.Errorf("localHashBytes: %w", err)
				}

				downloadsMutex.Lock()
				defer downloadsMutex.Unlock()

				if preverified, ok := versioned.Preverified.Get(fileInfo.Name()); ok {
					if hash := hex.EncodeToString(hashBytes); preverified.Hash == hash {
						downloadMap.Set(preverified.Name, preverified)
					} else {
						logger.Debug("[downloader] local file hash does not match known", "file", fileInfo.Name(), "local", hash, "known", preverified.Hash)
						// TODO: check if it has an index - if not use the known hash and delete the file
						downloadMap.Set(fileInfo.Name(), snapcfg.PreverifiedItem{Name: fileInfo.Name(), Hash: hash})
					}
				} else {
					versioned := func() *snapcfg.Cfg {
						versionedCfgLock.Lock()
						defer versionedCfgLock.Unlock()

						versioned, ok := versionedCfg[fileInfo.Version]

						if !ok {
							versioned = snapcfg.VersionedCfg(cfg.ChainName, fileInfo.Version, fileInfo.Version)
							versionedCfg[fileInfo.Version] = versioned
						}

						return versioned
					}()

					hashBytes, err := localHashBytes(ctx, fileInfo, db, stats, statsLock)

					if err != nil {
						return err
					}

					if preverified, ok := versioned.Preverified.Get(fileInfo.Name()); ok {
						if hash := hex.EncodeToString(hashBytes); preverified.Hash == hash {
							downloadMap.Set(preverified.Name, preverified)
						} else {
							logger.Debug("[downloader] local file hash does not match known", "file", fileInfo.Name(), "local", hash, "known", preverified.Hash)
							// TODO: check if it has an index - if not use the known hash and delete the file
							downloadMap.Set(fileInfo.Name(), snapcfg.PreverifiedItem{Name: fileInfo.Name(), Hash: hash})
						}
					} else {
						downloadMap.Set(fileInfo.Name(), snapcfg.PreverifiedItem{Name: fileInfo.Name(), Hash: hex.EncodeToString(hashBytes)})
					}
				}
			}

			return nil
		})
	}

	func() {
		for int(done.Load()) < len(files) {
			select {
			case <-ctx.Done():
				return // g.Wait() will return right error
			case <-logEvery.C:
				if int(done.Load()) == len(files) {
					return
				}
				log.Info("[snapshots] Initiating snapshot-lock", "progress", fmt.Sprintf("%d/%d", done.Load(), len(files)))
			}
		}
	}()

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var missingItems []snapcfg.PreverifiedItem
	var downloads snapcfg.Preverified

	downloadMap.Scan(func(key string, value snapcfg.PreverifiedItem) bool {
		downloads = append(downloads, value)
		return true
	})

	for _, item := range snapCfg.Preverified {
		_, _, ok := snaptype.ParseFileName(snapDir, item.Name)
		if !ok {
			continue
		}

		if !downloads.Contains(item.Name, true) {
			missingItems = append(missingItems, item)
		}
	}

	lock.Downloads = snapcfg.Merge(downloads, missingItems)
	return lock, nil
}

func localHashBytes(ctx context.Context, fileInfo snaptype.FileInfo, db kv.RoDB, stats *AggStats, statsLock *sync.RWMutex) ([]byte, error) {
	var hashBytes []byte

	if db != nil {
		err := db.View(ctx, func(tx kv.Tx) (err error) {
			hashBytes, err = downloaderrawdb.ReadTorrentInfoHash(tx, fileInfo.Name())
			if err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	if len(hashBytes) != 0 {
		return hashBytes, nil
	}

	meta, err := metainfo.LoadFromFile(fileInfo.Path + ".torrent")

	if err == nil {
		if spec, err := torrent.TorrentSpecFromMetaInfoErr(meta); err == nil {
			return spec.InfoHash.Bytes(), nil
		}
	}

	return fileHashBytes(ctx, fileInfo, stats, statsLock)
}

func fileHashBytes(ctx context.Context, fileInfo snaptype.FileInfo, stats *AggStats, statsLock *sync.RWMutex) ([]byte, error) {
	exists, err := dir.FileExist(fileInfo.Path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, os.ErrNotExist
	}

	defer func(t time.Time) {
		statsLock.Lock()
		defer statsLock.Unlock()
		stats.LocalFileHashes++
		stats.LocalFileHashTime += time.Since(t)
	}(time.Now())

	info := &metainfo.Info{PieceLength: downloadercfg.DefaultPieceSize, Name: fileInfo.Name()}

	if err := info.BuildFromFilePath(fileInfo.Path); err != nil {
		return nil, fmt.Errorf("can't get local hash for %s: %w", fileInfo.Name(), err)
	}

	meta, err := CreateMetaInfo(info, nil)

	if err != nil {
		return nil, fmt.Errorf("can't get local hash for %s: %w", fileInfo.Name(), err)
	}

	spec, err := torrent.TorrentSpecFromMetaInfoErr(meta)

	if err != nil {
		return nil, fmt.Errorf("can't get local hash for %s: %w", fileInfo.Name(), err)
	}

	return spec.InfoHash.Bytes(), nil
}

func (d *Downloader) MainLoopInBackground(silent bool) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := d.mainLoop(silent); err != nil {
			if !errors.Is(err, context.Canceled) {
				d.logger.Warn("[snapshots]", "err", err)
			}
		}
	}()
}

type downloadStatus struct {
	name     string
	length   int64
	infoHash infohash.T
	spec     *torrent.TorrentSpec
	err      error
}

type seedHash struct {
	url      *url.URL
	hash     *infohash.T
	reported bool
}

func (d *Downloader) mainLoop(silent bool) error {
	if d.webseedsDiscover {

		// CornerCase: no peers -> no anoncments to trackers -> no magnetlink resolution (but magnetlink has filename)
		// means we can start adding weebseeds without waiting for `<-t.GotInfo()`
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			// webseeds.Discover may create new .torrent files on disk
			d.webseeds.Discover(d.ctx, d.cfg.WebSeedFileProviders, d.cfg.Dirs.Snap)
			// apply webseeds to existing torrents
			if err := d.addTorrentFilesFromDisk(true); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Warn("[snapshots] addTorrentFilesFromDisk", "err", err)
			}

			d.lock.Lock()
			defer d.lock.Unlock()

			for _, t := range d.torrentClient.Torrents() {
				if urls, ok := d.webseeds.ByFileName(t.Name()); ok {
					// if we have created a torrent, but it has no info, assume that the
					// webseed download either has not been called yet or has failed and
					// try again here - otherwise the torrent will be left with no info
					if t.Info() == nil {
						ts, ok, err := d.webseeds.DownloadAndSaveTorrentFile(d.ctx, t.Name())
						if ok && err == nil {
							_, _, err = addTorrentFile(d.ctx, ts, d.torrentClient, d.db, d.webseeds)
							if err != nil {
								d.logger.Warn("[snapshots] addTorrentFile from webseed", "err", err)
								continue
							}
						}
					}

					t.AddWebSeeds(urls)
				}
			}
		}()
	}

	fileSlots := d.cfg.DownloadSlots

	var pieceSlots int

	if d.downloadLimit != nil {
		pieceSlots = int(math.Round(float64(*d.downloadLimit / rate.Limit(downloadercfg.DefaultPieceSize))))
	} else {
		pieceSlots = int(512 * datasize.MB / downloadercfg.DefaultPieceSize)
	}

	//TODO: feature is not ready yet
	//d.webDownloadClient, _ = NewRCloneClient(d.logger)
	d.webDownloadClient = nil

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		checking := map[string]struct{}{}
		failed := map[string]struct{}{}
		waiting := map[string]struct{}{}

		downloadComplete := make(chan downloadStatus, 100)
		seedHashMismatches := map[infohash.T][]*seedHash{}

		// set limit here to make load predictable, not to control Disk/CPU consumption
		// will impact start times depending on the amount of non complete files - should
		// be low unless the download db is deleted - in which case all files may be checked
		checkGroup, _ := errgroup.WithContext(d.ctx)
		checkGroup.SetLimit(runtime.GOMAXPROCS(-1) * 4)

		lastIntMult := time.Now()

		for {
			torrents := d.torrentClient.Torrents()

			var pending []*torrent.Torrent

			var plist []string
			var clist []string
			var flist []string
			var dlist []string

			for _, t := range torrents {
				if _, ok := d.completedTorrents[t.Name()]; ok {
					clist = append(clist, t.Name())
					continue
				}

				if isComplete, length, completionTime := d.checkComplete(t.Name()); isComplete && completionTime != nil {
					if _, ok := checking[t.Name()]; !ok {
						fileInfo, _, ok := snaptype.ParseFileName(d.SnapDir(), t.Name())

						if !ok {
							downloadComplete <- downloadStatus{
								name: fileInfo.Name(),
								err:  fmt.Errorf("can't parse file name: %s", fileInfo.Name()),
							}
						}

						stat, err := os.Stat(fileInfo.Path)

						if err != nil {
							downloadComplete <- downloadStatus{
								name: fileInfo.Name(),
								err:  err,
							}
						}

						if completionTime != nil {
							if !stat.ModTime().Equal(*completionTime) {
								checking[t.Name()] = struct{}{}

								go func(fileInfo snaptype.FileInfo, infoHash infohash.T, length int64, completionTime time.Time) {
									checkGroup.Go(func() error {
										fileHashBytes, _ := fileHashBytes(d.ctx, fileInfo, &d.stats, d.lock)

										if bytes.Equal(infoHash.Bytes(), fileHashBytes) {
											downloadComplete <- downloadStatus{
												name:     fileInfo.Name(),
												length:   length,
												infoHash: infoHash,
											}
										} else {
											downloadComplete <- downloadStatus{
												name: fileInfo.Name(),
												err:  errors.New("hash check failed"),
											}

											d.logger.Warn("[snapshots] Torrent hash does not match file", "file", fileInfo.Name(), "torrent-hash", infoHash, "file-hash", hex.EncodeToString(fileHashBytes))
										}

										return nil
									})
								}(fileInfo, t.InfoHash(), length, *completionTime)

							} else {
								clist = append(clist, t.Name())
								d.torrentCompleted(t.Name(), t.InfoHash())
								continue
							}
						}
					}
				} else {
					delete(failed, t.Name())
				}

				if _, ok := failed[t.Name()]; ok {
					flist = append(flist, t.Name())
					continue
				}

				d.lock.RLock()
				_, downloading := d.downloading[t.Name()]
				d.lock.RUnlock()

				if downloading && t.Complete.Bool() {
					select {
					case <-d.ctx.Done():
						return
					case <-t.GotInfo():
					}

					var completionTime *time.Time
					fileInfo, _, ok := snaptype.ParseFileName(d.SnapDir(), t.Name())

					if !ok {
						d.logger.Debug("[snapshots] Can't parse downloaded filename", "file", t.Name())
						failed[t.Name()] = struct{}{}
						continue
					}

					info, err := d.torrentInfo(t.Name())

					if err == nil {
						completionTime = info.Completed
					}

					if completionTime == nil {
						now := time.Now()
						completionTime = &now
					}

					if statInfo, _ := os.Stat(fileInfo.Path); statInfo != nil {
						if !statInfo.ModTime().Equal(*completionTime) {
							os.Chtimes(fileInfo.Path, time.Time{}, *completionTime)
						}

						if statInfo, _ := os.Stat(fileInfo.Path); statInfo != nil {
							// round completion time to os granularity
							modTime := statInfo.ModTime()
							completionTime = &modTime
						}
					}

					if err := d.db.Update(d.ctx,
						torrentInfoUpdater(t.Info().Name, nil, t.Info().Length, completionTime)); err != nil {
						d.logger.Warn("[snapshots] Failed to update file info", "file", t.Info().Name, "err", err)
					}

					d.lock.Lock()
					delete(d.downloading, t.Name())
					d.lock.Unlock()
					clist = append(clist, t.Name())
					d.torrentCompleted(t.Name(), t.InfoHash())

					continue
				}

				if downloading {
					dlist = append(dlist, t.Name())
					continue
				}

				pending = append(pending, t)
				plist = append(plist, t.Name())
			}

			select {
			case <-d.ctx.Done():
				return
			case status := <-downloadComplete:
				d.lock.Lock()
				delete(d.downloading, status.name)
				d.lock.Unlock()

				delete(checking, status.name)

				if status.spec != nil {
					_, _, err := d.torrentClient.AddTorrentSpec(status.spec)

					if err != nil {
						d.logger.Warn("Can't re-add spec after download", "file", status.name, "err", err)
					}

				}

				if status.err == nil {
					var completionTime *time.Time
					fileInfo, _, ok := snaptype.ParseFileName(d.SnapDir(), status.name)

					if !ok {
						d.logger.Debug("[snapshots] Can't parse downloaded filename", "file", status.name)
						continue
					}

					if info, err := d.torrentInfo(status.name); err == nil {
						completionTime = info.Completed
					}

					if completionTime == nil {
						now := time.Now()
						completionTime = &now
					}

					if statInfo, _ := os.Stat(fileInfo.Path); statInfo != nil {
						if !statInfo.ModTime().Equal(*completionTime) {
							os.Chtimes(fileInfo.Path, time.Time{}, *completionTime)
						}

						if statInfo, _ := os.Stat(fileInfo.Path); statInfo != nil {
							// round completion time to os granularity
							modTime := statInfo.ModTime()
							completionTime = &modTime
						}
					}

					if err := d.db.Update(context.Background(),
						torrentInfoUpdater(status.name, status.infoHash.Bytes(), status.length, completionTime)); err != nil {
						d.logger.Warn("[snapshots] Failed to update file info", "file", status.name, "err", err)
					}

					d.torrentCompleted(status.name, status.infoHash)
					continue
				} else {
					d.lock.Lock()
					delete(d.completedTorrents, status.name)
					d.lock.Unlock()
				}

			default:
			}

			d.lock.RLock()
			webDownloadInfoLen := len(d.webDownloadInfo)
			d.lock.RUnlock()

			if len(pending)+webDownloadInfoLen == 0 {
				select {
				case <-d.ctx.Done():
					return
				case <-time.After(10 * time.Second):
					continue
				}
			}

			d.lock.Lock()
			downloadingLen := len(d.downloading)
			d.stats.Downloading = int32(downloadingLen)
			d.lock.Unlock()

			// the call interval of the loop (elapsed sec) used to get slots/sec for
			// calculating the number of files to download based on the loop speed
			intervalMultiplier := int(time.Since(lastIntMult).Seconds())

			// min and max here are taken from the torrent peer config
			switch {
			case intervalMultiplier < 16:
				intervalMultiplier = 16
			case intervalMultiplier > 128:
				intervalMultiplier = 128
			}

			available := availableTorrents(d.ctx, pending, d.downloading, fileSlots, pieceSlots*intervalMultiplier)

			d.lock.RLock()
			for _, webDownload := range d.webDownloadInfo {
				_, downloading := d.downloading[webDownload.torrent.Name()]

				if downloading {
					continue
				}

				addDownload := true

				for _, t := range available {
					if t.Name() == webDownload.torrent.Name() {
						addDownload = false
						break
					}
				}

				if addDownload {
					if len(available) < d.cfg.DownloadSlots-downloadingLen {
						available = append(available, webDownload.torrent)
					}
				} else {
					if wi, isStateFile, ok := snaptype.ParseFileName(d.SnapDir(), webDownload.torrent.Name()); ok && !isStateFile {
						for i, t := range available {
							if ai, _, ok := snaptype.ParseFileName(d.SnapDir(), t.Name()); ok {
								if ai.CompareTo(wi) > 0 {
									available[i] = webDownload.torrent
									break
								}
							}
						}
					}
				}
			}
			d.lock.RUnlock()

			d.lock.RLock()
			completed := d.stats.Completed
			d.lock.RUnlock()

			if !completed {
				var alist []string

				for _, t := range available {
					alist = append(alist, t.Name())
				}

				d.logger.Trace("[snapshot] download status", "pending", plist, "availible", alist, "downloading", dlist, "complete", clist, "failed", flist)
			}

			for _, t := range available {

				torrentInfo, err := d.torrentInfo(t.Name())

				if err != nil {
					if err := d.db.Update(d.ctx, torrentInfoReset(t.Name(), t.InfoHash().Bytes(), 0)); err != nil {
						d.logger.Debug("[snapshots] Can't update torrent info", "file", t.Name(), "hash", t.InfoHash(), "err", err)
					}
				}

				fileInfo, _, ok := snaptype.ParseFileName(d.SnapDir(), t.Name())

				if !ok {
					d.logger.Debug("[snapshots] Can't parse download filename", "file", t.Name())
					failed[t.Name()] = struct{}{}
					continue
				}

				if torrentInfo != nil {
					if torrentInfo.Completed != nil {
						// is the last completed download for this file is the same as the current torrent
						// check if we can re-use the existing file rather than re-downloading it
						if bytes.Equal(t.InfoHash().Bytes(), torrentInfo.Hash) {
							// has the local file changed since we downloaded it - if it has just download it otherwise
							// do a hash check as if we already have the file - we don't need to download it again
							if fi, err := os.Stat(filepath.Join(d.SnapDir(), t.Name())); err == nil && fi.ModTime().Equal(*torrentInfo.Completed) {
								localHash, complete := localHashCompletionCheck(d.ctx, t, fileInfo, downloadComplete, &d.stats, d.lock)

								if complete {
									d.logger.Trace("[snapshots] Ignoring download request - already complete", "file", t.Name(), "hash", t.InfoHash())
									continue
								}

								failed[t.Name()] = struct{}{}
								d.logger.Debug("[snapshots] NonCanonical hash", "file", t.Name(), "got", hex.EncodeToString(localHash), "expected", t.InfoHash(), "downloaded", *torrentInfo.Completed)

								continue

							} else {
								if err := d.db.Update(d.ctx, torrentInfoReset(t.Name(), t.InfoHash().Bytes(), 0)); err != nil {
									d.logger.Debug("[snapshots] Can't reset torrent info", "file", t.Name(), "hash", t.InfoHash(), "err", err)
								}
							}
						} else {
							if err := d.db.Update(d.ctx, torrentInfoReset(t.Name(), t.InfoHash().Bytes(), 0)); err != nil {
								d.logger.Debug("[snapshots] Can't update torrent info", "file", t.Name(), "hash", t.InfoHash(), "err", err)
							}

							if _, complete := localHashCompletionCheck(d.ctx, t, fileInfo, downloadComplete, &d.stats, d.lock); complete {
								d.logger.Trace("[snapshots] Ignoring download request - already complete", "file", t.Name(), "hash", t.InfoHash())
								continue
							}
						}
					}
				} else {
					if _, ok := waiting[t.Name()]; !ok {
						if _, complete := localHashCompletionCheck(d.ctx, t, fileInfo, downloadComplete, &d.stats, d.lock); complete {
							d.logger.Trace("[snapshots] Ignoring download request - already complete", "file", t.Name(), "hash", t.InfoHash())
							continue
						}

						waiting[t.Name()] = struct{}{}
					}
				}

				switch {
				case len(t.PeerConns()) > 0:
					d.logger.Debug("[snapshots] Downloading from torrent", "file", t.Name(), "peers", len(t.PeerConns()), "webpeers", len(t.WebseedPeerConns()))
					delete(waiting, t.Name())
					d.torrentDownload(t, downloadComplete)
				case len(t.WebseedPeerConns()) > 0:
					if d.webDownloadClient != nil {
						var peerUrls []*url.URL

						for _, peer := range t.WebseedPeerConns() {
							if peerUrl, err := webPeerUrl(peer); err == nil {
								peerUrls = append(peerUrls, peerUrl)
							}
						}

						d.logger.Debug("[snapshots] Downloading from webseed", "file", t.Name(), "webpeers", len(t.WebseedPeerConns()))
						delete(waiting, t.Name())
						session, err := d.webDownload(peerUrls, t, nil, downloadComplete)

						if err != nil {
							d.logger.Warn("Can't complete web download", "file", t.Info().Name, "err", err)

							if session == nil {
								delete(waiting, t.Name())
								d.torrentDownload(t, downloadComplete)
							}
							continue
						}
					} else {
						d.logger.Debug("[snapshots] Downloading from torrent", "file", t.Name(), "peers", len(t.PeerConns()), "webpeers", len(t.WebseedPeerConns()))
						delete(waiting, t.Name())
						d.torrentDownload(t, downloadComplete)
					}
				default:
					if d.webDownloadClient != nil {
						d.lock.RLock()
						webDownload, ok := d.webDownloadInfo[t.Name()]
						d.lock.RUnlock()

						if !ok {
							var mismatches []*seedHash
							var err error

							webDownload, mismatches, err = d.getWebDownloadInfo(t)

							if err != nil {
								if len(mismatches) > 0 {
									seedHashMismatches[t.InfoHash()] = append(seedHashMismatches[t.InfoHash()], mismatches...)
									logSeedHashMismatches(t.InfoHash(), t.Name(), seedHashMismatches, d.logger)
								}

								d.logger.Warn("Can't complete web download", "file", t.Info().Name, "err", err)
								continue
							}
						}

						root, _ := path.Split(webDownload.url.String())
						peerUrl, err := url.Parse(root)

						if err != nil {
							d.logger.Warn("Can't complete web download", "file", t.Info().Name, "err", err)
							continue
						}

						d.lock.Lock()
						delete(d.webDownloadInfo, t.Name())
						d.lock.Unlock()

						d.logger.Debug("[snapshots] Downloading from web", "file", t.Name(), "webpeers", len(t.WebseedPeerConns()))
						delete(waiting, t.Name())
						d.webDownload([]*url.URL{peerUrl}, t, &webDownload, downloadComplete)
						continue
					}

					d.logger.Debug("[snapshots] Downloading from torrent", "file", t.Name(), "peers", len(t.PeerConns()))
					delete(waiting, t.Name())
					d.torrentDownload(t, downloadComplete)
				}
			}

			d.lock.Lock()
			lastMetadatUpdate := d.stats.LastMetadataUpdate
			d.lock.Unlock()

			if lastMetadatUpdate != nil &&
				((len(available) == 0 && time.Since(*lastMetadatUpdate) > 30*time.Second) ||
					time.Since(*lastMetadatUpdate) > 5*time.Minute) {

				for _, t := range d.torrentClient.Torrents() {
					if t.Info() == nil {
						if isComplete, _, _ := d.checkComplete(t.Name()); isComplete {
							continue
						}

						d.lock.RLock()
						_, ok := d.webDownloadInfo[t.Name()]
						d.lock.RUnlock()

						if !ok {
							if _, ok := seedHashMismatches[t.InfoHash()]; ok {
								continue
							}

							info, mismatches, err := d.getWebDownloadInfo(t)

							seedHashMismatches[t.InfoHash()] = append(seedHashMismatches[t.InfoHash()], mismatches...)

							if err != nil {
								if len(mismatches) > 0 {
									logSeedHashMismatches(t.InfoHash(), t.Name(), seedHashMismatches, d.logger)
								}
								continue
							}

							d.lock.Lock()
							d.webDownloadInfo[t.Name()] = info
							d.lock.Unlock()
						}
					} else {
						d.lock.Lock()
						delete(d.webDownloadInfo, t.Name())
						d.lock.Unlock()
					}
				}
			}

		}
	}()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	statInterval := 20 * time.Second
	statEvery := time.NewTicker(statInterval)
	defer statEvery.Stop()

	var m runtime.MemStats

	//Need to know is completed from previous stats in order to print "Download completed" message
	prevStatsCompleted := d.stats.Completed
	for {
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-statEvery.C:
			prevStatsCompleted = d.stats.Completed
			d.ReCalcStats(statInterval)

		case <-logEvery.C:
			if !prevStatsCompleted {
				d.logProgress()
			}

			if silent {
				continue
			}

			stats := d.Stats()

			dbg.ReadMemStats(&m)

			if stats.Completed && stats.FilesTotal > 0 {
				d.logger.Info("[snapshots] Seeding",
					"up", common.ByteCount(stats.UploadRate)+"/s",
					"peers", stats.PeersUnique,
					"conns", stats.ConnectionsTotal,
					"files", stats.FilesTotal,
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
				)
				continue
			}

			if stats.PeersUnique == 0 {
				ips := d.TorrentClient().BadPeerIPs()
				if len(ips) > 0 {
					d.logger.Info("[snapshots] Stats", "banned", ips)
				}
			}
		}
	}
}

func localHashCompletionCheck(ctx context.Context, t *torrent.Torrent, fileInfo snaptype.FileInfo, statusChan chan downloadStatus, stats *AggStats, statsLock *sync.RWMutex) ([]byte, bool) {
	localHash, err := fileHashBytes(ctx, fileInfo, stats, statsLock)

	if err == nil {
		if bytes.Equal(t.InfoHash().Bytes(), localHash) {
			statusChan <- downloadStatus{
				name:     t.Name(),
				length:   t.Length(),
				infoHash: t.InfoHash(),
				spec:     nil,
				err:      nil,
			}

			return localHash, true
		}
	}

	return localHash, false
}

func logSeedHashMismatches(torrentHash infohash.T, name string, seedHashMismatches map[infohash.T][]*seedHash, logger log.Logger) {
	var nohash []*seedHash
	var mismatch []*seedHash

	for _, entry := range seedHashMismatches[torrentHash] {
		if !entry.reported {
			if entry.hash == nil {
				nohash = append(nohash, entry)
			} else {
				mismatch = append(mismatch, entry)
			}

			entry.reported = true
		}
	}

	if len(nohash) > 0 {
		var webseeds string
		for _, entry := range nohash {
			if len(webseeds) > 0 {
				webseeds += ", "
			}

			webseeds += strings.TrimSuffix(entry.url.String(), "/")
		}

		logger.Warn("No webseed entry for torrent", "name", name, "hash", torrentHash.HexString(), "webseeds", webseeds)
	}

	if len(mismatch) > 0 {
		var webseeds string
		for _, entry := range mismatch {
			if len(webseeds) > 0 {
				webseeds += ", "
			}

			webseeds += strings.TrimSuffix(entry.url.String(), "/") + "#" + entry.hash.HexString()
		}

		logger.Debug("Webseed hash mismatch for torrent", "name", name, "hash", torrentHash.HexString(), "webseeds", webseeds)
	}
}

func (d *Downloader) checkComplete(name string) (complete bool, fileLen int64, completedAt *time.Time) {
	if err := d.db.View(d.ctx, func(tx kv.Tx) error {
		complete, fileLen, completedAt = downloaderrawdb.CheckFileComplete(tx, name, d.SnapDir())
		return nil
	}); err != nil {
		return false, 0, nil
	}
	return
}

func (d *Downloader) getWebDownloadInfo(t *torrent.Torrent) (webDownloadInfo, []*seedHash, error) {
	d.lock.RLock()
	info, ok := d.webDownloadInfo[t.Name()]
	d.lock.RUnlock()

	if ok {
		return info, nil, nil
	}

	// todo this function does not exit on first matched webseed hash, could make unexpected results
	infos, seedHashMismatches, err := d.webseeds.getWebDownloadInfo(d.ctx, t)
	if err != nil || len(infos) == 0 {
		return webDownloadInfo{}, seedHashMismatches, fmt.Errorf("can't find download info: %w", err)
	}
	return infos[0], seedHashMismatches, nil
}

func getWebpeerTorrentInfo(ctx context.Context, downloadUrl *url.URL) (*metainfo.MetaInfo, error) {
	torrentRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadUrl.String()+".torrent", nil)

	if err != nil {
		return nil, err
	}

	torrentResponse, err := http.DefaultClient.Do(torrentRequest)

	if err != nil {
		return nil, err
	}

	defer torrentResponse.Body.Close()

	if torrentResponse.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("can't get webpeer torrent unexpected http response: %s", torrentResponse.Status)
	}

	return metainfo.Load(torrentResponse.Body)
}

func (d *Downloader) torrentDownload(t *torrent.Torrent, statusChan chan downloadStatus) {
	d.lock.Lock()
	d.downloading[t.Name()] = &downloadInfo{torrent: t}
	d.lock.Unlock()

	d.wg.Add(1)

	go func(t *torrent.Torrent) {
		defer d.wg.Done()

		downloadStarted := time.Now()

		t.AllowDataDownload()

		select {
		case <-d.ctx.Done():
			return
		case <-t.GotInfo():
		}

		t.DownloadAll()

		idleCount := 0
		var lastRead int64

		for {
			select {
			case <-d.ctx.Done():
				return
			case <-t.Complete.On():
				downloadTime := time.Since(downloadStarted)
				downloaded := t.Stats().BytesReadUsefulData

				diagnostics.Send(diagnostics.FileDownloadedStatisticsUpdate{
					FileName:    t.Name(),
					TimeTook:    downloadTime.Seconds(),
					AverageRate: uint64(float64(downloaded.Int64()) / downloadTime.Seconds()),
				})

				d.logger.Debug("[snapshots] Downloaded from BitTorrent", "file", t.Name(),
					"download-time", downloadTime.Round(time.Second).String(), "downloaded", common.ByteCount(uint64(downloaded.Int64())),
					"rate", fmt.Sprintf("%s/s", common.ByteCount(uint64(float64(downloaded.Int64())/downloadTime.Seconds()))))
				return
			case <-time.After(10 * time.Second):
				bytesRead := t.Stats().BytesReadData

				if lastRead-bytesRead.Int64() == 0 {
					idleCount++
				} else {
					lastRead = bytesRead.Int64()
					idleCount = 0
				}

				//fallback to webDownloadClient, but only if it's enabled
				if d.webDownloadClient != nil && idleCount > 6 {
					t.DisallowDataDownload()
					return
				}
			}
		}
	}(t)
}

func (d *Downloader) webDownload(peerUrls []*url.URL, t *torrent.Torrent, i *webDownloadInfo, statusChan chan downloadStatus) (*RCloneSession, error) {
	if d.webDownloadClient == nil {
		return nil, errors.New("webdownload client not enabled")
	}

	peerUrl, err := selectDownloadPeer(d.ctx, peerUrls, t)

	if err != nil {
		return nil, err
	}

	peerUrl = strings.TrimSuffix(peerUrl, "/")

	session, ok := d.webDownloadSessions[peerUrl]

	if !ok {
		var err error
		session, err = d.webDownloadClient.NewSession(d.ctx, d.SnapDir(), peerUrl, cloudflareHeaders)

		if err != nil {
			return nil, err
		}

		d.webDownloadSessions[peerUrl] = session
	}

	name := t.Name()
	mi := t.Metainfo()
	infoHash := t.InfoHash()

	var length int64

	if i != nil {
		length = i.length
	} else {
		length = t.Length()
	}

	magnet := mi.Magnet(&infoHash, &metainfo.Info{Name: name})
	spec, err := torrent.TorrentSpecFromMagnetUri(magnet.String())

	if err != nil {
		return session, fmt.Errorf("can't get torrent spec for %s from info: %w", t.Info().Name, err)
	}

	spec.ChunkSize = downloadercfg.DefaultNetworkChunkSize
	spec.DisallowDataDownload = true

	info, _, ok := snaptype.ParseFileName(d.SnapDir(), name)

	if !ok {
		return nil, fmt.Errorf("can't parse filename: %s", name)
	}

	d.lock.Lock()
	t.Drop()
	d.downloading[name] = &downloadInfo{torrent: t}
	d.lock.Unlock()

	d.wg.Add(1)

	go func() {
		defer d.wg.Done()
		exists, err := dir.FileExist(info.Path)
		if err != nil {
			d.logger.Warn("FileExist error", "file", name, "path", info.Path, "err", err)
		}
		if exists {
			if err := os.Remove(info.Path); err != nil {
				d.logger.Warn("Couldn't remove previous file before download", "file", name, "path", info.Path, "err", err)
			}
		}

		if d.downloadLimit != nil {
			limit := float64(*d.downloadLimit) / float64(d.cfg.DownloadSlots)

			func() {
				d.lock.Lock()
				defer d.lock.Unlock()

				torrentLimit := d.cfg.ClientConfig.DownloadRateLimiter.Limit()
				rcloneLimit := d.webDownloadClient.GetBwLimit()

				d.cfg.ClientConfig.DownloadRateLimiter.SetLimit(torrentLimit - rate.Limit(limit))
				d.webDownloadClient.SetBwLimit(d.ctx, rcloneLimit+rate.Limit(limit))
			}()

			defer func() {
				d.lock.Lock()
				defer d.lock.Unlock()

				torrentLimit := d.cfg.ClientConfig.DownloadRateLimiter.Limit()
				rcloneLimit := d.webDownloadClient.GetBwLimit()

				d.cfg.ClientConfig.DownloadRateLimiter.SetLimit(torrentLimit + rate.Limit(limit))
				d.webDownloadClient.SetBwLimit(d.ctx, rcloneLimit-rate.Limit(limit))
			}()
		}

		err = session.Download(d.ctx, name)

		if err != nil {
			d.logger.Error("Web download failed", "file", name, "err", err)
		}

		localHash, err := fileHashBytes(d.ctx, info, &d.stats, d.lock)

		if err == nil {
			if !bytes.Equal(infoHash.Bytes(), localHash) {
				err = fmt.Errorf("hash mismatch: expected: 0x%x, got: 0x%x", infoHash.Bytes(), localHash)

				d.logger.Error("Web download failed", "file", name, "url", peerUrl, "err", err)

				if ferr := os.Remove(info.Path); ferr != nil {
					d.logger.Warn("Couldn't remove invalid file", "file", name, "path", info.Path, "err", ferr)
				}
			}
		} else {
			d.logger.Error("Web download failed", "file", name, "url", peerUrl, "err", err)
		}

		statusChan <- downloadStatus{
			name:     name,
			length:   length,
			infoHash: infoHash,
			spec:     spec,
			err:      err,
		}
	}()

	return session, nil
}

func selectDownloadPeer(ctx context.Context, peerUrls []*url.URL, t *torrent.Torrent) (string, error) {
	switch len(peerUrls) {
	case 0:
		return "", errors.New("no download peers")

	case 1:
		downloadUrl := peerUrls[0].JoinPath(t.Name())
		peerInfo, err := getWebpeerTorrentInfo(ctx, downloadUrl)

		if err == nil && bytes.Equal(peerInfo.HashInfoBytes().Bytes(), t.InfoHash().Bytes()) {
			return peerUrls[0].String(), nil
		}

	default:
		peerIndex := rand.Intn(len(peerUrls))
		peerUrl := peerUrls[peerIndex]
		downloadUrl := peerUrl.JoinPath(t.Name())
		peerInfo, err := getWebpeerTorrentInfo(ctx, downloadUrl)

		if err == nil && bytes.Equal(peerInfo.HashInfoBytes().Bytes(), t.InfoHash().Bytes()) {
			return peerUrl.String(), nil
		}

		for i := range peerUrls {
			if i == peerIndex {
				continue
			}
			peerInfo, err := getWebpeerTorrentInfo(ctx, downloadUrl)

			if err == nil && bytes.Equal(peerInfo.HashInfoBytes().Bytes(), t.InfoHash().Bytes()) {
				return peerUrl.String(), nil
			}
		}
	}

	return "", errors.New("can't find download peer")
}

func availableTorrents(ctx context.Context, pending []*torrent.Torrent, downloading map[string]*downloadInfo, fileSlots int, pieceSlots int) []*torrent.Torrent {

	piecesDownloading := 0
	pieceRemainder := int64(0)

	for _, info := range downloading {
		if info.torrent.NumPieces() == 1 {
			pieceRemainder += info.torrent.Info().Length

			if pieceRemainder >= downloadercfg.DefaultPieceSize {
				pieceRemainder = 0
				piecesDownloading++
			}
		} else {
			piecesDownloading += info.torrent.NumPieces() - info.torrent.Stats().PiecesComplete
		}
	}

	if len(downloading) >= fileSlots && piecesDownloading > pieceSlots {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(10 * time.Second):
			return nil
		}
	}

	var pendingStateFiles []*torrent.Torrent
	var pendingBlocksFiles []*torrent.Torrent

	for _, t := range pending {
		_, isStateFile, ok := snaptype.ParseFileName("", t.Name())
		if !ok {
			continue
		}
		if isStateFile {
			pendingStateFiles = append(pendingStateFiles, t)
		} else {
			pendingBlocksFiles = append(pendingBlocksFiles, t)
		}
	}
	pending = pendingBlocksFiles

	slices.SortFunc(pending, func(i, j *torrent.Torrent) int {
		in, _, ok1 := snaptype.ParseFileName("", i.Name())
		jn, _, ok2 := snaptype.ParseFileName("", j.Name())
		if ok1 && ok2 {
			return in.CompareTo(jn)
		}
		return strings.Compare(i.Name(), j.Name())
	})

	var available []*torrent.Torrent

	for len(pending) > 0 && pending[0].Info() != nil {
		available = append(available, pending[0])

		if pending[0].NumPieces() == 1 {
			pieceRemainder += pending[0].Info().Length

			if pieceRemainder >= downloadercfg.DefaultPieceSize {
				pieceRemainder = 0
				piecesDownloading++
			}
		} else {
			piecesDownloading += pending[0].NumPieces()
		}

		if len(available) >= fileSlots && piecesDownloading > pieceSlots {
			return available
		}

		pending = pending[1:]
	}
	for len(pendingStateFiles) > 0 && pendingStateFiles[0].Info() != nil {
		available = append(available, pendingStateFiles[0])

		if len(available) >= fileSlots && piecesDownloading > pieceSlots {
			return available
		}

		pendingStateFiles = pendingStateFiles[1:]
	}

	if len(pending) == 0 && len(pendingStateFiles) == 0 {
		return available
	}

	cases := make([]reflect.SelectCase, 0, len(pending)+2)

	for _, t := range pending {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(t.GotInfo()),
		})
	}

	if len(cases) == 0 {
		return nil
	}

	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	},
		reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.After(10 * time.Second)),
		})

	for {
		selected, _, _ := reflect.Select(cases)

		switch selected {
		case len(cases) - 2:
			return nil
		case len(cases) - 1:
			return available
		default:
			available = append(available, pending[selected])

			if pending[selected].NumPieces() == 1 {
				pieceRemainder += pending[selected].Info().Length

				if pieceRemainder >= downloadercfg.DefaultPieceSize {
					pieceRemainder = 0
					piecesDownloading++
				}
			} else {
				piecesDownloading += pending[selected].NumPieces()
			}

			if len(available) >= fileSlots && piecesDownloading > pieceSlots {
				return available
			}

			pending = append(pending[:selected], pending[selected+1:]...)
			cases = append(cases[:selected], cases[selected+1:]...)
		}
	}
}

func (d *Downloader) SnapDir() string { return d.cfg.Dirs.Snap }

func (d *Downloader) torrentInfo(name string) (*downloaderrawdb.TorrentInfo, error) {
	var info *downloaderrawdb.TorrentInfo
	err := d.db.View(d.ctx, func(tx kv.Tx) (err error) {
		info, err = downloaderrawdb.ReadTorrentInfo(tx, name)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (d *Downloader) ReCalcStats(interval time.Duration) {
	d.lock.RLock()

	torrentClient := d.torrentClient

	peers := make(map[torrent.PeerID]struct{}, 16)

	prevStats, stats := d.stats, d.stats

	logger := d.logger
	verbosity := d.verbosity

	downloading := map[string]*downloadInfo{}

	for file, info := range d.downloading {
		i := *info
		downloading[file] = &i
	}

	webDownloadClient := d.webDownloadClient

	webDownloadInfo := map[string]webDownloadInfo{}

	for key, value := range d.webDownloadInfo {
		webDownloadInfo[key] = value
	}

	ctx := d.ctx

	d.lock.RUnlock()

	//Call this methods outside of `lock` critical section, because they have own locks with contention
	torrents := torrentClient.Torrents()
	connStats := torrentClient.ConnStats()

	stats.Completed = true
	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())
	stats.BytesHashed = uint64(connStats.BytesHashed.Int64())
	stats.BytesFlushed = uint64(connStats.BytesFlushed.Int64())
	stats.BytesDownload = uint64(connStats.BytesReadData.Int64())
	stats.BytesCompleted = uint64(connStats.BytesCompleted.Int64())

	lastMetadataReady := stats.MetadataReady

	stats.BytesTotal, stats.ConnectionsTotal, stats.MetadataReady = 0, 0, 0

	var zeroProgress []string
	var noMetadata []string

	isDiagEnabled := diagnostics.TypeOf(diagnostics.SnapshoFilesList{}).Enabled()
	if isDiagEnabled {
		filesList := make([]string, 0, len(torrents))
		for _, t := range torrents {
			filesList = append(filesList, t.Name())
		}
		diagnostics.Send(diagnostics.SnapshoFilesList{Files: filesList})
	}

	var dbInfo int
	var tComplete int
	var torrentInfo int

	downloadedBytes := int64(0)

	for _, t := range torrents {
		select {
		case <-t.GotInfo():
		default: // if some torrents have no metadata, we are for-sure uncomplete
			stats.Completed = false
			noMetadata = append(noMetadata, t.Name())
			continue
		}

		torrentName := t.Name()
		torrentComplete := t.Complete.Bool()
		torrentInfo++
		stats.MetadataReady++

		// call methods once - to reduce internal mutex contention
		peersOfThisFile := t.PeerConns()
		weebseedPeersOfThisFile := t.WebseedPeerConns()

		tLen := t.Length()
		var bytesCompleted int64

		if torrentComplete {
			tComplete++
			bytesCompleted = tLen
			delete(downloading, torrentName)
		} else {
			bytesCompleted = t.BytesCompleted()
		}

		progress := float32(float64(100) * (float64(bytesCompleted) / float64(tLen)))

		if info, ok := downloading[torrentName]; ok {
			if progress != info.progress {
				info.time = time.Now()
				info.progress = progress
			}
		}

		downloadedBytes += bytesCompleted
		stats.BytesTotal += uint64(tLen)

		for _, peer := range peersOfThisFile {
			stats.ConnectionsTotal++
			peers[peer.PeerID] = struct{}{}
		}

		webseedRates, webseeds := getWebseedsRatesForlogs(weebseedPeersOfThisFile, torrentName, t.Complete.Bool())
		rates, peers := getPeersRatesForlogs(peersOfThisFile, torrentName)

		if !torrentComplete {
			d.lock.RLock()
			info, err := d.torrentInfo(torrentName)
			d.lock.RUnlock()

			if err == nil {
				if info != nil {
					dbInfo++
				}
			} else if _, ok := webDownloadInfo[torrentName]; ok {
				stats.MetadataReady++
			} else {
				noMetadata = append(noMetadata, torrentName)
			}

			if progress == 0 {
				zeroProgress = append(zeroProgress, torrentName)
			}
		}

		// more detailed statistic: download rate of each peer (for each file)
		if !torrentComplete && progress != 0 {
			if info, ok := downloading[torrentName]; ok {
				info.time = time.Now()
				info.progress = progress
			}

			logger.Log(verbosity, "[snapshots] progress", "file", torrentName, "progress", fmt.Sprintf("%.2f%%", progress), "peers", len(peersOfThisFile), "webseeds", len(weebseedPeersOfThisFile))
			logger.Log(verbosity, "[snapshots] webseed peers", webseedRates...)
			logger.Log(verbosity, "[snapshots] bittorrent peers", rates...)
		}

		diagnostics.Send(diagnostics.SegmentDownloadStatistics{
			Name:            torrentName,
			TotalBytes:      uint64(tLen),
			DownloadedBytes: uint64(bytesCompleted),
			Webseeds:        webseeds,
			Peers:           peers,
		})

		stats.Completed = stats.Completed && torrentComplete
	}

	stats.BytesDownload = uint64(downloadedBytes)

	var webTransfers int32

	if webDownloadClient != nil {
		webStats, _ := webDownloadClient.Stats(ctx)

		if webStats != nil {
			if len(webStats.Transferring) != 0 && stats.Completed {
				stats.Completed = false
			}

			for _, transfer := range webStats.Transferring {
				stats.MetadataReady++
				webTransfers++

				bytesCompleted := transfer.Bytes
				tLen := transfer.Size
				transferName := transfer.Name

				delete(downloading, transferName)

				if bytesCompleted > tLen {
					bytesCompleted = tLen
				}

				stats.BytesTotal += tLen

				stats.BytesDownload += bytesCompleted

				if transfer.Percentage == 0 {
					zeroProgress = append(zeroProgress, transferName)
				}

				var seeds []diagnostics.SegmentPeer
				var webseedRates []interface{}
				if peerUrl, err := url.Parse(transfer.Group); err == nil {
					rate := uint64(transfer.SpeedAvg)
					seeds = []diagnostics.SegmentPeer{
						{
							Url:          peerUrl.Host,
							DownloadRate: rate,
						}}

					if shortUrl, err := url.JoinPath(peerUrl.Host, peerUrl.Path); err == nil {
						webseedRates = []interface{}{strings.TrimSuffix(shortUrl, "/"), fmt.Sprintf("%s/s", common.ByteCount(rate))}
					}
				}

				// more detailed statistic: download rate of each peer (for each file)
				if transfer.Percentage != 0 {
					logger.Log(verbosity, "[snapshots] progress", "file", transferName, "progress", fmt.Sprintf("%.2f%%", float32(transfer.Percentage)), "webseeds", 1)
					logger.Log(verbosity, "[snapshots] web peers", webseedRates...)
				}

				diagnostics.Send(diagnostics.SegmentDownloadStatistics{
					Name:            transferName,
					TotalBytes:      tLen,
					DownloadedBytes: bytesCompleted,
					Webseeds:        seeds,
				})
			}
		}
	}

	if len(downloading) > 0 {
		if webDownloadClient != nil {
			webTransfers += int32(len(downloading))
		}

		stats.Completed = false
	}

	if lastMetadataReady != stats.MetadataReady {
		now := time.Now()
		stats.LastMetadataUpdate = &now
	}

	if len(noMetadata) > 0 {
		amount := len(noMetadata)
		if len(noMetadata) > 5 {
			noMetadata = append(noMetadata[:5], "...")
		}
		logger.Info("[snapshots] no metadata yet", "files", amount, "list", strings.Join(noMetadata, ","))
	}

	var noDownloadProgress []string

	if len(zeroProgress) > 0 {
		amount := len(zeroProgress)

		for _, file := range zeroProgress {
			if _, ok := downloading[file]; ok {
				noDownloadProgress = append(noDownloadProgress, file)
			}
		}

		if len(zeroProgress) > 5 {
			zeroProgress = append(zeroProgress[:5], "...")
		}

		logger.Info("[snapshots] no progress yet", "files", amount, "list", strings.Join(zeroProgress, ","))
	}

	if len(downloading) > 0 {
		amount := len(downloading)

		files := make([]string, 0, len(downloading))
		for file, info := range downloading {
			files = append(files, fmt.Sprintf("%s (%.0f%%)", file, info.progress))

			if dp, ok := downloading[file]; ok {
				if time.Since(dp.time) > 30*time.Minute {
					noDownloadProgress = append(noDownloadProgress, file)
				}
			}
		}
		sort.Strings(files)

		logger.Log(verbosity, "[snapshots] files progress", "files", amount, "list", strings.Join(files, ", "))
	}

	if d.stuckFileDetailedLogs && time.Since(stats.lastTorrentStatus) > 5*time.Minute {
		stats.lastTorrentStatus = time.Now()

		if len(noDownloadProgress) > 0 {
			progressStatus := getProgressStatus(torrentClient, noDownloadProgress)
			for file, status := range progressStatus {
				logger.Debug(fmt.Sprintf("[snapshots] torrent status: %s\n    %s", file,
					string(bytes.TrimRight(bytes.ReplaceAll(status, []byte("\n"), []byte("\n    ")), "\n "))))
			}
		}
	}

	decay := func(prev uint64) uint64 {
		switch {
		case prev < 1000:
			return prev / 16
		case stats.FlushRate < 10000:
			return prev / 8
		case stats.FlushRate < 100000:
			return prev / 4
		default:
			return prev / 2
		}
	}

	if stats.BytesDownload > prevStats.BytesDownload {
		stats.DownloadRate = (stats.BytesDownload - prevStats.BytesDownload) / uint64(interval.Seconds())
	} else {
		stats.DownloadRate = decay(prevStats.DownloadRate)
	}

	if stats.BytesHashed > prevStats.BytesHashed {
		stats.HashRate = (stats.BytesHashed - prevStats.BytesHashed) / uint64(interval.Seconds())
	} else {
		stats.HashRate = decay(prevStats.HashRate)
	}

	if stats.BytesCompleted > stats.BytesTotal {
		stats.BytesCompleted = stats.BytesTotal
	}

	if stats.BytesCompleted > prevStats.BytesCompleted {
		stats.CompletionRate = (stats.BytesCompleted - prevStats.BytesCompleted) / uint64(interval.Seconds())
	} else {
		stats.CompletionRate = decay(prevStats.CompletionRate)
	}

	if stats.BytesFlushed > prevStats.BytesFlushed {
		stats.FlushRate = (stats.BytesFlushed - prevStats.BytesFlushed) / uint64(interval.Seconds())
	} else {
		stats.FlushRate = decay(prevStats.FlushRate)
	}

	if stats.BytesUpload > prevStats.BytesUpload {
		stats.UploadRate = (stats.BytesUpload - prevStats.BytesUpload) / uint64(interval.Seconds())
	} else {
		stats.UploadRate = decay(prevStats.UploadRate)
	}

	if stats.BytesTotal == 0 {
		stats.Progress = 0
	} else {
		stats.Progress = float32(float64(100) * (float64(stats.BytesCompleted) / float64(stats.BytesTotal)))
		if int(stats.Progress) == 100 && !stats.Completed {
			stats.Progress = 99.9
		}
	}

	stats.PeersUnique = int32(len(peers))
	stats.FilesTotal = int32(len(torrents)) + webTransfers

	d.lock.Lock()
	d.stats = stats

	for file, info := range d.downloading {
		if updated, ok := downloading[file]; ok {
			info.time = updated.time
			info.progress = updated.progress
		}
	}

	d.lock.Unlock()

	if stats.Completed {
		d.saveAllCompleteFlag()
	}

	if !stats.Completed {
		logger.Debug("[snapshots] downloading",
			"len", len(torrents),
			"webTransfers", webTransfers,
			"torrent", torrentInfo,
			"db", dbInfo,
			"t-complete", tComplete,
			"hashed", common.ByteCount(stats.BytesHashed),
			"hash-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.HashRate)),
			"completed", common.ByteCount(stats.BytesCompleted),
			"completion-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.CompletionRate)),
			"flushed", common.ByteCount(stats.BytesFlushed),
			"flush-rate", fmt.Sprintf("%s/s", common.ByteCount(stats.FlushRate)),
			"webseed-trips", stats.WebseedTripCount.Load(),
			"webseed-active", stats.WebseedActiveTrips.Load(),
			"webseed-max-active", stats.WebseedMaxActiveTrips.Load(),
			"webseed-discards", stats.WebseedDiscardCount.Load(),
			"webseed-fails", stats.WebseedServerFails.Load(),
			"webseed-bytes", common.ByteCount(uint64(stats.WebseedBytesDownload.Load())),
			"localHashes", stats.LocalFileHashes, "localHashTime", stats.LocalFileHashTime)
	}
}

type filterWriter struct {
	files     map[string][]byte
	remainder []byte
	file      string
}

func (f *filterWriter) Write(p []byte) (n int, err error) {
	written := len(p)

	p = append(f.remainder, p...)

	for len(p) > 0 {
		scanned, line, _ := bufio.ScanLines(p, false)

		if scanned > 0 {
			if len(f.file) > 0 {
				if len(line) == 0 {
					f.file = ""
				} else {
					line = append(line, '\n')
					f.files[f.file] = append(f.files[f.file], line...)
				}
			} else {
				if _, ok := f.files[string(line)]; ok {
					f.file = string(line)
				}
			}

			p = p[scanned:]
		} else {
			f.remainder = p
			p = nil
		}
	}
	return written, nil
}

func getProgressStatus(torrentClient *torrent.Client, noDownloadProgress []string) map[string][]byte {
	writer := filterWriter{
		files: map[string][]byte{},
	}

	for _, file := range noDownloadProgress {
		writer.files[file] = nil
	}

	torrentClient.WriteStatus(&writer)

	return writer.files
}

func getWebseedsRatesForlogs(weebseedPeersOfThisFile []*torrent.Peer, fName string, finished bool) ([]interface{}, []diagnostics.SegmentPeer) {
	seeds := make([]diagnostics.SegmentPeer, 0, len(weebseedPeersOfThisFile))
	webseedRates := make([]interface{}, 0, len(weebseedPeersOfThisFile)*2)
	webseedRates = append(webseedRates, "file", fName)
	for _, peer := range weebseedPeersOfThisFile {
		if peerUrl, err := webPeerUrl(peer); err == nil {
			if shortUrl, err := url.JoinPath(peerUrl.Host, peerUrl.Path); err == nil {
				rate := uint64(peer.DownloadRate())
				if !finished {
					seed := diagnostics.SegmentPeer{
						Url:          peerUrl.Host,
						DownloadRate: rate,
					}
					seeds = append(seeds, seed)
				}
				webseedRates = append(webseedRates, strings.TrimSuffix(shortUrl, "/"), fmt.Sprintf("%s/s", common.ByteCount(rate)))
			}
		}
	}

	return webseedRates, seeds
}

func webPeerUrl(peer *torrent.Peer) (*url.URL, error) {
	root, _ := path.Split(strings.Trim(strings.TrimPrefix(peer.String(), "webseed peer for "), "\""))
	return url.Parse(root)
}

func getPeersRatesForlogs(peersOfThisFile []*torrent.PeerConn, fName string) ([]interface{}, []diagnostics.SegmentPeer) {
	peers := make([]diagnostics.SegmentPeer, 0, len(peersOfThisFile))
	rates := make([]interface{}, 0, len(peersOfThisFile)*2)
	rates = append(rates, "file", fName)

	for _, peer := range peersOfThisFile {
		dr := uint64(peer.DownloadRate())
		url := fmt.Sprintf("%v", peer.PeerClientName.Load())

		segPeer := diagnostics.SegmentPeer{
			Url:          url,
			DownloadRate: dr,
		}
		peers = append(peers, segPeer)
		rates = append(rates, url, fmt.Sprintf("%s/s", common.ByteCount(dr)))
	}

	return rates, peers
}

func (d *Downloader) VerifyData(ctx context.Context, whiteList []string, failFast bool) error {
	total := 0
	allTorrents := d.torrentClient.Torrents()
	toVerify := make([]*torrent.Torrent, 0, len(allTorrents))
	for _, t := range allTorrents {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.GotInfo(): //files to verify already have .torrent on disk. means must have `Info()` already
		default: // skip other files
			continue
		}

		exists, err := dir.FileExist(filepath.Join(d.SnapDir(), t.Name()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		if len(whiteList) > 0 {
			name := t.Name()
			exactOrPartialMatch := slices.ContainsFunc(whiteList, func(s string) bool {
				return name == s || strings.HasSuffix(name, s) || strings.HasPrefix(name, s)
			})
			if !exactOrPartialMatch {
				continue
			}
		}
		toVerify = append(toVerify, t)
		total += t.NumPieces()
	}
	d.logger.Info("[snapshots] Verify start")
	defer d.logger.Info("[snapshots] Verify done", "files", len(toVerify), "whiteList", whiteList)

	completedPieces, completedFiles := &atomic.Uint64{}, &atomic.Uint64{}

	{
		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-logEvery.C:
					d.logger.Info("[snapshots] Verify",
						"progress", fmt.Sprintf("%.2f%%", 100*float64(completedPieces.Load())/float64(total)),
						"files", fmt.Sprintf("%d/%d", completedFiles.Load(), len(toVerify)),
						"sz_gb", downloadercfg.DefaultPieceSize*completedPieces.Load()/1024/1024/1024,
					)
				}
			}
		}()
	}

	g, ctx := errgroup.WithContext(ctx)
	// torrent lib internally limiting amount of hashers per file
	// set limit here just to make load predictable, not to control Disk/CPU consumption
	g.SetLimit(runtime.GOMAXPROCS(-1) * 4)
	for _, t := range toVerify {
		t := t
		g.Go(func() error {
			defer completedFiles.Add(1)
			if failFast {
				return VerifyFileFailFast(ctx, t, d.SnapDir(), completedPieces)
			}

			err := ScheduleVerifyFile(ctx, t, completedPieces)

			if err != nil || !t.Complete.Bool() {
				if err := d.db.Update(ctx, torrentInfoReset(t.Name(), t.InfoHash().Bytes(), 0)); err != nil {
					return fmt.Errorf("verify data: %s: reset failed: %w", t.Name(), err)
				}
			}

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

// AddNewSeedableFile decides what we do depending on whether we have the .seg file or the .torrent file
// have .torrent no .seg => get .seg file from .torrent
// have .seg no .torrent => get .torrent from .seg
func (d *Downloader) AddNewSeedableFile(ctx context.Context, name string) error {
	ff, isStateFile, ok := snaptype.ParseFileName("", name)
	if ok {
		if isStateFile {
			if !snaptype.E3Seedable(name) {
				return nil
			}
		} else {
			if !d.cfg.SnapshotConfig.Seedable(ff) {
				return nil
			}
		}
	}

	// if we don't have the torrent file we build it if we have the .seg file
	_, err := BuildTorrentIfNeed(ctx, name, d.SnapDir(), d.torrentFS)
	if err != nil {
		return fmt.Errorf("AddNewSeedableFile: %w", err)
	}
	ts, err := d.torrentFS.LoadByName(name)
	if err != nil {
		return fmt.Errorf("AddNewSeedableFile: %w", err)
	}
	_, _, err = addTorrentFile(ctx, ts, d.torrentClient, d.db, d.webseeds)
	if err != nil {
		return fmt.Errorf("addTorrentFile: %w", err)
	}
	return nil
}

func (d *Downloader) alreadyHaveThisName(name string) bool {
	for _, t := range d.torrentClient.Torrents() {
		if t.Info() != nil {
			if t.Name() == name {
				return true
			}
		}
	}
	return false
}

func (d *Downloader) AddMagnetLink(ctx context.Context, infoHash metainfo.Hash, name string) error {
	// Paranoic Mode on: if same file changed infoHash - skip it
	// Example:
	//  - Erigon generated file X with hash H1. User upgraded Erigon. New version has preverified file X with hash H2. Must ignore H2 (don't send to Downloader)
	if d.alreadyHaveThisName(name) || !IsSnapNameAllowed(name) {
		return nil
	}
	isProhibited, err := d.torrentFS.NewDownloadsAreProhibited(name)
	if err != nil {
		return err
	}

	exists, err := d.torrentFS.Exists(name)
	if err != nil {
		return err
	}

	if isProhibited && !exists {
		return nil
	}

	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	magnet := mi.Magnet(&infoHash, &metainfo.Info{Name: name})
	spec, err := torrent.TorrentSpecFromMagnetUri(magnet.String())

	if err != nil {
		return err
	}

	t, ok, err := addTorrentFile(ctx, spec, d.torrentClient, d.db, d.webseeds)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	d.wg.Add(1)
	go func(t *torrent.Torrent) {
		defer d.wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-t.GotInfo():
		case <-time.After(30 * time.Second): //fallback to r2
			// TOOD: handle errors
			// TOOD: add `d.webseeds.Complete` chan - to prevent race - Discover is also async
			// TOOD: maybe run it in goroutine and return channel - to select with p2p

			ts, ok, err := d.webseeds.DownloadAndSaveTorrentFile(ctx, name)
			if ok && err == nil {
				_, _, err = addTorrentFile(ctx, ts, d.torrentClient, d.db, d.webseeds)
				if err != nil {
					return
				}
				return
			}

			// wait for p2p
			select {
			case <-ctx.Done():
				return
			case <-t.GotInfo():
			}
		}

		mi := t.Metainfo()
		if _, err := d.torrentFS.CreateWithMetaInfo(t.Info(), &mi); err != nil {
			d.logger.Warn("[snapshots] create torrent file", "err", err)
			return
		}

		urls, ok := d.webseeds.ByFileName(t.Name())
		if ok {
			t.AddWebSeeds(urls)
		}
	}(t)
	//log.Debug("[downloader] downloaded both seg and torrent files", "hash", infoHash)
	return nil
}

func SeedableFiles(dirs datadir.Dirs, chainName string, all bool) ([]string, error) {
	files, err := seedableSegmentFiles(dirs.Snap, chainName, all)
	if err != nil {
		return nil, fmt.Errorf("seedableSegmentFiles: %w", err)
	}
	l1, err := seedableStateFilesBySubDir(dirs.Snap, "idx", all)
	if err != nil {
		return nil, err
	}
	l2, err := seedableStateFilesBySubDir(dirs.Snap, "history", all)
	if err != nil {
		return nil, err
	}
	l3, err := seedableStateFilesBySubDir(dirs.Snap, "domain", all)
	if err != nil {
		return nil, err
	}
	var l4 []string
	if all {
		l4, err = seedableStateFilesBySubDir(dirs.Snap, "accessor", all)
		if err != nil {
			return nil, err
		}
	}
	files = append(append(append(append(files, l1...), l2...), l3...), l4...)
	return files, nil
}

const ParallelVerifyFiles = 4 // keep it small, to allow big `PieceHashersPerTorrent`. More `PieceHashersPerTorrent` - faster handling of big files.

func (d *Downloader) addTorrentFilesFromDisk(quiet bool) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	eg, ctx := errgroup.WithContext(d.ctx)
	eg.SetLimit(ParallelVerifyFiles)

	files, err := AllTorrentSpecs(d.cfg.Dirs, d.torrentFS)
	if err != nil {
		return err
	}

	// reduce mutex contention inside torrentClient - by enabling in/out peers connection after addig all files
	for _, ts := range files {
		ts.Trackers = nil
		ts.DisallowDataDownload = true
	}
	defer func() {
		tl := d.torrentClient.Torrents()
		for _, t := range tl {
			t.AllowDataUpload()
			t.AddTrackers(Trackers)
		}
	}()

	for i, ts := range files {
		//TODO: why we depend on Stat? Did you mean `dir.FileExist()` ? How it can be false here?
		//TODO: What this code doing? Why delete something from db?
		//if info, err := d.torrentInfo(ts.DisplayName); err == nil {
		//	if info.Completed != nil {
		//		_, serr := os.Stat(filepath.Join(d.SnapDir(), info.Name))
		//		if serr != nil {
		//			if err := d.db.Update(d.ctx, func(tx kv.RwTx) error {
		//				return tx.Delete(kv.BittorrentInfo, []byte(info.Name))
		//			}); err != nil {
		//				log.Error("[snapshots] Failed to delete db entry after stat error", "file", info.Name, "err", err, "stat-err", serr)
		//			}
		//		}
		//	}
		//}

		// this check is performed here becuase t.MergeSpec in addTorrentFile will do a file
		// update in place when it opens its MemMap.  This is non destructive for the data
		// but casues an update to the file which changes its size to the torrent length which
		// invalidated the file length check
		if info, err := d.torrentInfo(ts.DisplayName); err == nil {
			if info.Completed != nil {
				fi, serr := os.Stat(filepath.Join(d.SnapDir(), info.Name))
				if serr != nil || fi.Size() != *info.Length || !fi.ModTime().Equal(*info.Completed) {
					if err := d.db.Update(d.ctx, torrentInfoReset(info.Name, info.Hash, *info.Length)); err != nil {
						if serr != nil {
							log.Error("[snapshots] Failed to reset db entry after stat error", "file", info.Name, "err", err, "stat-err", serr)
						} else {
							log.Error("[snapshots] Failed to reset db entry after stat mismatch", "file", info.Name, "err", err)
						}
					}
				}
			}
		}

		if whitelisted, ok := d.webseeds.torrentsWhitelist.Get(ts.DisplayName); ok {
			if ts.InfoHash.HexString() != whitelisted.Hash {
				continue
			}
		}

		ts := ts
		i := i
		eg.Go(func() error {
			_, _, err := addTorrentFile(ctx, ts, d.torrentClient, d.db, d.webseeds)
			if err != nil {
				return err
			}
			select {
			case <-logEvery.C:
				if !quiet {
					log.Info("[snapshots] Adding .torrent files", "progress", fmt.Sprintf("%d/%d", i, len(files)))
				}
			default:
			}
			return nil
		})
	}
	return eg.Wait()
}
func (d *Downloader) BuildTorrentFilesIfNeed(ctx context.Context, chain string, ignore snapcfg.Preverified) error {
	_, err := BuildTorrentFilesIfNeed(ctx, d.cfg.Dirs, d.torrentFS, chain, ignore, false)
	return err
}

func (d *Downloader) Stats() AggStats {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.stats
}

func (d *Downloader) Close() {
	d.logger.Info("[snapshots] stopping downloader", "files", len(d.torrentClient.Torrents()))
	d.stopMainLoop()
	d.wg.Wait()
	d.logger.Info("[snapshots] closing torrents")
	d.torrentClient.Close()
	if err := d.folder.Close(); err != nil {
		d.logger.Warn("[snapshots] folder.close", "err", err)
	}
	if err := d.pieceCompletionDB.Close(); err != nil {
		d.logger.Warn("[snapshots] pieceCompletionDB.close", "err", err)
	}
	d.logger.Info("[snapshots] closing db")
	d.db.Close()
	d.logger.Info("[snapshots] downloader stopped")
}

func (d *Downloader) PeerID() []byte {
	peerID := d.torrentClient.PeerID()
	return peerID[:]
}

func (d *Downloader) StopSeeding(hash metainfo.Hash) error {
	t, ok := d.torrentClient.Torrent(hash)
	if !ok {
		return nil
	}
	ch := t.Closed()
	t.Drop()
	<-ch
	return nil
}

func (d *Downloader) TorrentClient() *torrent.Client { return d.torrentClient }

func openClient(ctx context.Context, dbDir, snapDir string, cfg *torrent.ClientConfig, writeMap bool, logger log.Logger) (db kv.RwDB, c storage.PieceCompletion, m storage.ClientImplCloser, torrentClient *torrent.Client, err error) {
	dbCfg := mdbx.NewMDBX(log.New()).
		Label(kv.DownloaderDB).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).
		GrowthStep(16 * datasize.MB).
		MapSize(16 * datasize.GB).
		PageSize(uint64(4 * datasize.KB)).
		RoTxsLimiter(semaphore.NewWeighted(9_000)).
		Path(dbDir).
		WriteMap(writeMap)
	db, err = dbCfg.Open(ctx)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrentcfg.openClient: %w", err)
	}
	//c, err = NewMdbxPieceCompletion(db)
	c, err = NewMdbxPieceCompletion(db, logger)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrentcfg.NewMdbxPieceCompletion: %w", err)
	}

	//Reasons why using MMAP instead of files-API:
	// - i see "10K threads exchaused" error earlier (on `--torrent.download.slots=500` and `pd-ssd`)
	// - "sig-bus" at disk-full - may happen anyway, because DB is mmap
	// - MMAP - means less GC pressure, more zero-copy
	// - MMAP files are pre-allocated - which is not cool, but: 1. we can live with it 2. maybe can just resize MMAP in future
	// See also: https://github.com/erigontech/erigon/pull/10074
	m = storage.NewMMapWithCompletion(snapDir, c)
	//m = storage.NewFileOpts(storage.NewFileClientOpts{
	//	ClientBaseDir:   snapDir,
	//	PieceCompletion: c,
	//})
	cfg.DefaultStorage = m

	dnsResolver := &downloadercfg.DnsCacheResolver{RefreshTimeout: 24 * time.Hour}
	cfg.TrackerDialContext = dnsResolver.DialContext

	err = func() (err error) {
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Errorf("openTorrentClient: %v", e)
			}
		}()

		torrentClient, err = torrent.NewClient(cfg)
		if err != nil {
			return fmt.Errorf("torrent.NewClient: %w", err)
		}
		return err
	}()

	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrentcfg.openClient: %w", err)
	}

	go func() {
		dnsResolver.Run(ctx)
	}()

	return db, c, m, torrentClient, nil
}

func (d *Downloader) SetLogPrefix(prefix string) {
	d.logPrefix = prefix
}

func (d *Downloader) logProgress() {
	var m runtime.MemStats
	prefix := d.logPrefix

	if d.logPrefix == "" {
		prefix = "snapshots"
	}

	if d.stats.Completed {
		log.Info(fmt.Sprintf("[%s] Downloading complete", prefix), "time", time.Since(d.startTime).String())
	}

	dbg.ReadMemStats(&m)

	status := "Downloading"

	percentDone := float32(100) * (float32(d.stats.BytesDownload) / float32(d.stats.BytesTotal))
	bytesDone := d.stats.BytesDownload
	rate := d.stats.DownloadRate
	remainingBytes := d.stats.BytesTotal - d.stats.BytesDownload

	if d.stats.BytesDownload >= d.stats.BytesTotal && d.stats.MetadataReady == d.stats.FilesTotal && d.stats.BytesTotal > 0 {
		status = "Verifying"
		percentDone = float32(100) * (float32(d.stats.BytesCompleted) / float32(d.stats.BytesTotal))
		bytesDone = d.stats.BytesCompleted
		rate = d.stats.CompletionRate
		remainingBytes = d.stats.BytesTotal - d.stats.BytesCompleted
	}

	if d.stats.BytesTotal == 0 {
		percentDone = 0
	}

	timeLeft := calculateTime(remainingBytes, rate)

	if !d.stats.Completed {
		log.Info(fmt.Sprintf("[%s] %s", prefix, status),
			"progress", fmt.Sprintf("(%d/%d files) %.2f%% - %s/%s", d.stats.MetadataReady, d.stats.FilesTotal, percentDone, common.ByteCount(bytesDone), common.ByteCount(d.stats.BytesTotal)),
			"rate", fmt.Sprintf("%s/s", common.ByteCount(rate)),
			"time-left", timeLeft,
			"total-time", time.Since(d.startTime).Round(time.Second).String(),
			"download-rate", fmt.Sprintf("%s/s", common.ByteCount(d.stats.DownloadRate)),
			"completion-rate", fmt.Sprintf("%s/s", common.ByteCount(d.stats.CompletionRate)),
			"alloc", common.ByteCount(m.Alloc),
			"sys", common.ByteCount(m.Sys))

		diagnostics.Send(diagnostics.SnapshotDownloadStatistics{
			Downloaded:           bytesDone,
			Total:                d.stats.BytesTotal,
			TotalTime:            time.Since(d.startTime).Round(time.Second).Seconds(),
			DownloadRate:         d.stats.DownloadRate,
			UploadRate:           d.stats.UploadRate,
			Peers:                d.stats.PeersUnique,
			Files:                d.stats.FilesTotal,
			Connections:          d.stats.ConnectionsTotal,
			Alloc:                m.Alloc,
			Sys:                  m.Sys,
			DownloadFinished:     d.stats.Completed,
			TorrentMetadataReady: d.stats.MetadataReady,
		})
	}
}

func calculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "999hrs:99m"
	}
	timeLeftInSeconds := amountLeft / rate

	hours := timeLeftInSeconds / 3600
	minutes := (timeLeftInSeconds / 60) % 60

	return fmt.Sprintf("%dhrs:%dm", hours, minutes)
}

func (d *Downloader) Completed() bool {
	return d.stats.Completed
}

func (d *Downloader) saveAllCompleteFlag() {
	if err := d.db.Update(d.ctx, downloaderrawdb.WriteAllCompleteFlag); err != nil {
		d.logger.Debug("[snapshots] Can't update 'all complete' flag", "err", err)
	}
}

// Store completed torrents in order to notify GrpcServer subscribers when they subscribe and there is already downloaded files
func (d *Downloader) torrentCompleted(tName string, tHash metainfo.Hash) {
	d.lock.Lock()
	defer d.lock.Unlock()
	hash := InfoHashes2Proto(tHash)

	//check is torrent already completed cause some funcs may call this method multiple times
	if _, ok := d.completedTorrents[tName]; !ok {
		d.notifyCompleted(tName, hash)
	}

	d.completedTorrents[tName] = completedTorrentInfo{
		path: tName,
		hash: hash,
	}
}

// Notify GrpcServer subscribers about completed torrent
func (d *Downloader) notifyCompleted(tName string, tHash *prototypes.H160) {
	d.onTorrentComplete(tName, tHash)
}

func (d *Downloader) getCompletedTorrents() map[string]completedTorrentInfo {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return d.completedTorrents
}
