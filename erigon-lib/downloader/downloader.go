/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package downloader

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/anacrolix/torrent/types/infohash"
	"github.com/c2h5oh/datasize"
	dir2 "github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/log/v3"
	"github.com/tidwall/btree"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"

	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
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

	torrentFiles    *TorrentFiles
	snapshotLock    *snapshotLock
	webDownloadInfo map[string]webDownloadInfo
	downloading     map[string]struct{}
	downloadLimit   *rate.Limit
}

type webDownloadInfo struct {
	url     *url.URL
	length  int64
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

	BytesCompleted, BytesTotal     uint64
	DroppedCompleted, DroppedTotal uint64

	BytesDownload, BytesUpload uint64
	UploadRate, DownloadRate   uint64
}

func New(ctx context.Context, cfg *downloadercfg.Cfg, dirs datadir.Dirs, logger log.Logger, verbosity log.Lvl, discover bool) (*Downloader, error) {
	db, c, m, torrentClient, err := openClient(ctx, cfg.Dirs.Downloader, cfg.Dirs.Snap, cfg.ClientConfig)
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

	lock, err := getSnapshotLock(ctx, cfg, db, logger)

	if err != nil {
		return nil, fmt.Errorf("can't initialize snapshot lock: %w", err)
	}

	d := &Downloader{
		cfg:                 cfg,
		db:                  db,
		pieceCompletionDB:   c,
		folder:              m,
		torrentClient:       torrentClient,
		lock:                &sync.RWMutex{},
		webseeds:            &WebSeeds{logger: logger, verbosity: verbosity, downloadTorrentFile: cfg.DownloadTorrentFilesFromWebseed, torrentsWhitelist: lock.Downloads},
		logger:              logger,
		verbosity:           verbosity,
		torrentFiles:        &TorrentFiles{dir: cfg.Dirs.Snap},
		snapshotLock:        lock,
		webDownloadInfo:     map[string]webDownloadInfo{},
		webDownloadSessions: map[string]*RCloneSession{},
		downloading:         map[string]struct{}{},
		webseedsDiscover:    discover,
	}

	if cfg.ClientConfig.DownloadRateLimiter != nil {
		downloadLimit := cfg.ClientConfig.DownloadRateLimiter.Limit()
		d.downloadLimit = &downloadLimit
	}

	d.webseeds.torrentFiles = d.torrentFiles
	d.ctx, d.stopMainLoop = context.WithCancel(ctx)

	if cfg.AddTorrentsFromDisk {
		var downloadMismatches []string

		for _, download := range lock.Downloads {
			if info, err := d.torrentInfo(download.Name); err == nil {
				if info.Completed != nil {
					if hash := hex.EncodeToString(info.Hash); download.Hash != hash {
						fileInfo, _, _ := snaptype.ParseFileName(d.SnapDir(), download.Name)

						// this is lazy as it can be expensive for large files
						fileHashBytes, err := fileHashBytes(d.ctx, fileInfo)

						if errors.Is(err, os.ErrNotExist) {
							hashBytes, _ := hex.DecodeString(download.Hash)
							if err := d.db.Update(d.ctx, torrentInfoReset(download.Name, hashBytes, 0)); err != nil {
								d.logger.Debug("[snapshots] Can't update torrent info", "file", download.Name, "hash", download.Hash, "err", err)
							}
							continue
						}

						fileHash := hex.EncodeToString(fileHashBytes)

						if fileHash != download.Hash && fileHash != hash {
							d.logger.Error("[snapshots] download db mismatch", "file", download.Name, "lock", download.Hash, "db", hash, "disk", fileHash, "downloaded", *info.Completed)
							downloadMismatches = append(downloadMismatches, download.Name)
						} else {
							d.logger.Warn("[snapshots] lock hash does not match completed download", "file", download.Name, "lock", hash, "download", download.Hash, "downloaded", *info.Completed)
						}
					}
				}
			}
		}

		if len(downloadMismatches) > 0 {
			return nil, fmt.Errorf("downloaded files have mismatched hashes: %s", strings.Join(downloadMismatches, ","))
		}

		if err := d.addPreConfiguredHashes(ctx, lock.Downloads); err != nil {
			return nil, err
		}

		if err := d.BuildTorrentFilesIfNeed(d.ctx, lock.Chain, lock.Downloads); err != nil {
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

func getSnapshotLock(ctx context.Context, cfg *downloadercfg.Cfg, db kv.RoDB, logger log.Logger) (*snapshotLock, error) {

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
	}

	return &lock, nil
}

func initSnapshotLock(ctx context.Context, cfg *downloadercfg.Cfg, db kv.RoDB, logger log.Logger) (*snapshotLock, error) {
	lock := &snapshotLock{
		Chain: cfg.ChainName,
	}

	files, err := seedableFiles(cfg.Dirs, cfg.ChainName)
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
	var i atomic.Int32

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	for _, file := range files {
		file := file

		g.Go(func() error {
			i.Add(1)

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
				hashBytes, err := localHashBytes(ctx, fileInfo, db)

				if err != nil {
					return fmt.Errorf("localHashBytes: %w", err)
				}

				downloadsMutex.Lock()
				defer downloadsMutex.Unlock()

				if hash := hex.EncodeToString(hashBytes); preverified.Hash == hash {
					downloadMap.Set(fileInfo.Name(), preverified)
				} else {
					logger.Warn("[downloader] local file hash does not match known", "file", fileInfo.Name(), "local", hash, "known", preverified.Hash)
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

				hashBytes, err := localHashBytes(ctx, fileInfo, db)

				if err != nil {
					return fmt.Errorf("localHashBytes: %w", err)
				}

				downloadsMutex.Lock()
				defer downloadsMutex.Unlock()

				if preverified, ok := versioned.Preverified.Get(fileInfo.Name()); ok {
					if hash := hex.EncodeToString(hashBytes); preverified.Hash == hash {
						downloadMap.Set(preverified.Name, preverified)
					} else {
						logger.Warn("[downloader] local file hash does not match known", "file", fileInfo.Name(), "local", hash, "known", preverified.Hash)
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

					hashBytes, err := localHashBytes(ctx, fileInfo, db)

					if err != nil {
						return err
					}

					if preverified, ok := versioned.Preverified.Get(fileInfo.Name()); ok {
						if hash := hex.EncodeToString(hashBytes); preverified.Hash == hash {
							downloadMap.Set(preverified.Name, preverified)
						} else {
							logger.Warn("[downloader] local file hash does not match known", "file", fileInfo.Name(), "local", hash, "known", preverified.Hash)
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
		for int(i.Load()) < len(files) {
			select {
			case <-ctx.Done():
				return // g.Wait() will return right error
			case <-logEvery.C:
				if int(i.Load()) == len(files) {
					return
				}
				log.Info("[snapshots] Initiating snapshot-lock", "progress", fmt.Sprintf("%d/%d", i.Load(), len(files)))
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

func localHashBytes(ctx context.Context, fileInfo snaptype.FileInfo, db kv.RoDB) ([]byte, error) {
	var hashBytes []byte

	if db != nil {
		err := db.View(ctx, func(tx kv.Tx) (err error) {
			infoBytes, err := tx.GetOne(kv.BittorrentInfo, []byte(fileInfo.Name()))

			if err != nil {
				return err
			}

			if len(infoBytes) == 20 {
				hashBytes = infoBytes
				return nil
			}

			var info torrentInfo

			if err = json.Unmarshal(infoBytes, &info); err == nil {
				hashBytes = info.Hash
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

	return fileHashBytes(ctx, fileInfo)
}

func fileHashBytes(ctx context.Context, fileInfo snaptype.FileInfo) ([]byte, error) {
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

// Add pre-configured
func (d *Downloader) addPreConfiguredHashes(ctx context.Context, snapshots snapcfg.Preverified) error {
	for _, it := range snapshots {
		if err := d.addMagnetLink(ctx, snaptype.Hex2InfoHash(it.Hash), it.Name, true); err != nil {
			return err
		}
	}
	return nil
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
			d.webseeds.Discover(d.ctx, d.cfg.WebSeedUrls, d.cfg.WebSeedFiles, d.cfg.Dirs.Snap)
			// webseeds.Discover may create new .torrent files on disk
			if err := d.addTorrentFilesFromDisk(true); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Warn("[snapshots] addTorrentFilesFromDisk", "err", err)
			}
		}()
	}

	var sem = semaphore.NewWeighted(int64(d.cfg.DownloadSlots))

	d.webDownloadClient, _ = NewRCloneClient(d.logger)

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		complete := map[string]struct{}{}
		checking := map[string]struct{}{}
		failed := map[string]struct{}{}
		downloadComplete := make(chan downloadStatus, 100)
		seedHashMismatches := map[infohash.T][]*seedHash{}

		// set limit here to make load predictable, not to control Disk/CPU consumption
		// will impact start times depending on the amount of non complete files - should
		// be low unless the download db is deleted - in which case all files may be checked
		checkGroup, _ := errgroup.WithContext(d.ctx)
		checkGroup.SetLimit(runtime.GOMAXPROCS(-1) * 4)

		for {
			torrents := d.torrentClient.Torrents()

			var pending []*torrent.Torrent

			for _, t := range torrents {
				if _, ok := complete[t.Name()]; ok {
					continue
				}

				if _, ok := failed[t.Name()]; ok {
					continue
				}

				if isComplete, length, completionTime := d.checkComplete(t.Name()); isComplete && completionTime != nil {
					if _, ok := checking[t.Name()]; !ok {
						fileInfo, _, _ := snaptype.ParseFileName(d.SnapDir(), t.Name())

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
										fileHashBytes, _ := fileHashBytes(d.ctx, fileInfo)

										if bytes.Equal(infoHash.Bytes(), fileHashBytes) {
											downloadComplete <- downloadStatus{
												name:     fileInfo.Name(),
												length:   length,
												infoHash: infoHash,
											}
										} else {
											downloadComplete <- downloadStatus{
												name: fileInfo.Name(),
												err:  fmt.Errorf("hash check failed"),
											}

											d.logger.Warn("[snapshots] Torrent hash does not match file", "file", fileInfo.Name(), "torrent-hash", infoHash, "file-hash", hex.EncodeToString(fileHashBytes))
										}

										return nil
									})
								}(fileInfo, t.InfoHash(), length, *completionTime)

							} else {
								complete[t.Name()] = struct{}{}
								continue
							}
						}
					}
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
					fileInfo, _, _ := snaptype.ParseFileName(d.SnapDir(), t.Name())

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
						d.logger.Warn("Failed to update file info", "file", t.Info().Name, "err", err)
					}

					d.lock.Lock()
					delete(d.downloading, t.Name())
					d.lock.Unlock()
					complete[t.Name()] = struct{}{}
					continue
				}

				if downloading {
					continue
				}

				pending = append(pending, t)
			}

			select {
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
					fileInfo, _, _ := snaptype.ParseFileName(d.SnapDir(), status.name)

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

					if err := d.db.Update(d.ctx,
						torrentInfoUpdater(status.name, status.infoHash.Bytes(), status.length, completionTime)); err != nil {
						d.logger.Warn("Failed to update file info", "file", status.name, "err", err)
					}

					complete[status.name] = struct{}{}
					continue
				} else {
					delete(complete, status.name)
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

			d.lock.RLock()
			downloadingLen := len(d.downloading)
			d.stats.Downloading = int32(downloadingLen)
			d.lock.RUnlock()

			available := availableTorrents(d.ctx, pending, d.cfg.DownloadSlots-downloadingLen)

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
					wi, _, _ := snaptype.ParseFileName(d.SnapDir(), webDownload.torrent.Name())

					for i, t := range available {
						ai, _, _ := snaptype.ParseFileName(d.SnapDir(), t.Name())

						if ai.CompareTo(wi) > 0 {
							available[i] = webDownload.torrent
							break
						}
					}
				}
			}
			d.lock.RUnlock()

			for _, t := range available {

				torrentInfo, _ := d.torrentInfo(t.Name())
				fileInfo, _, _ := snaptype.ParseFileName(d.SnapDir(), t.Name())

				if torrentInfo != nil && torrentInfo.Completed != nil {
					if bytes.Equal(t.InfoHash().Bytes(), torrentInfo.Hash) {
						if _, err := os.Stat(filepath.Join(d.SnapDir(), t.Name())); err == nil {
							localHash, complete := localHashCompletionCheck(d.ctx, t, fileInfo, downloadComplete)

							if complete {
								d.logger.Debug("[snapshots] Download already complete", "file", t.Name(), "hash", t.InfoHash())
								continue
							}

							failed[t.Name()] = struct{}{}
							d.logger.Warn("[snapshots] file hash does not match download", "file", t.Name(), "got", hex.EncodeToString(localHash), "expected", t.InfoHash(), "downloaded", *torrentInfo.Completed)
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

						if _, complete := localHashCompletionCheck(d.ctx, t, fileInfo, downloadComplete); complete {
							d.logger.Debug("[snapshots] Download already complete", "file", t.Name(), "hash", t.InfoHash())
							continue
						}
					}
				} else {
					if _, complete := localHashCompletionCheck(d.ctx, t, fileInfo, downloadComplete); complete {
						d.logger.Debug("[snapshots] Download already complete", "file", t.Name(), "hash", t.InfoHash())
						continue
					}
				}

				switch {
				case len(t.PeerConns()) > 0:
					d.logger.Debug("[snapshots] Downloading from torrent", "file", t.Name(), "peers", len(t.PeerConns()))
					d.torrentDownload(t, downloadComplete, sem)
				case len(t.WebseedPeerConns()) > 0:
					if d.webDownloadClient != nil {
						var peerUrls []*url.URL

						for _, peer := range t.WebseedPeerConns() {
							if peerUrl, err := webPeerUrl(peer); err == nil {
								peerUrls = append(peerUrls, peerUrl)
							}
						}

						d.logger.Debug("[snapshots] Downloading from webseed", "file", t.Name(), "webpeers", len(t.WebseedPeerConns()))
						session, err := d.webDownload(peerUrls, t, nil, downloadComplete, sem)

						if err != nil {
							d.logger.Warn("Can't complete web download", "file", t.Info().Name, "err", err)

							if session == nil {
								d.torrentDownload(t, downloadComplete, sem)
							}

							continue
						}

					} else {
						d.logger.Debug("[snapshots] Downloading from torrent", "file", t.Name(), "peers", len(t.PeerConns()), "webpeers", len(t.WebseedPeerConns()))
						d.torrentDownload(t, downloadComplete, sem)
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
						d.webDownload([]*url.URL{peerUrl}, t, &webDownload, downloadComplete, sem)
						continue
					}

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
	justCompleted := true
	for {
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-statEvery.C:
			d.ReCalcStats(statInterval)

		case <-logEvery.C:
			if silent {
				continue
			}

			stats := d.Stats()

			dbg.ReadMemStats(&m)
			if stats.Completed {
				if justCompleted {
					justCompleted = false
					// force fsync of db. to not loose results of downloading on power-off
					_ = d.db.Update(d.ctx, func(tx kv.RwTx) error { return nil })
				}

				d.logger.Info("[snapshots] Seeding",
					"up", common.ByteCount(stats.UploadRate)+"/s",
					"peers", stats.PeersUnique,
					"conns", stats.ConnectionsTotal,
					"files", stats.FilesTotal,
					"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
				)
				continue
			}

			d.logger.Info("[snapshots] Downloading",
				"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, common.ByteCount(stats.BytesCompleted), common.ByteCount(stats.BytesTotal)),
				"downloading", stats.Downloading,
				"download", common.ByteCount(stats.DownloadRate)+"/s",
				"upload", common.ByteCount(stats.UploadRate)+"/s",
				"peers", stats.PeersUnique,
				"conns", stats.ConnectionsTotal,
				"files", stats.FilesTotal,
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
			)

			if stats.PeersUnique == 0 {
				ips := d.TorrentClient().BadPeerIPs()
				if len(ips) > 0 {
					d.logger.Info("[snapshots] Stats", "banned", ips)
				}
			}
		}
	}
}

func localHashCompletionCheck(ctx context.Context, t *torrent.Torrent, fileInfo snaptype.FileInfo, statusChan chan downloadStatus) ([]byte, bool) {
	localHash, err := fileHashBytes(ctx, fileInfo)

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

		logger.Warn("Webseed hash mismatch for torrent", "name", name, "hash", torrentHash.HexString(), "webseeds", webseeds)
	}
}

func (d *Downloader) checkComplete(name string) (bool, int64, *time.Time) {
	if info, err := d.torrentInfo(name); err == nil {
		if info.Completed != nil && info.Completed.Before(time.Now()) {
			if info.Length != nil {
				if fi, err := os.Stat(filepath.Join(d.SnapDir(), name)); err == nil {
					return fi.Size() == *info.Length && fi.ModTime().Equal(*info.Completed), *info.Length, info.Completed
				}
			}
		}
	}

	return false, 0, nil
}

func (d *Downloader) getWebDownloadInfo(t *torrent.Torrent) (webDownloadInfo, []*seedHash, error) {
	torrentHash := t.InfoHash()

	d.lock.RLock()
	info, ok := d.webDownloadInfo[t.Name()]
	d.lock.RUnlock()

	if ok {
		return info, nil, nil
	}

	seedHashMismatches := make([]*seedHash, 0, len(d.cfg.WebSeedUrls))

	for _, webseed := range d.cfg.WebSeedUrls {
		downloadUrl := webseed.JoinPath(t.Name())

		if headRequest, err := http.NewRequestWithContext(d.ctx, "HEAD", downloadUrl.String(), nil); err == nil {
			headResponse, err := http.DefaultClient.Do(headRequest)

			if err != nil {
				continue
			}

			headResponse.Body.Close()

			if headResponse.StatusCode == http.StatusOK {
				if meta, err := getWebpeerTorrentInfo(d.ctx, downloadUrl); err == nil {
					if bytes.Equal(torrentHash.Bytes(), meta.HashInfoBytes().Bytes()) {
						// TODO check the torrent's hash matches this hash
						return webDownloadInfo{
							url:     downloadUrl,
							length:  headResponse.ContentLength,
							torrent: t,
						}, seedHashMismatches, nil
					} else {
						hash := meta.HashInfoBytes()
						seedHashMismatches = append(seedHashMismatches, &seedHash{url: webseed, hash: &hash})
						continue
					}
				}
			}
		}

		seedHashMismatches = append(seedHashMismatches, &seedHash{url: webseed})
	}

	return webDownloadInfo{}, seedHashMismatches, fmt.Errorf("can't find download info")
}

func getWebpeerTorrentInfo(ctx context.Context, downloadUrl *url.URL) (*metainfo.MetaInfo, error) {
	torrentRequest, err := http.NewRequestWithContext(ctx, "GET", downloadUrl.String()+".torrent", nil)

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

func (d *Downloader) torrentDownload(t *torrent.Torrent, statusChan chan downloadStatus, sem *semaphore.Weighted) {

	d.lock.Lock()
	d.downloading[t.Name()] = struct{}{}
	d.lock.Unlock()

	if err := sem.Acquire(d.ctx, 1); err != nil {
		d.logger.Warn("Failed to acquire download semaphore", "err", err)
		return
	}

	d.wg.Add(1)

	go func(t *torrent.Torrent) {
		defer d.wg.Done()
		defer sem.Release(1)

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
				return
			case <-time.After(10 * time.Second):
				bytesRead := t.Stats().BytesReadData

				if lastRead-bytesRead.Int64() == 0 {
					idleCount++
				} else {
					lastRead = bytesRead.Int64()
					idleCount = 0
				}

				if idleCount > 6 {
					t.DisallowDataDownload()
					return
				}
			}
		}
	}(t)
}

func (d *Downloader) webDownload(peerUrls []*url.URL, t *torrent.Torrent, i *webDownloadInfo, statusChan chan downloadStatus, sem *semaphore.Weighted) (*RCloneSession, error) {
	peerUrl, err := selectDownloadPeer(d.ctx, peerUrls, t)

	if err != nil {
		return nil, err
	}

	peerUrl = strings.TrimSuffix(peerUrl, "/")

	session, ok := d.webDownloadSessions[peerUrl]

	if !ok {
		var err error
		session, err = d.webDownloadClient.NewSession(d.ctx, d.SnapDir(), peerUrl)

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

	info, _, _ := snaptype.ParseFileName(d.SnapDir(), name)

	d.lock.Lock()
	t.Drop()
	d.downloading[name] = struct{}{}
	d.lock.Unlock()

	d.wg.Add(1)

	if err := sem.Acquire(d.ctx, 1); err != nil {
		d.logger.Warn("Failed to acquire download semaphore", "err", err)
		return nil, err
	}

	go func() {
		defer d.wg.Done()
		defer sem.Release(1)

		if dir.FileExist(info.Path) {
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

		err := session.Download(d.ctx, name)

		if err != nil {
			d.logger.Error("Web download failed", "file", name, "err", err)
		}

		localHash, err := fileHashBytes(d.ctx, info)

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
		return "", fmt.Errorf("no download peers")

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

	return "", fmt.Errorf("can't find download peer")
}

func availableTorrents(ctx context.Context, pending []*torrent.Torrent, slots int) []*torrent.Torrent {
	if slots == 0 {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(10 * time.Second):
			return nil
		}
	}

	slices.SortFunc(pending, func(i, j *torrent.Torrent) int {
		in, _, _ := snaptype.ParseFileName("", i.Name())
		jn, _, _ := snaptype.ParseFileName("", j.Name())
		return in.CompareTo(jn)
	})

	var available []*torrent.Torrent

	for len(pending) > 0 && pending[0].Info() != nil {
		available = append(available, pending[0])

		if len(available) == slots {
			return available
		}

		pending = pending[1:]
	}

	if len(pending) == 0 {
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

			if len(available) == slots {
				return available
			}

			pending = append(pending[:selected], pending[selected+1:]...)
			cases = append(cases[:selected], cases[selected+1:]...)
		}
	}
}

func (d *Downloader) SnapDir() string { return d.cfg.Dirs.Snap }

func (d *Downloader) torrentInfo(name string) (*torrentInfo, error) {
	var info torrentInfo

	err := d.db.View(d.ctx, func(tx kv.Tx) (err error) {
		infoBytes, err := tx.GetOne(kv.BittorrentInfo, []byte(name))

		if err != nil {
			return err
		}

		if err = json.Unmarshal(infoBytes, &info); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &info, nil
}

func (d *Downloader) ReCalcStats(interval time.Duration) {
	d.lock.Lock()
	defer d.lock.Unlock()
	//Call this methods outside of `lock` critical section, because they have own locks with contention
	torrents := d.torrentClient.Torrents()
	connStats := d.torrentClient.ConnStats()
	peers := make(map[torrent.PeerID]struct{}, 16)

	prevStats, stats := d.stats, d.stats

	stats.Completed = true
	stats.BytesDownload = uint64(connStats.BytesReadUsefulIntendedData.Int64())
	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())

	lastMetadataReady := stats.MetadataReady

	stats.BytesTotal, stats.BytesCompleted, stats.ConnectionsTotal, stats.MetadataReady =
		atomic.LoadUint64(&stats.DroppedTotal), atomic.LoadUint64(&stats.DroppedCompleted), 0, 0

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

	downloading := map[string]struct{}{}

	for file := range d.downloading {
		downloading[file] = struct{}{}
	}

	var dbInfo int
	var dbComplete int
	var tComplete int
	var torrentInfo int

	for _, t := range torrents {
		var torrentComplete bool
		torrentName := t.Name()

		if _, ok := downloading[torrentName]; ok {
			torrentComplete = t.Complete.Bool()
		}

		var progress float32

		if t.Info() != nil {
			torrentInfo++
			stats.MetadataReady++

			// call methods once - to reduce internal mutex contention
			peersOfThisFile := t.PeerConns()
			weebseedPeersOfThisFile := t.WebseedPeerConns()

			bytesRead := t.Stats().BytesReadData
			tLen := t.Length()

			var bytesCompleted int64

			if torrentComplete {
				tComplete++
				bytesCompleted = t.Length()
			} else {
				bytesCompleted = bytesRead.Int64()
			}

			delete(downloading, torrentName)

			for _, peer := range peersOfThisFile {
				stats.ConnectionsTotal++
				peers[peer.PeerID] = struct{}{}
			}

			stats.BytesCompleted += uint64(bytesCompleted)
			stats.BytesTotal += uint64(tLen)

			progress = float32(float64(100) * (float64(bytesCompleted) / float64(tLen)))

			webseedRates, webseeds := getWebseedsRatesForlogs(weebseedPeersOfThisFile, torrentName, t.Complete.Bool())
			rates, peers := getPeersRatesForlogs(peersOfThisFile, torrentName)
			// more detailed statistic: download rate of each peer (for each file)
			if !torrentComplete && progress != 0 {
				d.logger.Log(d.verbosity, "[snapshots] progress", "file", torrentName, "progress", fmt.Sprintf("%.2f%%", progress), "peers", len(peersOfThisFile), "webseeds", len(weebseedPeersOfThisFile))
				d.logger.Log(d.verbosity, "[snapshots] webseed peers", webseedRates...)
				d.logger.Log(d.verbosity, "[snapshots] bittorrent peers", rates...)
			}

			diagnostics.Send(diagnostics.SegmentDownloadStatistics{
				Name:            torrentName,
				TotalBytes:      uint64(tLen),
				DownloadedBytes: uint64(bytesCompleted),
				Webseeds:        webseeds,
				Peers:           peers,
			})
		}

		if !torrentComplete {
			if info, err := d.torrentInfo(torrentName); err == nil {
				updateStats := t.Info() == nil

				if updateStats {
					dbInfo++
				}

				if info.Completed != nil && info.Completed.Before(time.Now()) {
					if info.Length != nil {
						if updateStats {
							stats.MetadataReady++
							stats.BytesTotal += uint64(*info.Length)
						}

						if fi, err := os.Stat(filepath.Join(d.SnapDir(), t.Name())); err == nil {
							if torrentComplete = (fi.Size() == *info.Length); torrentComplete {
								infoRead := t.Stats().BytesReadData
								if updateStats || infoRead.Int64() == 0 {
									stats.BytesCompleted += uint64(*info.Length)
								}
								dbComplete++
								progress = float32(100)
							}
						}
					}
				}
			} else if _, ok := d.webDownloadInfo[torrentName]; ok {
				stats.MetadataReady++
			} else {
				noMetadata = append(noMetadata, torrentName)
			}

			if progress == 0 {
				zeroProgress = append(zeroProgress, torrentName)
			}
		}

		stats.Completed = stats.Completed && torrentComplete
	}

	var webTransfers int32

	if d.webDownloadClient != nil {
		webStats, _ := d.webDownloadClient.Stats(d.ctx)

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

				stats.BytesCompleted += bytesCompleted
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
					d.logger.Log(d.verbosity, "[snapshots] progress", "file", transferName, "progress", fmt.Sprintf("%.2f%%", float32(transfer.Percentage)), "webseeds", 1)
					d.logger.Log(d.verbosity, "[snapshots] web peers", webseedRates...)
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
		webTransfers += int32(len(downloading))
		stats.Completed = false
	}

	d.logger.Debug("[snapshots] info", "len", len(torrents), "webTransfers", webTransfers, "torrent", torrentInfo, "db", dbInfo, "t-complete", tComplete, "db-complete", dbComplete)

	if lastMetadataReady != stats.MetadataReady {
		now := time.Now()
		stats.LastMetadataUpdate = &now
	}

	if len(noMetadata) > 0 {
		amount := len(noMetadata)
		if len(noMetadata) > 5 {
			noMetadata = append(noMetadata[:5], "...")
		}
		d.logger.Info("[snapshots] no metadata yet", "files", amount, "list", strings.Join(noMetadata, ","))
	}

	if len(zeroProgress) > 0 {
		amount := len(zeroProgress)
		if len(zeroProgress) > 5 {
			zeroProgress = append(zeroProgress[:5], "...")
		}
		d.logger.Info("[snapshots] no progress yet", "files", amount, "list", strings.Join(zeroProgress, ","))
	}

	if len(d.downloading) > 0 {
		amount := len(d.downloading)

		files := make([]string, 0, len(downloading))

		for file := range d.downloading {
			files = append(files, file)
		}

		d.logger.Log(d.verbosity, "[snapshots] downloading", "files", amount, "list", strings.Join(files, ","))
	}

	if stats.BytesDownload > prevStats.BytesDownload {
		stats.DownloadRate = (stats.BytesDownload - prevStats.BytesDownload) / uint64(interval.Seconds())
	} else {
		stats.DownloadRate = prevStats.DownloadRate / 2
	}

	if stats.BytesUpload > prevStats.BytesUpload {
		stats.UploadRate = (stats.BytesUpload - prevStats.BytesUpload) / uint64(interval.Seconds())
	} else {
		stats.UploadRate = prevStats.UploadRate / 2
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

	d.stats = stats
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
		rates = append(rates, peer.PeerClientName.Load(), fmt.Sprintf("%s/s", common.ByteCount(dr)))
	}

	return rates, peers
}

func (d *Downloader) VerifyData(ctx context.Context, whiteList []string, failFast bool) error {
	total := 0
	allTorrents := d.torrentClient.Torrents()
	toVerify := make([]*torrent.Torrent, 0, len(allTorrents))
	for _, t := range allTorrents {
		select {
		case <-t.GotInfo():
		case <-ctx.Done():
			return ctx.Err()
		}

		if !dir2.FileExist(filepath.Join(d.SnapDir(), t.Name())) {
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
			return ScheduleVerifyFile(ctx, t, completedPieces)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	// force fsync of db. to not loose results of validation on power-off
	return d.db.Update(context.Background(), func(tx kv.RwTx) error { return nil })
}

// AddNewSeedableFile decides what we do depending on wether we have the .seg file or the .torrent file
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
	err := BuildTorrentIfNeed(ctx, name, d.SnapDir(), d.torrentFiles)
	if err != nil {
		return fmt.Errorf("AddNewSeedableFile: %w", err)
	}
	ts, err := d.torrentFiles.LoadByName(name)
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
	return d.addMagnetLink(ctx, infoHash, name, false)
}

func (d *Downloader) addMagnetLink(ctx context.Context, infoHash metainfo.Hash, name string, force bool) error {
	// Paranoic Mode on: if same file changed infoHash - skip it
	// Example:
	//  - Erigon generated file X with hash H1. User upgraded Erigon. New version has preverified file X with hash H2. Must ignore H2 (don't send to Downloader)
	if d.alreadyHaveThisName(name) || !IsSnapNameAllowed(name) {
		return nil
	}

	if !force && d.torrentFiles.newDownloadsAreProhibited() {
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
		}

		if !d.snapshotLock.Downloads.Contains(name) {
			mi := t.Metainfo()
			if err := CreateTorrentFileIfNotExists(d.SnapDir(), t.Info(), &mi, d.torrentFiles); err != nil {
				d.logger.Warn("[snapshots] create torrent file", "err", err)
				return
			}
		}

		urls, ok := d.webseeds.ByFileName(t.Name())
		if ok {
			t.AddWebSeeds(urls)
		}
	}(t)
	//log.Debug("[downloader] downloaded both seg and torrent files", "hash", infoHash)
	return nil
}

func seedableFiles(dirs datadir.Dirs, chainName string) ([]string, error) {
	files, err := seedableSegmentFiles(dirs.Snap, chainName)
	if err != nil {
		return nil, fmt.Errorf("seedableSegmentFiles: %w", err)
	}
	l1, err := seedableStateFilesBySubDir(dirs.Snap, "idx")
	if err != nil {
		return nil, err
	}
	l2, err := seedableStateFilesBySubDir(dirs.Snap, "history")
	if err != nil {
		return nil, err
	}
	l3, err := seedableStateFilesBySubDir(dirs.Snap, "domain")
	if err != nil {
		return nil, err
	}
	files = append(append(append(files, l1...), l2...), l3...)
	return files, nil
}

func (d *Downloader) addTorrentFilesFromDisk(quiet bool) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	files, err := AllTorrentSpecs(d.cfg.Dirs, d.torrentFiles)
	if err != nil {
		return err
	}
	for i, ts := range files {
		d.lock.RLock()
		_, downloading := d.downloading[ts.DisplayName]
		d.lock.RUnlock()

		if downloading {
			continue
		}

		if info, err := d.torrentInfo(ts.DisplayName); err == nil {
			if info.Completed != nil {
				_, serr := os.Stat(filepath.Join(d.SnapDir(), info.Name))

				if serr != nil {
					if err := d.db.Update(d.ctx, func(tx kv.RwTx) error {
						return tx.Delete(kv.BittorrentInfo, []byte(info.Name))
					}); err != nil {
						log.Error("[snapshots] Failed to delete db entry after stat error", "file", info.Name, "err", err, "stat-err", serr)
					}
				}
			}
		}

		if whitelisted, ok := d.webseeds.torrentsWhitelist.Get(ts.DisplayName); ok {
			if ts.InfoHash.HexString() != whitelisted.Hash {
				continue
			}
		}
		_, _, err := addTorrentFile(d.ctx, ts, d.torrentClient, d.db, d.webseeds)

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
	}
	return nil
}
func (d *Downloader) BuildTorrentFilesIfNeed(ctx context.Context, chain string, ignore snapcfg.Preverified) error {
	return BuildTorrentFilesIfNeed(ctx, d.cfg.Dirs, d.torrentFiles, chain, ignore)
}
func (d *Downloader) Stats() AggStats {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.stats
}

func (d *Downloader) Close() {
	d.logger.Debug("[snapshots] stopping downloader")
	d.stopMainLoop()
	d.wg.Wait()
	d.logger.Debug("[snapshots] closing torrents")
	d.torrentClient.Close()
	if err := d.folder.Close(); err != nil {
		d.logger.Warn("[snapshots] folder.close", "err", err)
	}
	if err := d.pieceCompletionDB.Close(); err != nil {
		d.logger.Warn("[snapshots] pieceCompletionDB.close", "err", err)
	}
	d.logger.Debug("[snapshots] closing db")
	d.db.Close()
	d.logger.Debug("[snapshots] downloader stopped")
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

func openClient(ctx context.Context, dbDir, snapDir string, cfg *torrent.ClientConfig) (db kv.RwDB, c storage.PieceCompletion, m storage.ClientImplCloser, torrentClient *torrent.Client, err error) {
	db, err = mdbx.NewMDBX(log.New()).
		Label(kv.DownloaderDB).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).
		GrowthStep(16 * datasize.MB).
		MapSize(16 * datasize.GB).
		PageSize(uint64(8 * datasize.KB)).
		Path(dbDir).
		Open(ctx)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrentcfg.openClient: %w", err)
	}
	c, err = NewMdbxPieceCompletion(db)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrentcfg.NewMdbxPieceCompletion: %w", err)
	}
	m = storage.NewMMapWithCompletion(snapDir, c)
	cfg.DefaultStorage = m

	torrentClient, err = torrent.NewClient(cfg)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrent.NewClient: %w", err)
	}

	return db, c, m, torrentClient, nil
}
