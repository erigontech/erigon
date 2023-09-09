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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Downloader - component which downloading historical files. Can use BitTorrent, or other protocols
type Downloader struct {
	db                kv.RwDB
	pieceCompletionDB storage.PieceCompletion
	torrentClient     *torrent.Client
	clientLock        *sync.RWMutex

	cfg *downloadercfg.Cfg

	statsLock *sync.RWMutex
	stats     AggStats

	folder       storage.ClientImplCloser
	stopMainLoop context.CancelFunc
	wg           sync.WaitGroup
}

type AggStats struct {
	MetadataReady, FilesTotal int32
	PeersUnique               int32
	ConnectionsTotal          uint64

	Completed bool
	Progress  float32

	BytesCompleted, BytesTotal     uint64
	DroppedCompleted, DroppedTotal atomic.Uint64

	BytesDownload, BytesUpload uint64
	UploadRate, DownloadRate   uint64
}

func New(ctx context.Context, cfg *downloadercfg.Cfg) (*Downloader, error) {
	if err := portMustBeTCPAndUDPOpen(cfg.ListenPort); err != nil {
		return nil, err
	}

	// Application must never see partially-downloaded files
	// To provide such consistent view - downloader does:
	// add <datadir>/snapshots/tmp - then method .onComplete will remove this suffix
	// and App only work with <datadir>/snapshot s folder
	if dir.FileExist(cfg.DataDir + "_tmp") { // migration from prev versions
		_ = os.Rename(cfg.DataDir+"_tmp", filepath.Join(cfg.DataDir, "tmp")) // ignore error, because maybe they are on different drive, or target folder already created manually, all is fine
	}
	if err := moveFromTmp(cfg.DataDir); err != nil {
		return nil, err
	}

	db, c, m, torrentClient, err := openClient(cfg.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("openClient: %w", err)
	}

	peerID, err := readPeerID(db)
	if err != nil {
		return nil, fmt.Errorf("get peer id: %w", err)
	}
	cfg.PeerID = string(peerID)
	if len(peerID) == 0 {
		if err = savePeerID(db, torrentClient.PeerID()); err != nil {
			return nil, fmt.Errorf("save peer id: %w", err)
		}
	}

	d := &Downloader{
		cfg:               cfg,
		db:                db,
		pieceCompletionDB: c,
		folder:            m,
		torrentClient:     torrentClient,
		clientLock:        &sync.RWMutex{},

		statsLock: &sync.RWMutex{},
	}
	if err := d.addSegments(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Downloader) MainLoopInBackground(ctx context.Context, silent bool) {
	ctx, d.stopMainLoop = context.WithCancel(ctx)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := d.mainLoop(ctx, silent); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Warn("[snapshots]", "err", err)
			}
		}
	}()
}

func (d *Downloader) mainLoop(ctx context.Context, silent bool) error {
	var sem = semaphore.NewWeighted(int64(d.cfg.DownloadSlots))

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		// Torrents that are already taken care of
		torrentMap := map[metainfo.Hash]struct{}{}
		// First loop drops torrents that were downloaded or are already complete
		// This improves efficiency of download by reducing number of active torrent (empirical observation)
	DownloadLoop:
		torrents := d.Torrent().Torrents()
		for _, t := range torrents {
			if _, already := torrentMap[t.InfoHash()]; already {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-t.GotInfo():
			}
			if t.Complete.Bool() {
				d.stats.DroppedCompleted.Add(uint64(t.BytesCompleted()))
				d.stats.DroppedTotal.Add(uint64(t.Length()))
				//t.Drop()
				torrentMap[t.InfoHash()] = struct{}{}
				continue
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			t.AllowDataDownload()
			t.DownloadAll()
			torrentMap[t.InfoHash()] = struct{}{}
			d.wg.Add(1)
			go func(t *torrent.Torrent) {
				defer d.wg.Done()
				defer sem.Release(1)
				select {
				case <-ctx.Done():
					return
				case <-t.Complete.On():
				}
				d.stats.DroppedCompleted.Add(uint64(t.BytesCompleted()))
				d.stats.DroppedTotal.Add(uint64(t.Length()))
				//t.Drop()
			}(t)
		}
		if len(torrents) != len(d.Torrent().Torrents()) { //if amount of torrents changed - keep downloading
			goto DownloadLoop
		}

		if err := d.addSegments(ctx); err != nil {
			return
		}
	DownloadLoop2:
		torrents = d.Torrent().Torrents()
		for _, t := range torrents {
			if _, already := torrentMap[t.InfoHash()]; already {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-t.GotInfo():
			}
			if t.Complete.Bool() {
				d.stats.DroppedCompleted.Add(uint64(t.BytesCompleted()))
				d.stats.DroppedTotal.Add(uint64(t.Length()))
				//t.Drop()
				torrentMap[t.InfoHash()] = struct{}{}
				continue
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			t.AllowDataDownload()
			t.DownloadAll()
			torrentMap[t.InfoHash()] = struct{}{}
			d.wg.Add(1)
			go func(t *torrent.Torrent) {
				defer d.wg.Done()
				defer sem.Release(1)
				select {
				case <-ctx.Done():
					return
				case <-t.Complete.On():
				}
				d.stats.DroppedCompleted.Add(uint64(t.BytesCompleted()))
				d.stats.DroppedTotal.Add(uint64(t.Length()))
				//t.Drop()
			}(t)
		}
		if len(torrents) != len(d.Torrent().Torrents()) { //if amount of torrents changed - keep downloading
			goto DownloadLoop2
		}
	}()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	statInterval := 20 * time.Second
	statEvery := time.NewTicker(statInterval)
	defer statEvery.Stop()

	justCompleted := true
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-statEvery.C:
			d.ReCalcStats(statInterval)

		case <-logEvery.C:
			if silent {
				continue
			}

			stats := d.Stats()

			if stats.Completed {
				if justCompleted {
					justCompleted = false
					// force fsync of db. to not loose results of downloading on power-off
					_ = d.db.Update(ctx, func(tx kv.RwTx) error { return nil })
				}

				log.Info("[snapshots] Seeding",
					"up", common2.ByteCount(stats.UploadRate)+"/s",
					"peers", stats.PeersUnique,
					"conns", stats.ConnectionsTotal,
					"files", stats.FilesTotal)
				continue
			}

			log.Info("[snapshots] Downloading",
				"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, common2.ByteCount(stats.BytesCompleted), common2.ByteCount(stats.BytesTotal)),
				"download", common2.ByteCount(stats.DownloadRate)+"/s",
				"upload", common2.ByteCount(stats.UploadRate)+"/s",
				"peers", stats.PeersUnique,
				"conns", stats.ConnectionsTotal,
				"files", stats.FilesTotal)

			if stats.PeersUnique == 0 {
				ips := d.Torrent().BadPeerIPs()
				if len(ips) > 0 {
					log.Info("[snapshots] Stats", "banned", ips)
				}
			}
		}
	}
}

func (d *Downloader) SnapDir() string {
	d.clientLock.RLock()
	defer d.clientLock.RUnlock()
	return d.cfg.DataDir
}

func (d *Downloader) ReCalcStats(interval time.Duration) {
	//Call this methods outside of `statsLock` critical section, because they have own locks with contention
	torrents := d.torrentClient.Torrents()
	connStats := d.torrentClient.ConnStats()
	peers := make(map[torrent.PeerID]struct{}, 16)

	d.statsLock.Lock()
	defer d.statsLock.Unlock()
	prevStats, stats := d.stats, d.stats

	stats.Completed = true
	stats.BytesDownload = uint64(connStats.BytesReadUsefulIntendedData.Int64())
	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())

	stats.BytesTotal, stats.BytesCompleted, stats.ConnectionsTotal, stats.MetadataReady = stats.DroppedTotal.Load(), stats.DroppedCompleted.Load(), 0, 0
	for _, t := range torrents {
		select {
		case <-t.GotInfo():
			stats.MetadataReady++
			for _, peer := range t.PeerConns() {
				stats.ConnectionsTotal++
				peers[peer.PeerID] = struct{}{}
			}
			stats.BytesCompleted += uint64(t.BytesCompleted())
			stats.BytesTotal += uint64(t.Length())
			if !t.Complete.Bool() {
				progress := float32(float64(100) * (float64(t.BytesCompleted()) / float64(t.Length())))
				log.Debug("[downloader] file not downloaded yet", "name", t.Name(), "progress", fmt.Sprintf("%.2f%%", progress))
			}
		default:
			log.Debug("[downloader] file has no metadata yet", "name", t.Name())
		}

		stats.Completed = stats.Completed && t.Complete.Bool()
	}

	stats.DownloadRate = (stats.BytesDownload - prevStats.BytesDownload) / uint64(interval.Seconds())
	stats.UploadRate = (stats.BytesUpload - prevStats.BytesUpload) / uint64(interval.Seconds())

	if stats.BytesTotal == 0 {
		stats.Progress = 0
	} else {
		stats.Progress = float32(float64(100) * (float64(stats.BytesCompleted) / float64(stats.BytesTotal)))
		if stats.Progress == 100 && !stats.Completed {
			stats.Progress = 99.99
		}
	}
	stats.PeersUnique = int32(len(peers))
	stats.FilesTotal = int32(len(torrents))

	d.stats = stats
}

func moveFromTmp(snapDir string) error {
	tmpDir := filepath.Join(snapDir, "tmp")
	if !dir.FileExist(tmpDir) {
		return nil
	}

	snFs := os.DirFS(tmpDir)
	paths, err := fs.ReadDir(snFs, ".")
	if err != nil {
		return err
	}
	for _, p := range paths {
		if p.Name() == "." || p.Name() == ".." || p.Name() == "tmp" {
			continue
		}
		src := filepath.Join(tmpDir, p.Name())
		if err := os.Rename(src, filepath.Join(snapDir, p.Name())); err != nil {
			if os.IsExist(err) {
				_ = os.Remove(src)
				continue
			}
			return err
		}
	}
	_ = os.Remove(tmpDir)
	return nil
}

func (d *Downloader) verifyFile(ctx context.Context, t *torrent.Torrent, completePieces *atomic.Uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.GotInfo():
	}

	g := &errgroup.Group{}
	for i := 0; i < t.NumPieces(); i++ {
		i := i
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			t.Piece(i).VerifyData()
			completePieces.Add(1)
			return nil
		})
		//<-t.Complete.On()
	}
	return g.Wait()
}

func (d *Downloader) VerifyData(ctx context.Context) error {
	total := 0
	for _, t := range d.torrentClient.Torrents() {
		select {
		case <-t.GotInfo():
			total += t.NumPieces()
		default:
			continue
		}
	}

	completedPieces := &atomic.Uint64{}

	{
		log.Info("[snapshots] Verify start")
		defer log.Info("[snapshots] Verify done")
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		logInterval := 20 * time.Second
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-logEvery.C:
					log.Info("[snapshots] Verify", "progress", fmt.Sprintf("%.2f%%", 100*float64(completedPieces.Load())/float64(total)))
				}
			}
		}()
	}

	g, ctx := errgroup.WithContext(ctx)
	// torrent lib internally limiting amount of hashers per file
	// set limit here just to make load predictable, not to control Disk/CPU consumption
	g.SetLimit(runtime.GOMAXPROCS(-1) * 2)

	for _, t := range d.torrentClient.Torrents() {
		t := t
		g.Go(func() error {
			return d.verifyFile(ctx, t, completedPieces)
		})
	}

	g.Wait()
	// force fsync of db. to not loose results of validation on power-off
	return d.db.Update(context.Background(), func(tx kv.RwTx) error { return nil })
}

func (d *Downloader) createMagnetLinkWithInfoHash(ctx context.Context, hash *prototypes.H160, snapDir string) (bool, error) {
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	if hash == nil {
		return false, nil
	}
	infoHash := Proto2InfoHash(hash)
	//log.Debug("[downloader] downloading torrent and seg file", "hash", infoHash)

	if _, ok := d.torrentClient.Torrent(infoHash); ok {
		//log.Debug("[downloader] torrent client related to hash found", "hash", infoHash)
		return true, nil
	}

	magnet := mi.Magnet(&infoHash, nil)
	t, err := d.torrentClient.AddMagnet(magnet.String())
	if err != nil {
		//log.Warn("[downloader] add magnet link", "err", err)
		return false, err
	}
	t.DisallowDataDownload()
	t.AllowDataUpload()
	d.wg.Add(1)
	go func(t *torrent.Torrent) {
		defer d.wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-t.GotInfo():
		}

		mi := t.Metainfo()
		if err := CreateTorrentFileIfNotExists(snapDir, t.Info(), &mi); err != nil {
			log.Warn("[downloader] create torrent file", "err", err)
			return
		}
	}(t)
	//log.Debug("[downloader] downloaded both seg and torrent files", "hash", infoHash)
	return false, nil
}

func (d *Downloader) addSegments(ctx context.Context) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	_, err := BuildTorrentFilesIfNeed(context.Background(), d.SnapDir())
	if err != nil {
		return err
	}
	files, err := seedableSegmentFiles(d.SnapDir())
	if err != nil {
		return fmt.Errorf("seedableSegmentFiles: %w", err)
	}
	files2, err := seedableHistorySnapshots(d.SnapDir())
	if err != nil {
		return fmt.Errorf("seedableHistorySnapshots: %w", err)
	}
	files = append(files, files2...)

	g, ctx := errgroup.WithContext(ctx)
	i := atomic.Int64{}
	for _, f := range files {
		f := f
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			_, err := AddSegment(f, d.cfg.DataDir, d.torrentClient)
			if err != nil {
				return err
			}

			i.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info("[snpshots] initializing", "files", fmt.Sprintf("%d/%d", i.Load(), len(files)))
			default:
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (d *Downloader) Stats() AggStats {
	d.statsLock.RLock()
	defer d.statsLock.RUnlock()
	return d.stats
}

func (d *Downloader) Close() {
	d.stopMainLoop()
	d.wg.Wait()
	d.torrentClient.Close()
	if err := d.folder.Close(); err != nil {
		log.Warn("[snapshots] folder.close", "err", err)
	}
	if err := d.pieceCompletionDB.Close(); err != nil {
		log.Warn("[snapshots] pieceCompletionDB.close", "err", err)
	}
	d.db.Close()
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

func (d *Downloader) Torrent() *torrent.Client {
	d.clientLock.RLock()
	defer d.clientLock.RUnlock()
	return d.torrentClient
}

func openClient(cfg *torrent.ClientConfig) (db kv.RwDB, c storage.PieceCompletion, m storage.ClientImplCloser, torrentClient *torrent.Client, err error) {
	snapDir := cfg.DataDir
	db, err = mdbx.NewMDBX(log.New()).
		Label(kv.DownloaderDB).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).
		SyncPeriod(15 * time.Second).
		Path(filepath.Join(snapDir, "db")).
		Open()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	c, err = NewMdbxPieceCompletion(db)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrentcfg.NewMdbxPieceCompletion: %w", err)
	}
	m = storage.NewMMapWithCompletion(snapDir, c)
	cfg.DefaultStorage = m

	for retry := 0; retry < 5; retry++ {
		torrentClient, err = torrent.NewClient(cfg)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrent.NewClient: %w", err)
	}

	return db, c, m, torrentClient, nil
}
