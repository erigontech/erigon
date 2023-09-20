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
	"net/http"
	"net/url"
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
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Downloader - component which downloading historical files. Can use BitTorrent, or other protocols
type Downloader struct {
	db                kv.RwDB
	pieceCompletionDB storage.PieceCompletion
	torrentClient     *torrent.Client

	cfg *downloadercfg.Cfg

	statsLock *sync.RWMutex
	stats     AggStats

	folder storage.ClientImplCloser

	ctx          context.Context
	stopMainLoop context.CancelFunc
	wg           sync.WaitGroup

	webseeds *WebSeeds
}

type AggStats struct {
	MetadataReady, FilesTotal int32
	PeersUnique               int32
	ConnectionsTotal          uint64

	Completed bool
	Progress  float32

	BytesCompleted, BytesTotal     uint64
	DroppedCompleted, DroppedTotal uint64

	BytesDownload, BytesUpload uint64
	UploadRate, DownloadRate   uint64
}

func New(ctx context.Context, cfg *downloadercfg.Cfg) (*Downloader, error) {
	if err := portMustBeTCPAndUDPOpen(cfg.ClientConfig.ListenPort); err != nil {
		return nil, err
	}

	// Application must never see partially-downloaded files
	// To provide such consistent view - downloader does:
	// add <datadir>/snapshots/tmp - then method .onComplete will remove this suffix
	// and App only work with <datadir>/snapshot s folder
	if dir.FileExist(cfg.SnapDir + "_tmp") { // migration from prev versions
		_ = os.Rename(cfg.SnapDir+"_tmp", filepath.Join(cfg.SnapDir, "tmp")) // ignore error, because maybe they are on different drive, or target folder already created manually, all is fine
	}
	if err := moveFromTmp(cfg.SnapDir); err != nil {
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
	cfg.ClientConfig.PeerID = string(peerID)
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
		statsLock:         &sync.RWMutex{},
		webseeds:          &WebSeeds{},
	}
	d.ctx, d.stopMainLoop = context.WithCancel(ctx)

	if err := d.addSegments(d.ctx); err != nil {
		return nil, err
	}
	// CornerCase: no peers -> no anoncments to trackers -> no magnetlink resolution (but magnetlink has filename)
	// means we can start adding weebseeds without waiting for `<-t.GotInfo()`
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.webseeds.Discover(d.ctx, d.cfg.WebSeedUrls, d.cfg.WebSeedFiles)
		d.applyWebseeds()
	}()
	return d, nil
}

func (d *Downloader) MainLoopInBackground(silent bool) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := d.mainLoop(silent); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Warn("[snapshots]", "err", err)
			}
		}
	}()
}

func (d *Downloader) mainLoop(silent bool) error {
	var sem = semaphore.NewWeighted(int64(d.cfg.DownloadSlots))

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		// Torrents that are already taken care of
		torrentMap := map[metainfo.Hash]struct{}{}
		// First loop drops torrents that were downloaded or are already complete
		// This improves efficiency of download by reducing number of active torrent (empirical observation)
		for torrents := d.torrentClient.Torrents(); len(torrents) > 0; torrents = d.torrentClient.Torrents() {
			for _, t := range torrents {
				if _, already := torrentMap[t.InfoHash()]; already {
					continue
				}
				select {
				case <-d.ctx.Done():
					return
				case <-t.GotInfo():
				}
				if t.Complete.Bool() {
					atomic.AddUint64(&d.stats.DroppedCompleted, uint64(t.BytesCompleted()))
					atomic.AddUint64(&d.stats.DroppedTotal, uint64(t.Length()))
					t.Drop()
					torrentMap[t.InfoHash()] = struct{}{}
					continue
				}
				if err := sem.Acquire(d.ctx, 1); err != nil {
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
					case <-d.ctx.Done():
						return
					case <-t.Complete.On():
					}
					atomic.AddUint64(&d.stats.DroppedCompleted, uint64(t.BytesCompleted()))
					atomic.AddUint64(&d.stats.DroppedTotal, uint64(t.Length()))
					t.Drop()
				}(t)
			}
		}
		atomic.StoreUint64(&d.stats.DroppedCompleted, 0)
		atomic.StoreUint64(&d.stats.DroppedTotal, 0)
		d.addSegments(d.ctx)
		maps.Clear(torrentMap)
		for {
			torrents := d.torrentClient.Torrents()
			for _, t := range torrents {
				if _, already := torrentMap[t.InfoHash()]; already {
					continue
				}
				select {
				case <-d.ctx.Done():
					return
				case <-t.GotInfo():
				}
				if t.Complete.Bool() {
					torrentMap[t.InfoHash()] = struct{}{}
					continue
				}
				if err := sem.Acquire(d.ctx, 1); err != nil {
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
					case <-d.ctx.Done():
						return
					case <-t.Complete.On():
					}
				}(t)
			}
			time.Sleep(10 * time.Second)
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
		case <-d.ctx.Done():
			return d.ctx.Err()
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
					_ = d.db.Update(d.ctx, func(tx kv.RwTx) error { return nil })
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
				ips := d.TorrentClient().BadPeerIPs()
				if len(ips) > 0 {
					log.Info("[snapshots] Stats", "banned", ips)
				}
			}
		}
	}
}

func (d *Downloader) SnapDir() string { return d.cfg.SnapDir }

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

	stats.BytesTotal, stats.BytesCompleted, stats.ConnectionsTotal, stats.MetadataReady = atomic.LoadUint64(&stats.DroppedTotal), atomic.LoadUint64(&stats.DroppedCompleted), 0, 0
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
		if p.IsDir() || !p.Type().IsRegular() {
			continue
		}
		if p.Name() == "tmp" {
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
	g.SetLimit(runtime.GOMAXPROCS(-1) * 4)

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

// AddNewSeedableFile decides what we do depending on wether we have the .seg file or the .torrent file
// have .torrent no .seg => get .seg file from .torrent
// have .seg no .torrent => get .torrent from .seg
func (d *Downloader) AddNewSeedableFile(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// if we don't have the torrent file we build it if we have the .seg file
	torrentFilePath, err := BuildTorrentIfNeed(ctx, name, d.SnapDir())
	if err != nil {
		return err
	}
	ts, err := loadTorrent(torrentFilePath)
	if err != nil {
		return err
	}
	_, err = addTorrentFile(ts, d.torrentClient)
	if err != nil {
		return fmt.Errorf("addTorrentFile: %w", err)
	}
	return nil
}

func (d *Downloader) exists(name string) bool {
	// Paranoic Mode on: if same file changed infoHash - skip it
	// use-cases:
	//	- release of re-compressed version of same file,
	//	- ErigonV1.24 produced file X, then ErigonV1.25 released with new compression algorithm and produced X with anouther infoHash.
	//		ErigonV1.24 node must keep using existing file instead of downloading new one.
	for _, t := range d.torrentClient.Torrents() {
		if t.Name() == name {
			return true
		}
	}
	return false
}
func (d *Downloader) AddInfoHashAsMagnetLink(ctx context.Context, infoHash metainfo.Hash, name string) error {
	if d.exists(name) {
		return nil
	}
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}

	magnet := mi.Magnet(&infoHash, &metainfo.Info{Name: name})
	t, err := d.torrentClient.AddMagnet(magnet.String())
	if err != nil {
		//log.Warn("[downloader] add magnet link", "err", err)
		return err
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
		if err := CreateTorrentFileIfNotExists(d.SnapDir(), t.Info(), &mi); err != nil {
			log.Warn("[downloader] create torrent file", "err", err)
			return
		}
	}(t)
	//log.Debug("[downloader] downloaded both seg and torrent files", "hash", infoHash)
	return nil
}

func seedableFiles(snapDir string) ([]string, error) {
	files, err := seedableSegmentFiles(snapDir)
	if err != nil {
		return nil, fmt.Errorf("seedableSegmentFiles: %w", err)
	}
	files2, err := seedableHistorySnapshots(snapDir, "history")
	if err != nil {
		return nil, fmt.Errorf("seedableHistorySnapshots: %w", err)
	}
	files = append(files, files2...)
	files2, err = seedableHistorySnapshots(snapDir, "warm")
	if err != nil {
		return nil, fmt.Errorf("seedableHistorySnapshots: %w", err)
	}
	files = append(files, files2...)
	return files, nil
}
func (d *Downloader) addSegments(ctx context.Context) error {
	_, err := BuildTorrentFilesIfNeed(ctx, d.SnapDir())
	if err != nil {
		return err
	}
	return AddTorrentFiles(d.SnapDir(), d.torrentClient)
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

func (d *Downloader) TorrentClient() *torrent.Client { return d.torrentClient }

func openClient(cfg *torrent.ClientConfig) (db kv.RwDB, c storage.PieceCompletion, m storage.ClientImplCloser, torrentClient *torrent.Client, err error) {
	snapDir := cfg.DataDir
	db, err = mdbx.NewMDBX(log.New()).
		Label(kv.DownloaderDB).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).
		SyncPeriod(15 * time.Second).
		Path(filepath.Join(snapDir, "db")).
		Open()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("torrentcfg.openClient: %w", err)
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

func (d *Downloader) applyWebseeds() {
	for _, t := range d.TorrentClient().Torrents() {
		urls, ok := d.webseeds.GetByFileNames()[t.Name()]
		if !ok {
			continue
		}
		log.Debug("[downloader] addd webseeds", "file", t.Name())
		t.AddWebSeeds(urls)
	}
}

type WebSeeds struct {
	lock              sync.Mutex
	webSeedsByFilName snaptype.WebSeeds
}

func (d *WebSeeds) GetByFileNames() snaptype.WebSeeds {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.webSeedsByFilName
}
func (d *WebSeeds) SetByFileNames(l snaptype.WebSeeds) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.webSeedsByFilName = l
}

func (d *WebSeeds) callWebSeedsProvider(ctx context.Context, webSeedProviderUrl *url.URL) (snaptype.WebSeedsFromProvider, error) {
	request, err := http.NewRequest(http.MethodGet, webSeedProviderUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	request = request.WithContext(ctx)
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	response := snaptype.WebSeedsFromProvider{}
	if err := toml.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}
	return response, nil
}
func (d *WebSeeds) readWebSeedsFile(webSeedProviderPath string) (snaptype.WebSeedsFromProvider, error) {
	data, err := os.ReadFile(webSeedProviderPath)
	if err != nil {
		return nil, err
	}
	response := snaptype.WebSeedsFromProvider{}
	if err := toml.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	return response, nil
}

func (d *WebSeeds) Discover(ctx context.Context, urls []*url.URL, files []string) {
	list := make([]snaptype.WebSeedsFromProvider, len(urls)+len(files))
	for _, webSeedProviderURL := range urls {
		select {
		case <-ctx.Done():
			break
		default:
		}
		response, err := d.callWebSeedsProvider(ctx, webSeedProviderURL)
		if err != nil { // don't fail on error
			log.Warn("[downloader] callWebSeedsProvider", "err", err, "url", webSeedProviderURL.EscapedPath())
			continue
		}
		list = append(list, response)
	}
	for _, webSeedFile := range files {
		response, err := d.readWebSeedsFile(webSeedFile)
		if err != nil { // don't fail on error
			_, fileName := filepath.Split(webSeedFile)
			log.Warn("[downloader] readWebSeedsFile", "err", err, "file", fileName)
			continue
		}
		list = append(list, response)
	}
	d.SetByFileNames(snaptype.NewWebSeeds(list))
}
