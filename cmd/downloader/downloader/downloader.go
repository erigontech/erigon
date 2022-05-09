package downloader

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/torrentcfg"
	"github.com/ledgerwatch/log/v3"
	mdbx2 "github.com/torquem-ch/mdbx-go/mdbx"
	"golang.org/x/sync/semaphore"
)

type Downloader struct {
	db                kv.RwDB
	pieceCompletionDB storage.PieceCompletion
	torrentClient     *torrent.Client
	clientLock        *sync.RWMutex

	cfg *torrentcfg.Cfg

	statsLock   *sync.RWMutex
	stats       AggStats
	snapshotDir string

	folder storage.ClientImplCloser
}

type AggStats struct {
	MetadataReady, FilesTotal int32
	PeersUnique               int32
	ConnectionsTotal          uint64

	Completed bool
	Progress  float32

	BytesCompleted, BytesTotal uint64

	BytesDownload, BytesUpload uint64
	UploadRate, DownloadRate   uint64
}

func New(cfg *torrentcfg.Cfg, snapshotDir string) (*Downloader, error) {
	if err := portMustBeTCPAndUDPOpen(cfg.ListenPort); err != nil {
		return nil, err
	}

	db, c, m, torrentClient, err := openClient(snapshotDir, cfg.ClientConfig)
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

	return &Downloader{
		cfg:               cfg,
		db:                db,
		pieceCompletionDB: c,
		folder:            m,
		torrentClient:     torrentClient,
		clientLock:        &sync.RWMutex{},

		statsLock:   &sync.RWMutex{},
		snapshotDir: snapshotDir,
	}, nil
}

func openClient(snapshotDir string, cfg *torrent.ClientConfig) (db kv.RwDB, c storage.PieceCompletion, m storage.ClientImplCloser, torrentClient *torrent.Client, err error) {
	db, err = mdbx.NewMDBX(log.New()).
		Flags(func(f uint) uint { return f | mdbx2.SafeNoSync }).
		Label(kv.DownloaderDB).
		WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.DownloaderTablesCfg }).
		SyncPeriod(15 * time.Second).
		Path(filepath.Join(snapshotDir, "db")).
		Open()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	c, err = torrentcfg.NewMdbxPieceCompletion(db)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	m = storage.NewMMapWithCompletion(snapshotDir, c)
	torrentClient, err = torrent.NewClient(cfg)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return db, c, m, torrentClient, nil
}

func (d *Downloader) Start(ctx context.Context, silent bool) error {
	if err := BuildTorrentsAndAdd(ctx, d.snapshotDir, d.torrentClient); err != nil {
		return fmt.Errorf("BuildTorrentsAndAdd: %w", err)
	}

	var sem = semaphore.NewWeighted(int64(d.cfg.DownloadSlots))

	go func() {
		for {
			torrents := d.Torrent().Torrents()
			for _, t := range torrents {
				<-t.GotInfo()
				if t.Complete.Bool() {
					continue
				}
				if err := sem.Acquire(ctx, 1); err != nil {
					return
				}
				t.AllowDataDownload()
				t.DownloadAll()
				go func(t *torrent.Torrent) {
					//r := t.NewReader()
					//r.SetReadahead(t.Length())
					//_, _ = io.Copy(io.Discard, r) // enable streaming - it will prioritize sequential download

					<-t.Complete.On()
					sem.Release(1)
				}(t)
			}
			time.Sleep(30 * time.Second)
		}
	}()

	go func() {
		var m runtime.MemStats
		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()

		interval := 10 * time.Second
		statEvery := time.NewTicker(interval)
		defer statEvery.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-statEvery.C:
				d.ReCalcStats(interval)

			case <-logEvery.C:
				if silent {
					continue
				}

				stats := d.Stats()

				if stats.MetadataReady < stats.FilesTotal {
					log.Info(fmt.Sprintf("[Snapshots] Waiting for torrents metadata: %d/%d", stats.MetadataReady, stats.FilesTotal))
					continue
				}

				runtime.ReadMemStats(&m)
				if stats.Completed {
					log.Info("[Snapshots] Seeding",
						"up", common2.ByteCount(stats.UploadRate)+"/s",
						"peers", stats.PeersUnique,
						"connections", stats.ConnectionsTotal,
						"files", stats.FilesTotal,
						"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
					continue
				}

				log.Info("[Snapshots] Downloading",
					"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, common2.ByteCount(stats.BytesCompleted), common2.ByteCount(stats.BytesTotal)),
					"download", common2.ByteCount(stats.DownloadRate)+"/s",
					"upload", common2.ByteCount(stats.UploadRate)+"/s",
					"peers", stats.PeersUnique,
					"connections", stats.ConnectionsTotal,
					"files", stats.FilesTotal,
					"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
				if stats.PeersUnique == 0 {
					ips := d.Torrent().BadPeerIPs()
					if len(ips) > 0 {
						log.Info("[Snapshots] Stats", "banned", ips)
					}
				}
			}
		}
	}()

	return nil
}

func (d *Downloader) ReCalcStats(interval time.Duration) {
	d.statsLock.Lock()
	defer d.statsLock.Unlock()
	prevStats, stats := d.stats, d.stats

	peers := make(map[torrent.PeerID]struct{}, 16)
	torrents := d.torrentClient.Torrents()
	connStats := d.torrentClient.ConnStats()

	stats.Completed = true
	stats.BytesDownload = uint64(connStats.BytesReadUsefulIntendedData.Int64())
	stats.BytesUpload = uint64(connStats.BytesWrittenData.Int64())

	stats.BytesTotal, stats.BytesCompleted, stats.ConnectionsTotal, stats.MetadataReady = 0, 0, 0, 0
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
		default:
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

	if prevStats.Completed == false && stats.Completed == true {
		d.onComplete()
	}

	d.stats = stats
}

func (d *Downloader) onComplete() {
	d.clientLock.Lock()
	defer d.clientLock.Unlock()

	d.torrentClient.Close()
	d.folder.Close()
	d.pieceCompletionDB.Close()
	d.db.Close()

	db, c, m, torrentClient, err := openClient(d.snapshotDir, d.cfg.ClientConfig)
	if err != nil {
		panic(err)
	}
	d.db = db
	d.pieceCompletionDB = c
	d.folder = m
	d.torrentClient = torrentClient
}

func (d *Downloader) Stats() AggStats {
	d.statsLock.RLock()
	defer d.statsLock.RUnlock()
	return d.stats
}

func (d *Downloader) Close() {
	d.torrentClient.Close()
	if err := d.folder.Close(); err != nil {
		log.Warn("[Snapshots] folder.close", "err", err)
	}
	if err := d.pieceCompletionDB.Close(); err != nil {
		log.Warn("[Snapshots] pieceCompletionDB.close", "err", err)
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
