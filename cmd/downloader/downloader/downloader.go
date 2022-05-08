package downloader

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/torrentcfg"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
)

type Downloader struct {
	torrentClient *torrent.Client
	db            kv.RwDB
	cfg           *torrentcfg.Cfg

	statsLock   *sync.RWMutex
	stats       AggStats
	snapshotDir string
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

	peerID, err := readPeerID(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("get peer id: %w", err)
	}
	cfg.PeerID = string(peerID)
	torrentClient, err := torrent.NewClient(cfg.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("fail to start torrent client: %w", err)
	}
	if len(peerID) == 0 {
		if err = savePeerID(cfg.DB, torrentClient.PeerID()); err != nil {
			return nil, fmt.Errorf("save peer id: %w", err)
		}
	}

	return &Downloader{
		cfg:           cfg,
		torrentClient: torrentClient,
		db:            cfg.DB,
		statsLock:     &sync.RWMutex{},
		snapshotDir:   snapshotDir,
	}, nil
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

	d.stats = stats
}

func (d *Downloader) Stats() AggStats {
	d.statsLock.RLock()
	defer d.statsLock.RUnlock()
	return d.stats
}

func (d *Downloader) Close() {
	d.torrentClient.Close()
	d.db.Close()
	if d.cfg.CompletionCloser != nil {
		if err := d.cfg.CompletionCloser.Close(); err != nil {
			log.Warn("[Snapshots] CompletionCloser", "err", err)
		}
	}
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
	return d.torrentClient
}
