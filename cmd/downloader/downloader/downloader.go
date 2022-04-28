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
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/torrentcfg"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
)

type Protocols struct {
	TorrentClient *torrent.Client
	DB            kv.RwDB
	cfg           *torrentcfg.Cfg

	statsLock   *sync.RWMutex
	stats       AggStats
	snapshotDir *dir.Rw
}

func New(cfg *torrentcfg.Cfg, snapshotDir *dir.Rw) (*Protocols, error) {
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

	return &Protocols{
		cfg:           cfg,
		TorrentClient: torrentClient,
		DB:            cfg.DB,
		statsLock:     &sync.RWMutex{},
		snapshotDir:   snapshotDir,
	}, nil
}

func savePeerID(db kv.RwDB, peerID torrent.PeerID) error {
	return db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Put(kv.BittorrentInfo, []byte(kv.BittorrentPeerID), peerID[:])
	})
}

func readPeerID(db kv.RoDB) (peerID []byte, err error) {
	if err = db.View(context.Background(), func(tx kv.Tx) error {
		peerIDFromDB, err := tx.GetOne(kv.BittorrentInfo, []byte(kv.BittorrentPeerID))
		if err != nil {
			return fmt.Errorf("get peer id: %w", err)
		}
		peerID = common2.Copy(peerIDFromDB)
		return nil
	}); err != nil {
		return nil, err
	}
	return peerID, nil
}

func (cli *Protocols) Start(ctx context.Context, silent bool) error {
	if err := BuildTorrentsAndAdd(ctx, cli.snapshotDir, cli.TorrentClient); err != nil {
		return err
	}

	var sem = semaphore.NewWeighted(int64(cli.cfg.DownloadSlots))

	go func() {
		for {
			torrents := cli.TorrentClient.Torrents()
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
					//_, _ = io.Copy(io.Discard, r) // enable streaming - it will prioritize sequential download

					<-t.Complete.On()
					sem.Release(1)
				}(t)
			}
			time.Sleep(330 * time.Second)
		}
	}()

	go func() {
		var m runtime.MemStats
		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()

		interval := 5 * time.Second
		statEvery := time.NewTicker(interval)
		defer statEvery.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-statEvery.C:
				cli.ReCalcStats(interval)

			case <-logEvery.C:
				if silent {
					continue
				}

				stats := cli.Stats()

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
					ips := cli.TorrentClient.BadPeerIPs()
					if len(ips) > 0 {
						log.Info("[Snapshots] Stats", "banned", ips)
					}
				}
			}
		}
	}()
	return nil
}

func (cli *Protocols) ReCalcStats(interval time.Duration) {
	cli.statsLock.Lock()
	defer cli.statsLock.Unlock()
	prevStats, stats := cli.stats, cli.stats

	peers := make(map[torrent.PeerID]struct{}, 16)
	torrents := cli.TorrentClient.Torrents()
	connStats := cli.TorrentClient.ConnStats()

	stats.BytesRead = uint64(connStats.BytesReadUsefulIntendedData.Int64())
	stats.BytesWritten = uint64(connStats.BytesWrittenData.Int64())

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

	stats.DownloadRate = (stats.BytesRead - prevStats.BytesRead) / uint64(interval.Seconds())
	stats.UploadRate = (stats.BytesWritten - prevStats.BytesWritten) / uint64(interval.Seconds())

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

	cli.stats = stats
}

func (cli *Protocols) Stats() AggStats {
	cli.statsLock.RLock()
	defer cli.statsLock.RUnlock()
	return cli.stats
}

func (cli *Protocols) Close() {
	//for _, tr := range cli.TorrentClient.Torrents() {
	//	go func() {}()
	//	fmt.Printf("alex: CLOse01: %s\n", tr.Name())
	//	tr.DisallowDataUpload()
	//	fmt.Printf("alex: CLOse02: %s\n", tr.Name())
	//	tr.DisallowDataDownload()
	//	fmt.Printf("alex: CLOse03: %s\n", tr.Name())
	//  ch := t.Closed()
	//	tr.Drop()
	//  <-ch
	//}
	cli.TorrentClient.Close()
	cli.DB.Close()
	if cli.cfg.CompletionCloser != nil {
		if err := cli.cfg.CompletionCloser.Close(); err != nil {
			log.Warn("[Snapshots] CompletionCloser", "err", err)
		}
	}
}

func (cli *Protocols) PeerID() []byte {
	peerID := cli.TorrentClient.PeerID()
	return peerID[:]
}

func (cli *Protocols) StopSeeding(hash metainfo.Hash) error {
	t, ok := cli.TorrentClient.Torrent(hash)
	if !ok {
		return nil
	}
	ch := t.Closed()
	t.Drop()
	<-ch
	return nil
}

type AggStats struct {
	MetadataReady, FilesTotal int32
	PeersUnique               int32
	ConnectionsTotal          uint64

	Completed bool
	Progress  float32

	BytesCompleted, BytesTotal uint64

	UploadRate, DownloadRate uint64

	BytesRead    uint64
	BytesWritten uint64
}

// AddTorrentFile - adding .torrent file to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - Progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func AddTorrentFile(ctx context.Context, torrentFilePath string, torrentClient *torrent.Client) (*torrent.Torrent, error) {
	mi, err := metainfo.LoadFromFile(torrentFilePath)
	if err != nil {
		return nil, err
	}
	mi.AnnounceList = Trackers
	t, err := torrentClient.AddTorrent(mi)
	if err != nil {
		return nil, err
	}
	t.DisallowDataDownload()
	t.AllowDataUpload()
	return t, nil
}

func VerifyDtaFiles(ctx context.Context, snapshotDir string) error {
	logEvery := time.NewTicker(5 * time.Second)
	defer logEvery.Stop()
	files, err := AllTorrentPaths(snapshotDir)
	if err != nil {
		return err
	}
	totalPieces := 0
	for _, f := range files {
		metaInfo, err := metainfo.LoadFromFile(f)
		if err != nil {
			return err
		}
		info, err := metaInfo.UnmarshalInfo()
		if err != nil {
			return err
		}
		totalPieces += info.NumPieces()
	}

	j := 0
	for _, f := range files {
		metaInfo, err := metainfo.LoadFromFile(f)
		if err != nil {
			return err
		}
		info, err := metaInfo.UnmarshalInfo()
		if err != nil {
			return err
		}

		err = verifyTorrent(&info, snapshotDir, func(i int, good bool) error {
			j++
			if !good {
				log.Error("[Snapshots] Verify hash mismatch", "at piece", i, "file", f)
				return fmt.Errorf("invalid file")
			}
			select {
			case <-logEvery.C:
				log.Info("[Snapshots] Verify", "Progress", fmt.Sprintf("%.2f%%", 100*float64(j)/float64(totalPieces)))
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	log.Info("[Snapshots] Verify succeed")
	return nil
}
