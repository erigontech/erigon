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
)

const ASSERT = false

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
	if err := CreateTorrentFilesAndAdd(ctx, cli.snapshotDir, cli.TorrentClient); err != nil {
		return fmt.Errorf("CreateTorrentFilesAndAdd: %w", err)
	}

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

				torrents := cli.TorrentClient.Torrents()
				allComplete := true
				gotInfo := 0
				for _, t := range torrents {
					select {
					case <-t.GotInfo(): // all good
						gotInfo++
					default:
					}
					allComplete = allComplete && t.Complete.Bool()
				}
				if gotInfo < len(torrents) {
					log.Info(fmt.Sprintf("[torrent] Waiting for torrents metadata: %d/%d", gotInfo, len(torrents)))
					continue
				}

				runtime.ReadMemStats(&m)
				stats := cli.Stats()
				if allComplete {
					log.Info("[torrent] Seeding",
						"download", common2.ByteCount(uint64(stats.readBytesPerSec))+"/s",
						"upload", common2.ByteCount(uint64(stats.writeBytesPerSec))+"/s",
						"unique_peers", stats.peersCount,
						"files", stats.torrentsCount,
						"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
					continue
				}

				log.Info("[torrent] Downloading",
					"Progress", fmt.Sprintf("%.2f%%", stats.Progress),
					"download", common2.ByteCount(uint64(stats.readBytesPerSec))+"/s",
					"upload", common2.ByteCount(uint64(stats.writeBytesPerSec))+"/s",
					"unique_peers", stats.peersCount,
					"files", stats.torrentsCount,
					"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
				if stats.peersCount == 0 {
					ips := cli.TorrentClient.BadPeerIPs()
					if len(ips) > 0 {
						log.Info("[torrent] Stats", "banned", ips)
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
	prevStats := cli.stats

	var aggBytesCompleted, aggLen int64
	peers := map[torrent.PeerID]*torrent.PeerConn{}
	torrents := cli.TorrentClient.Torrents()
	connStats := cli.TorrentClient.ConnStats()

	cli.stats.bytesRead += connStats.BytesReadUsefulIntendedData.Int64()
	cli.stats.bytesWritten += connStats.BytesWrittenData.Int64()

	for _, t := range torrents {
		aggBytesCompleted += t.BytesCompleted()
		aggLen += t.Length()

		for _, peer := range t.PeerConns() {
			peers[peer.PeerID] = peer
		}
	}

	cli.stats.readBytesPerSec += (cli.stats.bytesRead - prevStats.bytesRead) / int64(interval.Seconds())
	cli.stats.writeBytesPerSec += (cli.stats.bytesWritten - prevStats.bytesWritten) / int64(interval.Seconds())

	cli.stats.Progress = float32(float64(100) * (float64(aggBytesCompleted) / float64(aggLen)))
	cli.stats.peersCount = int64(len(peers))
	cli.stats.torrentsCount = len(torrents)
}

func (cli *Protocols) Stats() AggStats {
	cli.statsLock.RLock()
	defer cli.statsLock.RUnlock()
	return cli.stats
}

func (cli *Protocols) Close() {
	for _, tr := range cli.TorrentClient.Torrents() {
		tr.Drop()
	}
	cli.TorrentClient.Close()
	cli.DB.Close()
	if cli.cfg.CompletionCloser != nil {
		cli.cfg.CompletionCloser.Close() //nolint
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
	readBytesPerSec  int64
	writeBytesPerSec int64
	peersCount       int64

	Progress      float32
	torrentsCount int

	bytesRead    int64
	bytesWritten int64
}

func CalcStats(prevStats AggStats, interval time.Duration, client *torrent.Client) (result AggStats) {
	var aggBytesCompleted, aggLen int64
	//var aggCompletedPieces, aggNumPieces, aggPartialPieces int
	peers := map[torrent.PeerID]*torrent.PeerConn{}
	torrents := client.Torrents()
	connStats := client.ConnStats()

	result.bytesRead += connStats.BytesReadUsefulIntendedData.Int64()
	result.bytesWritten += connStats.BytesWrittenData.Int64()

	for _, t := range torrents {
		aggBytesCompleted += t.BytesCompleted()
		aggLen += t.Length()

		for _, peer := range t.PeerConns() {
			peers[peer.PeerID] = peer
		}
	}

	result.readBytesPerSec += (result.bytesRead - prevStats.bytesRead) / int64(interval.Seconds())
	result.writeBytesPerSec += (result.bytesWritten - prevStats.bytesWritten) / int64(interval.Seconds())

	result.Progress = float32(float64(100) * (float64(aggBytesCompleted) / float64(aggLen)))

	result.peersCount = int64(len(peers))
	result.torrentsCount = len(torrents)
	return result
}

func AddTorrentFile(ctx context.Context, torrentFilePath string, torrentClient *torrent.Client) (mi *metainfo.MetaInfo, err error) {
	mi, err = metainfo.LoadFromFile(torrentFilePath)
	if err != nil {
		return nil, err
	}
	mi.AnnounceList = Trackers

	t := time.Now()
	_, err = torrentClient.AddTorrent(mi)
	if err != nil {
		return mi, err
	}
	took := time.Since(t)
	if took > 3*time.Second {
		log.Info("[torrent] Check validity", "file", torrentFilePath, "took", took)
	}
	return mi, nil
}

// AddTorrentFiles - adding .torrent files to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - Progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func AddTorrentFiles(ctx context.Context, snapshotsDir *dir.Rw, torrentClient *torrent.Client) error {
	files, err := AllTorrentPaths(snapshotsDir.Path)
	if err != nil {
		return err
	}
	for _, torrentFilePath := range files {
		if _, err := AddTorrentFile(ctx, torrentFilePath, torrentClient); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

	}

	return nil
}

// ResolveAbsentTorrents - add hard-coded hashes (if client doesn't have) as magnet links and download everything
func ResolveAbsentTorrents(ctx context.Context, cli *Protocols, preverifiedHashes []metainfo.Hash, snapshotDir *dir.Rw) error {
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	torrentClient := cli.TorrentClient
	for i := range preverifiedHashes {
		if _, ok := torrentClient.Torrent(preverifiedHashes[i]); ok {
			continue
		}
		magnet := mi.Magnet(&preverifiedHashes[i], nil)
		t, err := torrentClient.AddMagnet(magnet.String())
		if err != nil {
			return err
		}
		t.AllowDataDownload()
		t.AllowDataUpload()
	}

	for _, t := range torrentClient.Torrents() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.GotInfo():
			if !t.Complete.Bool() {
				t.DownloadAll()
			}
			mi := t.Metainfo()
			if err := CreateTorrentFileIfNotExists(snapshotDir, t.Info(), &mi); err != nil {
				return err
			}
		}
	}

	return nil
}

//nolint
func waitForChecksumVerify(ctx context.Context, torrentClient *torrent.Client) {
	//TODO: tr.VerifyData() - find when to call it
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		interval := time.Second * 5
		logEvery := time.NewTicker(interval)
		defer logEvery.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-logEvery.C:
				var aggBytesCompleted, aggLen int64
				for _, t := range torrentClient.Torrents() {
					aggBytesCompleted += t.BytesCompleted()
					aggLen += t.Length()
				}

				line := fmt.Sprintf(
					"[torrent] verifying snapshots: %s/%s",
					common2.ByteCount(uint64(aggBytesCompleted)),
					common2.ByteCount(uint64(aggLen)),
				)
				log.Info(line)
			}
		}
	}()
	torrentClient.WaitAll() // wait for checksum verify
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
				log.Error("[torrent] Verify hash mismatch", "at piece", i, "file", f)
				return fmt.Errorf("invalid file")
			}
			select {
			case <-logEvery.C:
				log.Info("[torrent] Verify", "Progress", fmt.Sprintf("%.2f%%", 100*float64(j)/float64(totalPieces)))
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
	log.Info("[torrent] Verify succeed")
	return nil
}
