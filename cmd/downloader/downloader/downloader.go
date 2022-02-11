package downloader

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
)

const ASSERT = false

type Protocols struct {
	TorrentClient *torrent.Client
	DB            kv.RwDB
}

func New(cfg *torrent.ClientConfig, snapshotDir string) (*Protocols, error) {
	db := mdbx.MustOpen(filepath.Join(snapshotDir, "db"))

	peerID, err := readPeerID(db)
	if err != nil {
		return nil, fmt.Errorf("get peer id: %w", err)
	}
	cfg.PeerID = string(peerID)
	torrentClient, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("fail to start torrent client: %w", err)
	}
	if len(peerID) == 0 {
		if err = savePeerID(db, torrentClient.PeerID()); err != nil {
			return nil, fmt.Errorf("save peer id: %w", err)
		}
	}

	return &Protocols{
		TorrentClient: torrentClient,
		DB:            db,
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

func (cli *Protocols) Close() {
	for _, tr := range cli.TorrentClient.Torrents() {
		tr.Drop()
	}
	cli.TorrentClient.Close()
	cli.DB.Close()
}

func (cli *Protocols) PeerID() []byte {
	peerID := cli.TorrentClient.PeerID()
	return peerID[:]
}

func LoggingLoop(ctx context.Context, torrentClient *torrent.Client) {
	interval := time.Second * 5
	logEvery := time.NewTicker(interval)
	defer logEvery.Stop()
	var m runtime.MemStats
	var stats AggStats

	for {
		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			torrents := torrentClient.Torrents()
			allComplete := true
			gotInfo := 0
			for _, t := range torrents {
				select {
				case <-t.GotInfo(): // all good
					gotInfo++
				default:
					t.AllowDataUpload()
					t.AllowDataDownload()
				}
				allComplete = allComplete && t.Complete.Bool()
			}
			if gotInfo < len(torrents) {
				log.Info(fmt.Sprintf("[torrent] Waiting for torrents metadata: %d/%d", gotInfo, len(torrents)))
				continue
			}

			runtime.ReadMemStats(&m)
			stats = CalcStats(stats, interval, torrentClient)
			if allComplete {
				log.Info("[torrent] Seeding",
					"download", common2.ByteCount(uint64(stats.readBytesPerSec))+"/s",
					"upload", common2.ByteCount(uint64(stats.writeBytesPerSec))+"/s",
					"peers", stats.peersCount,
					"torrents", stats.torrentsCount,
					"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
				continue
			}

			log.Info("[torrent] Downloading",
				"Progress", fmt.Sprintf("%.2f%%", stats.Progress),
				"download", common2.ByteCount(uint64(stats.readBytesPerSec))+"/s",
				"upload", common2.ByteCount(uint64(stats.writeBytesPerSec))+"/s",
				"peers", stats.peersCount,
				"torrents", stats.torrentsCount,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys))
			if stats.peersCount == 0 {
				ips := torrentClient.BadPeerIPs()
				if len(ips) > 0 {
					log.Info("[torrent] Stats", "banned", ips)
				}

			}
		}
	}
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
	for _, t := range torrents {
		stats := t.Stats()
		/*
			var completedPieces, partialPieces int
			psrs := t.PieceStateRuns()
			for _, r := range psrs {
				if r.Complete {
					completedPieces += r.Length
				}
				if r.Partial {
					partialPieces += r.Length
				}
			}
			aggCompletedPieces += completedPieces
			aggPartialPieces += partialPieces
			aggNumPieces = t.NumPieces()
		*/
		result.bytesRead += stats.BytesRead.Int64() + stats.BytesReadData.Int64()
		result.bytesWritten += stats.BytesWritten.Int64() + stats.BytesWrittenData.Int64()
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

// AddTorrentFiles - adding .torrent files to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - Progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func AddTorrentFiles(snapshotsDir string, torrentClient *torrent.Client) error {
	files, err := AllTorrentPaths(snapshotsDir)
	if err != nil {
		return err
	}
	for _, torrentFilePath := range files {
		mi, err := metainfo.LoadFromFile(torrentFilePath)
		if err != nil {
			return err
		}
		mi.AnnounceList = Trackers

		t := time.Now()
		_, err = torrentClient.AddTorrent(mi)
		if err != nil {
			return err
		}
		took := time.Since(t)
		if took > 3*time.Second {
			log.Info("[torrent] Check validity", "file", torrentFilePath, "took", took)
		}
	}

	return nil
}

// ResolveAbsentTorrents - add hard-coded hashes (if client doesn't have) as magnet links and download everything
func ResolveAbsentTorrents(ctx context.Context, torrentClient *torrent.Client, preverifiedHashes []metainfo.Hash, snapshotDir string, silent bool) error {
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	for _, infoHash := range preverifiedHashes {
		if _, ok := torrentClient.Torrent(infoHash); ok {
			continue
		}
		magnet := mi.Magnet(&infoHash, nil)
		t, err := torrentClient.AddMagnet(magnet.String())
		if err != nil {
			return err
		}
		t.AllowDataDownload()
		t.AllowDataUpload()
	}
	if !silent {
		ctxLocal, cancel := context.WithCancel(ctx)
		defer cancel()
		go LoggingLoop(ctxLocal, torrentClient)
	}

	for _, t := range torrentClient.Torrents() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.GotInfo():
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
