package downloader

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/peer_protocol"
	"github.com/anacrolix/torrent/storage"
	"github.com/c2h5oh/datasize"
	"github.com/dustin/go-humanize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/time/rate"
)

const ASSERT = false

type Client struct {
	Cli                  *torrent.Client
	pieceCompletionStore storage.PieceCompletion
}

func TorrentConfig(snapshotsDir string, seeding bool, peerID string, verbosity lg.Level, downloadRate, uploadRate datasize.ByteSize) (*torrent.ClientConfig, storage.PieceCompletion, error) {
	torrentConfig := DefaultTorrentConfig()
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	torrentConfig.UpnpID = torrentConfig.UpnpID + "leecher"
	torrentConfig.PeerID = peerID

	// rates are divided by 2 - I don't know why it works, maybe bug inside torrent lib accounting
	torrentConfig.UploadRateLimiter = rate.NewLimiter(rate.Limit(uploadRate.Bytes()/2), 2*DefaultPieceSize)     // default: unlimited
	torrentConfig.DownloadRateLimiter = rate.NewLimiter(rate.Limit(downloadRate.Bytes()/2), 2*DefaultPieceSize) // default: unlimited

	// debug
	if lg.Debug == verbosity {
		torrentConfig.Debug = true
	}
	torrentConfig.Logger = NewAdapterLogger().FilterLevel(verbosity)

	progressStore, err := storage.NewBoltPieceCompletion(snapshotsDir)
	if err != nil {
		return nil, nil, err
	}
	torrentConfig.DefaultStorage = storage.NewMMapWithCompletion(snapshotsDir, progressStore)

	return torrentConfig, progressStore, nil
}

func New(cfg *torrent.ClientConfig, progressStore storage.PieceCompletion) (*Client, error) {
	torrentClient, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("fail to start torrent client: %w", err)
	}
	return &Client{
		Cli:                  torrentClient,
		pieceCompletionStore: progressStore,
	}, nil
}

func DefaultTorrentConfig() *torrent.ClientConfig {
	torrentConfig := torrent.NewDefaultClientConfig()
	torrentConfig.ListenPort = 0

	// enable dht
	torrentConfig.NoDHT = true
	torrentConfig.DisableTrackers = false
	//torrentConfig.DisableWebtorrent = true
	//torrentConfig.DisableWebseeds = true

	// Increase default timeouts, because we often run on commodity networks
	torrentConfig.MinDialTimeout = 6 * time.Second      // default: 3sec
	torrentConfig.NominalDialTimeout = 20 * time.Second // default: 20sec
	torrentConfig.HandshakesTimeout = 8 * time.Second   // default: 4sec

	torrentConfig.MinPeerExtensions.SetBit(peer_protocol.ExtensionBitFast, true)

	torrentConfig.EstablishedConnsPerTorrent = 10 // default: 50
	torrentConfig.TorrentPeersHighWater = 100     // default: 500
	torrentConfig.TorrentPeersLowWater = 50       // default: 50

	return torrentConfig
}

func (cli *Client) SavePeerID(db kv.Putter) error {
	return db.Put(kv.BittorrentInfo, []byte(kv.BittorrentPeerID), cli.PeerID())
}

func (cli *Client) Close() {
	for _, tr := range cli.Cli.Torrents() {
		tr.Drop()
	}
	cli.pieceCompletionStore.Close()
	cli.Cli.Close()
}

func (cli *Client) PeerID() []byte {
	peerID := cli.Cli.PeerID()
	return peerID[:]
}

func MainLoop(ctx context.Context, torrentClient *torrent.Client) {
	interval := time.Second * 5
	logEvery := time.NewTicker(interval)
	defer logEvery.Stop()
	var m runtime.MemStats
	var stats aggStats

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
			alloc := common.StorageSize(m.Alloc)
			sys := common.StorageSize(m.Sys)

			stats = calcStats(stats, interval, torrentClient)
			if allComplete {
				log.Info(fmt.Sprintf(
					"[torrent] Seeding: ↓%v/s ↑%v/s, peers: %d, torrents: %d",
					humanize.Bytes(uint64(stats.readBytesPerSec)),
					humanize.Bytes(uint64(stats.writeBytesPerSec)),
					stats.peersCount,
					stats.torrentsCount,
				), "alloc", alloc, "sys", sys)
				continue
			}

			log.Info(fmt.Sprintf(
				"[torrent] Downloading: %.2f%%, ↓%v/s ↑%v/s, peers: %d, torrents: %d",
				stats.progress,
				humanize.Bytes(uint64(stats.readBytesPerSec)),
				humanize.Bytes(uint64(stats.writeBytesPerSec)),
				stats.peersCount,
				stats.torrentsCount,
			), "alloc", alloc, "sys", sys)
		}
	}
}

func (cli *Client) StopSeeding(hash metainfo.Hash) error {
	t, ok := cli.Cli.Torrent(hash)
	if !ok {
		return nil
	}
	ch := t.Closed()
	t.Drop()
	<-ch
	return nil
}

type aggStats struct {
	readBytesPerSec  int64
	writeBytesPerSec int64
	peersCount       int64

	progress      float32
	torrentsCount int

	bytesRead    int64
	bytesWritten int64
}

func calcStats(prevStats aggStats, interval time.Duration, client *torrent.Client) (result aggStats) {
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

	result.progress = float32(float64(100) * (float64(aggBytesCompleted) / float64(aggLen)))

	result.peersCount = int64(len(peers))
	result.torrentsCount = len(torrents)
	return result
}

// AddTorrentFiles - adding .torrent files to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func AddTorrentFiles(ctx context.Context, snapshotsDir string, torrentClient *torrent.Client) error {
	if err := ForEachTorrentFile(snapshotsDir, func(torrentFilePath string) error {
		mi, err := metainfo.LoadFromFile(torrentFilePath)
		if err != nil {
			return err
		}
		mi.AnnounceList = Trackers

		if _, err = torrentClient.AddTorrent(mi); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	//waitForChecksumVerify(ctx, torrentClient)
	return nil
}

// ResolveAbsentTorrents - add hard-coded hashes (if client doesn't have) as magnet links and download everything
func ResolveAbsentTorrents(ctx context.Context, torrentClient *torrent.Client, preverifiedHashes []metainfo.Hash, snapshotDir string) error {
	mi := &metainfo.MetaInfo{AnnounceList: Trackers}
	wg := &sync.WaitGroup{}
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

		wg.Add(1)
		go func(t *torrent.Torrent, infoHash metainfo.Hash) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				t.Drop()
				return
			case <-t.GotInfo():
				mi := t.Metainfo()
				_ = CreateTorrentFileIfNotExists(snapshotDir, t.Info(), &mi)
			}
		}(t, infoHash)
	}

	wg.Wait()

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
					humanize.Bytes(uint64(aggBytesCompleted)),
					humanize.Bytes(uint64(aggLen)),
				)
				log.Info(line)
			}
		}
	}()
	torrentClient.WaitAll() // wait for checksum verify
}
