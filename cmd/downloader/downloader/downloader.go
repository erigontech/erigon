package downloader

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/peer_protocol"
	"github.com/anacrolix/torrent/storage"
	"github.com/dustin/go-humanize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"github.com/ledgerwatch/log/v3"
)

type Client struct {
	Cli                  *torrent.Client
	pieceCompletionStore storage.PieceCompletion
}

func New(snapshotsDir string, seeding bool, peerID string) (*Client, error) {
	torrentConfig := DefaultTorrentConfig()
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	torrentConfig.UpnpID = torrentConfig.UpnpID + "leecher"
	torrentConfig.PeerID = peerID

	progressStore, err := storage.NewBoltPieceCompletion(snapshotsDir)
	if err != nil {
		panic(err)
	}
	torrentConfig.DefaultStorage = storage.NewMMapWithCompletion(snapshotsDir, progressStore)

	torrentClient, err := torrent.NewClient(torrentConfig)
	if err != nil {
		log.Error("Fail to start torrnet client", "err", err)
		return nil, fmt.Errorf("fail to start: %w", err)
	}

	log.Info(fmt.Sprintf("Seeding: %t, my peerID: %x", seeding, torrentClient.PeerID()))

	return &Client{
		Cli:                  torrentClient,
		pieceCompletionStore: progressStore,
	}, nil
}

func DefaultTorrentConfig() *torrent.ClientConfig {
	torrentConfig := torrent.NewDefaultClientConfig()
	torrentConfig.ListenPort = 0
	// debug
	torrentConfig.Debug = false
	torrentConfig.Logger = NewAdapterLogger()
	torrentConfig.Logger = torrentConfig.Logger.FilterLevel(lg.Info)

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
	for {
		var stats aggStats
		//var prevBytesReadUsefulData, aggByteRate int64
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
			if allComplete {
				peers := map[torrent.PeerID]struct{}{}
				for _, t := range torrentClient.Torrents() {
					for _, peer := range t.PeerConns() {
						peers[peer.PeerID] = struct{}{}
					}
				}

				log.Info("[torrent] Seeding", "peers", len(peers), "torrents", len(torrents))
				continue
			}

			stats = calcStats(stats, interval, torrentClient)

			line := fmt.Sprintf(
				"[torrent] Downloading: %d%%, %v/s, peers: %d",
				stats.progress,
				humanize.Bytes(uint64(stats.readBytesPerSec)),
				stats.peersCount,
			)
			log.Info(line)
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

	progress int

	bytesRead    int64
	bytesWritten int64
}

func calcStats(prevStats aggStats, interval time.Duration, client *torrent.Client) (result aggStats) {
	var aggBytesCompleted, aggLen int64
	//var aggCompletedPieces, aggNumPieces, aggPartialPieces int
	peers := map[torrent.PeerID]*torrent.PeerConn{}
	for _, t := range client.Torrents() {
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

		result.bytesRead = stats.BytesReadUsefulData.Int64()
		result.readBytesPerSec = (result.bytesRead - prevStats.bytesRead) / int64(interval)

		result.bytesWritten = stats.BytesWritten.Int64()
		result.writeBytesPerSec = (result.bytesWritten - prevStats.bytesWritten) / int64(interval)

		aggBytesCompleted += t.BytesCompleted()
		aggLen += t.Length()

		for _, peer := range t.PeerConns() {
			peers[peer.PeerID] = peer
		}
	}
	result.progress = int(100 * (float64(aggBytesCompleted) / float64(aggLen)))
	result.peersCount = int64(len(peers))
	return result
}

// AddTorrentFiles - adding .torrent files to torrentClient (and checking their hashes), if .torrent file
// added first time - pieces verification process will start (disk IO heavy) - progress
// kept in `piece completion storage` (surviving reboot). Once it done - no disk IO needed again.
// Don't need call torrent.VerifyData manually
func AddTorrentFiles(ctx context.Context, snapshotsDir string, torrentClient *torrent.Client, preverifiedHashes snapshothashes.Preverified) error {
	if err := ForEachTorrentFile(snapshotsDir, func(torrentFilePath string) error {
		mi, err := metainfo.LoadFromFile(torrentFilePath)
		if err != nil {
			return err
		}
		mi.AnnounceList = Trackers

		// skip non-preverified files
		_, torrentFileName := filepath.Split(torrentFilePath)
		segmentFileName := segmentFileNameFromTorrentFileName(torrentFileName)
		hashString, ok := preverifiedHashes[segmentFileName]
		if !ok {
			return nil
		}
		expect := metainfo.NewHashFromHex(hashString)
		if mi.HashInfoBytes() != expect {
			return fmt.Errorf("file %s has unexpected hash %x, expected %x", torrentFileName, mi.HashInfoBytes(), expect)
		}

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
