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
	"github.com/anacrolix/torrent/storage"
	"github.com/dustin/go-humanize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshothashes"
	"github.com/ledgerwatch/log/v3"
)

type Client struct {
	Cli          *torrent.Client
	snapshotsDir string
}

func New(snapshotsDir string, seeding bool, peerID string) (*Client, error) {
	torrentConfig := DefaultTorrentConfig()
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	torrentConfig.UpnpID = torrentConfig.UpnpID + "leecher"
	torrentConfig.PeerID = peerID
	//torrentConfig.DisableWebtorrent = true
	//torrentConfig.DisableWebseeds = true
	//torrentConfig.NoDHT = true

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

	//log.Info(fmt.Sprintf("Torrent protocol listen: %s,  my IP: %s", publicIP, GetOutboundIP()))
	log.Info(fmt.Sprintf("Seeding: %t, my peerID: %x", seeding, torrentClient.PeerID()))

	/*
		{
			if err := BuildTorrentFilesIfNeed(context.Background(), snapshotsDir); err != nil {
				return nil, err
			}
			preverifiedHashes := snapshothashes.Goerli
			if err := AddTorrentFiles(context.Background(), snapshotsDir, torrentClient, preverifiedHashes); err != nil {
				return nil, err
			}

			infoHashes := make([]metainfo.Hash, len(preverifiedHashes))
			i := 0
			for _, hashStr := range preverifiedHashes {
				infoHashes[i] = metainfo.NewHashFromHex(hashStr)
				i++
			}
			if err := ResolveAbsentTorrents(context.Background(), torrentClient, infoHashes); err != nil {
				return nil, err
			}
			if err := CreateAbsentTorrentFiles(context.Background(), torrentClient, snapshotsDir); err != nil {
				return nil, err
			}
			if err := DownloadAll(context.Background(), torrentClient); err != nil {
				return nil, err
			}
			if err := Seed(context.Background(), torrentClient); err != nil {
				return nil, err
			}

			torrentClient.Close()
			progressStore.Close()
			panic(1)
		}
	*/

	return &Client{
		Cli:          torrentClient,
		snapshotsDir: snapshotsDir,
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

	//torrentConfig.MinPeerExtensions.SetBit(peer_protocol.ExtensionBitFast, true)
	return torrentConfig
}

func (cli *Client) SavePeerID(db kv.Putter) error {
	return db.Put(kv.BittorrentInfo, []byte(kv.BittorrentPeerID), cli.PeerID())
}

func (cli *Client) Close() {
	cli.Cli.Close()
}

func (cli *Client) PeerID() []byte {
	peerID := cli.Cli.PeerID()
	return peerID[:]
}

func MainLoop(ctx context.Context, srv *SNDownloaderServer) {
	torrentClient := srv.t.Cli
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := DownloadAll(ctx, torrentClient); err != nil {
			log.Error("DownloadAll", "err", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if err := Seed(ctx, torrentClient); err != nil {
			log.Error("Seed", "err", err)
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

func (cli *Client) Download() {
	log.Info("Start snapshot downloading")
	torrents := cli.Cli.Torrents()
	for i := range torrents {
		t := torrents[i]
		go func(t *torrent.Torrent) {
			defer debug.LogPanic()
			t.AllowDataDownload()
			t.DownloadAll()

			tt := time.Now()
			prev := t.BytesCompleted()
		dwn:
			for {
				if t.Info().TotalLength()-t.BytesCompleted() == 0 {
					log.Info("Dowloaded", "snapshot", t.Name(), "t", time.Since(tt))
					break dwn
				} else {
					stats := t.Stats()
					log.Info("Downloading snapshot",
						"snapshot", t.Name(),
						"%", int(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))),
						"mb", t.BytesCompleted()/1024/1024,
						"diff(kb)", (t.BytesCompleted()-prev)/1024,
						"seeders", stats.ConnectedSeeders,
						"active", stats.ActivePeers,
						"total", stats.TotalPeers)
					prev = t.BytesCompleted()
					time.Sleep(time.Second * 10)

				}

			}
		}(t)
	}
	cli.Cli.WaitAll()

	for _, t := range cli.Cli.Torrents() {
		log.Info("Snapshot seeding", "name", t.Name(), "seeding", t.Seeding())
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
		hashString, ok := preverifiedHashes[torrentFileName]
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

	waitForChecksumVerify(ctx, torrentClient)
	return nil
}

func CreateAbsentTorrentFiles(ctx context.Context, torrentClient *torrent.Client, snapshotDir string) error {
	for _, t := range torrentClient.Torrents() {
		if err := CreateTorrentFileIfNotExists(snapshotDir, t.Info()); err != nil {
			return err
		}
	}
	return nil
}

func DownloadAll(ctx context.Context, torrentClient *torrent.Client) error {
	for _, t := range torrentClient.Torrents() {
		t.DownloadAll()
	}
	waitForDownloadAll(ctx, torrentClient)
	return nil
}

func Seed(ctx context.Context, torrentClient *torrent.Client) error {
	interval := 30 * time.Second
	logEvery := time.NewTicker(interval)
	defer logEvery.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			peers := map[torrent.PeerID]struct{}{}
			for _, t := range torrentClient.Torrents() {
				for _, peer := range t.PeerConns() {
					peers[peer.PeerID] = struct{}{}
				}
			}
			log.Info("[torrent] Seeding", "peers", len(peers))
		}
	}
}

// ResolveAbsentTorrents - add hard-coded hashes (if client doesn't have) as magnet links and download everything
func ResolveAbsentTorrents(ctx context.Context, torrentClient *torrent.Client, preverifiedHashes []metainfo.Hash) error {
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

func waitForDownloadAll(ctx context.Context, torrentClient *torrent.Client) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		interval := time.Second * 5
		logEvery := time.NewTicker(interval)
		defer logEvery.Stop()

		var prevBytesReadUsefulData, aggByteRate int64

		/*
			tt := time.Now()
				prev := t.BytesCompleted()
			dwn:
				for {
					if t.Info().TotalLength()-t.BytesCompleted() == 0 {
						log.Info("Dowloaded", "snapshot", t.Name(), "t", time.Since(tt))
						break dwn
					} else {
						stats := t.Stats()
						log.Info("Downloading snapshot",
							"snapshot", t.Name(),
							"%", int(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))),
							"mb", t.BytesCompleted()/1024/1024,
							"diff(kb)", (t.BytesCompleted()-prev)/1024,
							"seeders", stats.ConnectedSeeders,
							"active", stats.ActivePeers,
							"total", stats.TotalPeers)
						prev = t.BytesCompleted()
						time.Sleep(time.Second * 10)

					}

				}
		*/
		for {
			select {
			case <-ctx.Done():
				return
			case <-logEvery.C:
				var aggBytesCompleted, aggLen int64
				//var aggCompletedPieces, aggNumPieces, aggPartialPieces int
				peers := map[torrent.PeerID]*torrent.PeerConn{}

				for _, t := range torrentClient.Torrents() {
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

					byteRate := int64(time.Second)
					bytesReadUsefulData := stats.BytesReadUsefulData.Int64()
					byteRate *= stats.BytesReadUsefulData.Int64() - prevBytesReadUsefulData
					byteRate /= int64(interval)
					aggByteRate += byteRate

					prevBytesReadUsefulData = bytesReadUsefulData
					aggBytesCompleted += t.BytesCompleted()
					aggLen += t.Length()

					for _, peer := range t.PeerConns() {
						peers[peer.PeerID] = peer
					}

				}

				line := fmt.Sprintf(
					"[torrent] Download: %d%%, %v/s, peers: %d",
					int(100*(float64(aggBytesCompleted)/float64(aggLen))),
					//humanize.Bytes(uint64(aggBytesCompleted)),
					//	humanize.Bytes(uint64(aggLen)),
					humanize.Bytes(uint64(aggByteRate)),
					len(peers),
				)
				log.Info(line)
			}
		}
	}()

	torrentClient.WaitAll()
}
