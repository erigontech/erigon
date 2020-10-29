package bittorrent

import (
	"context"
	"errors"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"golang.org/x/sync/errgroup"
	"time"
)

type Client struct {
	Cli          *torrent.Client
	snapshotsDir string
}

func New(snapshotsDir string, seeding bool) *Client {
	torrentConfig:=DefaultTorrentConfig()
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir

	torrentClient, err := torrent.NewClient(torrentConfig)
	if err != nil {
		log.Error("Fail to start torrnet client", "err", err)
	}

	return &Client{
		Cli:          torrentClient,
		snapshotsDir: snapshotsDir,
	}
}


func DefaultTorrentConfig() *torrent.ClientConfig {
	torrentConfig := torrent.NewDefaultClientConfig()
	torrentConfig.ListenPort = 0
	torrentConfig.NoDHT = true
	torrentConfig.DisableTrackers = false
	torrentConfig.Debug = false
	torrentConfig.Logger = NewLogger()
	return torrentConfig
}

func (cli *Client) AddTorrent(ctx context.Context, db ethdb.Database, snapshotName string, snapshotHash metainfo.Hash) error {
	boltPath := cli.snapshotsDir + "/pieces/" + snapshotName
	pc, err := storage.NewBoltPieceCompletion(boltPath)
	if err != nil {
		return err
	}

	newTorrent := false
	ts, err := getTorrentSpec(db, []byte(snapshotName))
	if errors.Is(err, ethdb.ErrKeyNotFound) {
		log.Info("Uninited snapshot", "snapshot", snapshotName)
		newTorrent = true
		ts = torrentSpec{
			InfoHash: snapshotHash,
		}
	} else if err != nil {
		return err
	}

	t, _, err := cli.Cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:    Trackers,
		InfoHash:    ts.InfoHash,
		DisplayName: snapshotName,
		Storage:     storage.NewFileWithCompletion(cli.snapshotsDir, pc),
		InfoBytes:   ts.InfoBytes,
	})
	if err != nil {
		return err
	}

	select {
	case <-t.GotInfo():
		log.Info("Init", "snapshot", snapshotName)
		if newTorrent {
			log.Info("Save spec", "snapshot", snapshotName)
			ts.InfoBytes = common.CopyBytes(t.Metainfo().InfoBytes)
			err = saveTorrentSpec(db, []byte(snapshotName), ts)
			if err != nil {
				return err
			}
		} else {
			log.Info("Loaded from db", "snapshot", snapshotName)
		}

	case <-ctx.Done():
		log.Warn("Init failure", "snapshot", snapshotName, "err", ctx.Err())
		return errors.New("add torrent timeout")
	}
	return nil
}

func (cli *Client) AddSnapshotsTorrens(db ethdb.Database, networkId uint64,  mode snapshotsync.SnapshotMode) error {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Minute*10))
	defer cancel()
	eg := errgroup.Group{}

	if mode.Headers {
		eg.Go(func() error {
			hash,ok:= TorrentHashes[networkId][snapshotsync.HeadersSnapshotName]
			if !ok {
				return ErrInvalidSnapshot
			}
			return cli.AddTorrent(ctx, db, snapshotsync.HeadersSnapshotName, hash)
		})
	}
	if mode.Bodies {
		eg.Go(func() error {
			hash,ok:= TorrentHashes[networkId][snapshotsync.BodiesSnapshotName]
			if !ok {
				return ErrInvalidSnapshot
			}

			return cli.AddTorrent(ctx, db, snapshotsync.BodiesSnapshotName, hash)
		})
	}
	if mode.State {
		eg.Go(func() error {
			hash,ok:= TorrentHashes[networkId][snapshotsync.StateSnapshotName]
			if !ok {
				return ErrInvalidSnapshot
			}

			return cli.AddTorrent(ctx, db, snapshotsync.StateSnapshotName, hash)
		})
	}
	if mode.Receipts {
		eg.Go(func() error {
			hash,ok:= TorrentHashes[networkId][snapshotsync.ReceiptsSnapshotName]
			if !ok {
				return ErrInvalidSnapshot
			}

			return cli.AddTorrent(ctx, db, snapshotsync.ReceiptsSnapshotName, hash)
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (cli *Client) Download()  {
	log.Info("Start snapshot downloading")
	torrents := cli.Cli.Torrents()
	for i := range torrents {
		t := torrents[i]
		go func(t *torrent.Torrent) {
			t.AllowDataDownload()
			t.DownloadAll()

			tt := time.Now()
		dwn:
			for {
				if t.Info().TotalLength()-t.BytesCompleted() == 0 {
					log.Info("Dowloaded", "snapshot", t.Name(), "t", time.Since(tt))
					break dwn
				} else {
					stats := t.Stats()
					log.Info("Downloading snapshot", "snapshot", t.Name(), "%", int(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))), "seeders", stats.ConnectedSeeders)
					time.Sleep(time.Minute)
				}

			}
		}(t)
	}
	cli.Cli.WaitAll()

	for _, t := range cli.Cli.Torrents() {
		log.Info("Snapshot seeding", "name", t.Name(), "seeding", t.Seeding())
	}
}

type torrentSpec struct {
	InfoHash  metainfo.Hash
	InfoBytes []byte
}

func getTorrentSpec(db ethdb.Database, key []byte) (torrentSpec, error) {
	v, err := db.Get(dbutils.SnapshotInfoBucket, key)
	if err != nil {
		return torrentSpec{}, err
	}
	ts := torrentSpec{}
	err = rlp.DecodeBytes(v, &ts)
	return ts, err
}
func saveTorrentSpec(db ethdb.Database, key []byte, ts torrentSpec) error { //nolint
	v, err := rlp.EncodeToBytes(ts)
	if err != nil {
		return err
	}
	return db.Put(dbutils.SnapshotInfoBucket, key, v)
}

