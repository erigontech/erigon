package bittorrent

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
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

func (cli *Client) AddTorrentSpec(ctx context.Context, db ethdb.Database, snapshotName string, snapshotHash metainfo.Hash, infoBytes []byte) error {
	boltPath := cli.snapshotsDir + "/pieces/" + snapshotName
	pc, err := storage.NewBoltPieceCompletion(boltPath)
	if err != nil {
		return err
	}

	_, _, err = cli.Cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:    Trackers,
		InfoHash:    snapshotHash,
		DisplayName: snapshotName,
		Storage:     storage.NewFileWithCompletion(cli.snapshotsDir, pc),
		InfoBytes:   infoBytes,
	})
	if err != nil {
		return err
	}
	return nil
}


func (cli *Client) AddTorrent(ctx context.Context,  db ethdb.Database, snapshotType snapshotsync.SnapshotType, networkID uint64) error {
	infoHashBytes, infoBytes, err:=getTorrentSpec(db, snapshotType.String(),networkID)
	if err!=nil {
		return err
	}
	var infoHash metainfo.Hash
	newTorrent := false
	if infoHashBytes!=nil {
		copy(infoHash[:], infoHashBytes[:metainfo.HashSize])
	} else {
		newTorrent=true
		var ok bool
		infoHash,ok= TorrentHashes[networkID][snapshotType]
		if !ok {
			return ErrInvalidSnapshot
		}
	}
	err = cli.AddTorrentSpec(ctx, db, snapshotType.String(), infoHash, infoBytes)
	if err!=nil {
		return fmt.Errorf("error on add snapshot: %w", err)
	}
	infoBytes, err=cli.GetInfoBytes(context.Background(), infoHash)
	if err!=nil {
		log.Warn("Init failure", "snapshot", snapshotType.String(), "err", ctx.Err())
		return fmt.Errorf("error on get info bytes: %w", err)
	}
	log.Info("Init", "snapshot", snapshotType.String())

	if newTorrent {
		log.Info("Save spec", "snapshot", snapshotType.String())
		err := saveTorrentSpec(db, snapshotType.String(), networkID, infoHash, infoBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cli *Client) GetInfoBytes(ctx context.Context, snapshotHash metainfo.Hash) ([]byte, error) {
	t, ok:=cli.Cli.Torrent(snapshotHash)
	if !ok {
		return nil, errors.New("torrent not added")
	}

	select {
	case <-t.GotInfo():
	case <-ctx.Done():
		return nil, fmt.Errorf("add torrent timeout: %w", ctx.Err())
	}
	return common.CopyBytes(t.Metainfo().InfoBytes), nil
}

func (cli *Client) AddSnapshotsTorrents(db ethdb.Database, networkId uint64,  mode snapshotsync.SnapshotMode) error {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Minute*10))
	defer cancel()
	eg := errgroup.Group{}


	if mode.Headers {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, snapshotsync.SnapshotType_headers, networkId)
		})
	}

	if mode.Bodies {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, snapshotsync.SnapshotType_bodies, networkId)
		})
	}

	if mode.State {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, snapshotsync.SnapshotType_state, networkId)
		})
	}

	if mode.Receipts {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, snapshotsync.SnapshotType_receipts, networkId)
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

const (
	SnapshotInfoHashPrefix = "ih"
	SnapshotInfoBytesPrefix = "ib"
)
func getTorrentSpec(db ethdb.Database, snapshotName string, networkID uint64) ([]byte, []byte, error) {
	var infohash, infobytes []byte
	var err error
	b:=make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	infohash, err = db.Get(dbutils.SnapshotInfoBucket, MakeInfoHashKey(snapshotName, networkID))
	if err!=nil && !errors.Is(err,ethdb.ErrKeyNotFound) {
		return nil, nil, err
	}
	infobytes, err = db.Get(dbutils.SnapshotInfoBucket, MakeInfoBytesKey(snapshotName, networkID))
	if err!=nil && !errors.Is(err,ethdb.ErrKeyNotFound) {
		return nil, nil, err
	}

	return infohash, infobytes, nil
}
func saveTorrentSpec(db ethdb.Database, snapshotName string, networkID uint64, hash torrent.InfoHash, infobytes []byte) error {
	b:=make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	err:=db.Put(dbutils.SnapshotInfoBucket, MakeInfoHashKey(snapshotName, networkID), hash.Bytes())
	if err!=nil {
		return err
	}
	return db.Put(dbutils.SnapshotInfoBucket, MakeInfoBytesKey(snapshotName, networkID), infobytes)
}

func MakeInfoHashKey(snapshotName string, networkID uint64) []byte {
	b:=make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	return append(b, []byte(SnapshotInfoHashPrefix+snapshotName)...)
}

func MakeInfoBytesKey(snapshotName string, networkID uint64) []byte {
	b:=make([]byte, 8)
	binary.BigEndian.PutUint64(b, networkID)
	return append(b, []byte(SnapshotInfoBytesPrefix+snapshotName)...)
}

