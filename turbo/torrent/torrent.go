package torrent

import (
	"context"
	"errors"
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"io"
	"os"
	"path/filepath"
	"strings"

	//"github.com/anacrolix/torrent/storage"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"golang.org/x/sync/errgroup"
	"time"
)

const (
	DefaultChunkSize = 1024 * 1024
	LmdbFilename     = "data.mdb"
)

func New(snapshotsDir string, snapshotMode SnapshotMode, seeding bool) *Client {
	torrentConfig := torrent.NewDefaultClientConfig()
	torrentConfig.ListenPort = 0
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	torrentConfig.NoDHT = true
	torrentConfig.DisableTrackers = false
	torrentConfig.Debug = false
	torrentConfig.Logger = NewLogger()
	torrentClient, err := torrent.NewClient(torrentConfig)
	if err != nil {
		log.Error("Fail to start torrnet client", "err", err)
	}

	return &Client{
		Cli:          torrentClient,
		snMode:       snapshotMode,
		snapshotsDir: snapshotsDir,
	}
}

type Client struct {
	Cli          *torrent.Client
	snMode       SnapshotMode
	snapshotsDir string
}

func (cli *Client) AddTorrent(ctx context.Context, db ethdb.Database, snapshotName, snapshotHash string) error {
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
			InfoHash: metainfo.NewHashFromHex(snapshotHash),
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
		return ctx.Err()
	}
	t.VerifyData()
	t.DisallowDataDownload()
	return nil
}

func (cli *Client) Run(db ethdb.Database) error {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Minute*10))
	defer cancel()
	eg := errgroup.Group{}

	if cli.snMode.Headers {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, HeadersSnapshotName, HeadersSnapshotHash)
		})
	}
	if cli.snMode.Bodies {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, BodiesSnapshotName, BlocksSnapshotHash)
		})
	}
	if cli.snMode.State {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, StateSnapshotName, StateSnapshotHash)
		})
	}
	if cli.snMode.Receipts {
		eg.Go(func() error {
			return cli.AddTorrent(ctx, db, ReceiptsSnapshotName, ReceiptsSnapshotHash)
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}

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

	return nil
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

func WrapBySnapshots(kv ethdb.KV, snapshotDir string, mode SnapshotMode) (ethdb.KV, error) {
	if mode.Bodies {
		snapshotKV, err := ethdb.NewLMDB().Path(snapshotDir + "/bodies").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.BlockBodyPrefix:    dbutils.BucketConfigItem{},
				dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
			}
		}).ReadOnly().Open()
		if err != nil {
			log.Error("Can't open body snapshot", "err", err)
			return nil, err
		} else { //nolint
			kv = ethdb.NewSnapshotKV().SnapshotDB(snapshotKV).
				For(dbutils.BlockBodyPrefix).
				For(dbutils.SnapshotInfoBucket).
				DB(kv).MustOpen()
		}
	}

	if mode.Headers {
		snapshotKV, err := ethdb.NewLMDB().Path(snapshotDir + "/headers").WithBucketsConfig(func(defaultBuckets dbutils.BucketsCfg) dbutils.BucketsCfg {
			return dbutils.BucketsCfg{
				dbutils.HeaderPrefix:       dbutils.BucketConfigItem{},
				dbutils.SnapshotInfoBucket: dbutils.BucketConfigItem{},
				dbutils.HeadHeaderKey:      dbutils.BucketConfigItem{},
			}
		}).ReadOnly().Open()
		if err != nil {
			log.Error("Can't open headers snapshot", "err", err)
			return nil, err
		} else { //nolint
			kv = ethdb.NewSnapshotKV().SnapshotDB(snapshotKV).
				For(dbutils.HeaderPrefix).
				For(dbutils.SnapshotInfoBucket).
				DB(kv).MustOpen()
		}
	}
	return kv, nil
}

func BuildInfoBytesForLMDBSnapshot(root string) (metainfo.Info, error) {
	path := root + "/" + LmdbFilename
	fi, err := os.Stat(path)
	if err != nil {
		return metainfo.Info{}, err
	}
	relPath, err := filepath.Rel(root, path)
	if err != nil {
		return metainfo.Info{}, fmt.Errorf("error getting relative path: %s", err)
	}

	info := metainfo.Info{
		Name:        filepath.Base(root),
		PieceLength: DefaultChunkSize,
		Length:      fi.Size(),
		Files: []metainfo.FileInfo{
			{
				Length:   fi.Size(),
				Path:     []string{relPath},
				PathUTF8: nil,
			},
		},
	}

	err = info.GeneratePieces(func(fi metainfo.FileInfo) (io.ReadCloser, error) {
		fmt.Println("info.GeneratePieces", filepath.Join(root, strings.Join(fi.Path, string(filepath.Separator))))
		return os.Open(filepath.Join(root, strings.Join(fi.Path, string(filepath.Separator))))
	})
	if err != nil {
		err = fmt.Errorf("error generating pieces: %s", err)
		return metainfo.Info{}, err
	}
	return info, nil
}
