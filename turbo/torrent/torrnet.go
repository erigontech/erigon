package torrent

import (
	"context"
	"fmt"
	lg "github.com/anacrolix/log"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"golang.org/x/sync/errgroup"
	"time"
)

const (
	DefaultChunkSize = 1024  * 1024
	LmdbFilename = "data.mdb"
)

func New(snapshotsDir string, snapshotMode SnapshotMode, seeding bool) *Client {
	torrentConfig :=torrent.NewDefaultClientConfig()
	torrentConfig.ListenPort=0
	torrentConfig.Seed = seeding
	torrentConfig.DataDir = snapshotsDir
	//torrentConfig.NoDHT = true
	torrentConfig.DisableTrackers = false
	torrentConfig.Debug=false
	torrentConfig.Logger = torrentConfig.Logger.FilterLevel(lg.Error)
	torrentClient, err := torrent.NewClient(torrentConfig)
	if err!=nil {
		log.Error("Fail to start torrnet client", "err",err)
	}
	fmt.Println(snapshotsDir)
	return &Client{
		cli:          torrentClient,
		snMode:       snapshotMode,
		snapshotsDir: snapshotsDir,
	}
}

type Client struct {
	cli          *torrent.Client
	snMode       SnapshotMode
	snapshotsDir string
}

func (cli *Client) AddTorrent(ctx context.Context, db ethdb.Database, snapshotName, snapshotHash string) error  {
	boltPath:=cli.snapshotsDir +"/pieces/"+snapshotName
	pc,err:=storage.NewBoltPieceCompletion(boltPath)
	if err!=nil {
		return err
	}

	newTorrent :=false
	ts,err:= getTorrentSpec(db, []byte(snapshotName))
	if err==ethdb.ErrKeyNotFound {
		log.Info("Uninited snapshot", "snapshot", snapshotName)
		newTorrent = true
		ts = torrentSpec{
			InfoHash:  metainfo.NewHashFromHex(snapshotHash),
		}
	} else if err!=nil {
		return err
	}

	t, _, err:=cli.cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:    Trackers,
		InfoHash:    ts.InfoHash,
		DisplayName: snapshotName,
		Storage:     storage.NewFileWithCompletion(cli.snapshotsDir,pc),
		InfoBytes: 	 ts.InfoBytes,
	})
	if err!=nil {
		return err
	}

	select {
		case <-t.GotInfo():
			log.Info("Init", "snapshot", snapshotName)
			if newTorrent {
				log.Info("Save spec", "snapshot", snapshotName)
				ts.InfoBytes=common.CopyBytes(t.Metainfo().InfoBytes)
				err=saveTorrentSpec(db, []byte(snapshotName), ts)
				if err!=nil {
					return err
				}
			} else {
				log.Info("Loaded from db", "snapshot", snapshotName)
			}

		case <-ctx.Done():
			log.Warn("Init failure", "snapshot", snapshotName, "err", ctx.Err())
			return ctx.Err()
	}
	//t.VerifyData()
	t.DisallowDataDownload()
	return nil
}


func (cli *Client) Run(db ethdb.Database) error  {
	ctx:=context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Minute*10))
	defer cancel()
	eg:=errgroup.Group{}
	//todo remove
	//db.Delete(dbutils.SnapshotInfoBucket, []byte(HeadersSnapshotName))
	//db.Delete(dbutils.SnapshotInfoBucket, []byte(BodiesSnapshotName))

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
	err:=eg.Wait()
	if err!=nil {
		return err
	}

	for i:=range cli.cli.Torrents() {
		go func(t *torrent.Torrent) {
			t.AllowDataDownload()
			t.DownloadAll()

			tt:=time.Now()
		dwn:
			for {
				if t.Info().TotalLength()-t.BytesCompleted()==0 {
					fmt.Println("Downloaded", t.Name())
					log.Info("Dowloaded", "snapshot", t.Name(), "t",time.Since(tt))
					break dwn
				} else {
					stats:=t.Stats()
					log.Info("Downloading snapshot", "snapshot", t.Name(),"%", int(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))),  "seeders", stats.ConnectedSeeders)
					time.Sleep(time.Minute)
				}

			}
		}(cli.cli.Torrents()[i])
	}
	cli.cli.WaitAll()

	for _,t:=range cli.cli.Torrents() {
		log.Info("Snapshot seeding", "name", t.Name(), "seeding", t.Seeding())
	}

	return nil
}


type torrentSpec struct {
	InfoHash metainfo.Hash
	InfoBytes []byte
}

func getTorrentSpec(db ethdb.Database, key []byte) (torrentSpec, error) {
	v,err:=db.Get(dbutils.SnapshotInfoBucket, key)
	if err!=nil {
		return torrentSpec{}, err
	}
	ts:=torrentSpec{}
	err = rlp.DecodeBytes(v, &ts)
	return ts, err
}
func saveTorrentSpec(db ethdb.Database, key []byte, ts torrentSpec) error {
	v,err:=rlp.EncodeToBytes(ts)
	if err!=nil {
		return err
	}
	return db.Put(dbutils.SnapshotInfoBucket, key, v)
}




//omplete!!!!!!!!!!!!!!!!!!
//--- PASS: TestTorrentBodies (15997.60s)
