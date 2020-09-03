package torrent

import (
	"context"
	"fmt"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"golang.org/x/sync/errgroup"
	"time"
)

//only for mainnet
const (
	HeadersSnapshotName = "headers"
	BodiesSnapshotName = "bodies"
	StateSnapshotName = "state"
	ReceiptsSnapshotName = "receipts"


	HeadersSnapshotHash = "72a8b7fa55890b1b37b96d91b37adf3689795bf1" //11kk block 1mb chunk
	BlocksSnapshotHash = "0fc6f416651385df347fe05eefae1c26469585a2" //11kk block 1mb chunk
	StateSnapshotHash = ""
	ReceiptsSnapshotHash = ""

)

//var (
//	HeadersSnapshotHash =metainfo.NewHashFromHex(HeadersSnapshotHashHex)
//)



/*
	{
		ID:          stages.DownloadHeadersSnapshot,
		Description: "Download headers snapshot",
		ExecFunc: func(s *StageState, u Unwinder) error {
			return SpawnHeadersSnapshotDownload(s,stateDB,datadir, quitCh )
		},
		UnwindFunc: func(u *UnwindState, s *StageState) error {
			return u.Done(stateDB)
		},
		Disabled: !snapshotMode.Headers,
		DisabledDescription: "Experimental stage",
	},


		{
			ID:          stages.DownloadBodiesSnapshot,
			Description: "Download bodies snapshot",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(stateDB)
			},
			Disabled: !snapshotMode.Bodies,
			DisabledDescription: "Experimental stage",
		},

		{
			ID:          stages.DownloadStateStateSnapshot,
			Description: "Download state snapshot",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(stateDB)
			},
			Disabled: !snapshotMode.State,
		},
		{
			ID:          stages.DownloadReceiptsSnapshot,
			Description: "Download receipts snapshot",
			ExecFunc: func(s *StageState, u Unwinder) error {
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				return u.Done(stateDB)
			},
			Disabled: !snapshotMode.Receipts,
		},

 */




func (c *Client) DownloadHeadersSnapshot(db ethdb.Database) error  {
	pc,err:=storage.NewBoltPieceCompletion(c.datadir+"/pieces/"+ HeadersSnapshotName)
	if err!=nil {
		return err
	}
	infoBytes,err:=db.Get(dbutils.DatabaseInfoBucket, []byte(HeadersSnapshotName+"_info"))
	if err!=nil && err!=ethdb.ErrKeyNotFound {
		return err
	}
	fmt.Println("Info bytes", common.Bytes2Hex(infoBytes))

	t, _, err:=c.cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:    Trackers,
		InfoHash:    metainfo.NewHashFromHex(HeadersSnapshotHash),
		DisplayName: HeadersSnapshotName,
		Storage:     storage.NewFileWithCompletion(c.datadir+"/"+HeadersSnapshotName,pc),
		InfoBytes:   infoBytes,
	})

	if err!=nil {
		return err
	}
	tm:=time.Now()

gi:
	for {
		select {
		case <-t.GotInfo():
			log.Info("Snapshot information collected", "t", time.Since(tm))
			fmt.Println(t.Info().PieceLength)
			fmt.Println(t.Info().Length)
			//fmt.Println(t.Metainfo().)
			break gi
		default:
			log.Info("Collecting snapshot info", "t", time.Since(tm))
			time.Sleep(time.Second*10)
		}
	}
	err=db.Put(dbutils.DatabaseInfoBucket, []byte(HeadersSnapshotName+"_info"), t.Metainfo().InfoBytes)
	if err!=nil {
		return err
	}
	t.AllowDataDownload()
	t.DownloadAll()

	tt2:=time.Now()
dwn:
	for {
		if t.Info().TotalLength()-t.BytesCompleted()==0 {
			log.Info("Dowloaded", "t",time.Since(tt2))
			//fmt.Println("Complete!!!!!!!!!!!!!!!!!!", time.Since(tt2),  t.Info().TotalLength(), t.BytesCompleted())
			break dwn
		} else {
			stats:=t.Stats()
			log.Info("Downloading snapshot", "%", int(100*(float64(t.BytesCompleted())/float64(t.Info().TotalLength()))),  "seeders", stats.ConnectedSeeders)
			time.Sleep(time.Second*10)
		}

	}
	return nil
}



func (c *Client) DownloadBodiesSnapshot(db ethdb.Database) error  {
	pc,err:=storage.NewBoltPieceCompletion(c.datadir+"/pieces/"+ BodiesSnapshotName)
	if err!=nil {
		return err
	}
	infoBytes,err:=db.Get(dbutils.DatabaseInfoBucket, []byte(BodiesSnapshotName+"_info"))
	if err!=nil && err!=ethdb.ErrKeyNotFound {
		return err
	}
	fmt.Println("Info bytes", common.Bytes2Hex(infoBytes))

	t, new, err:=c.cli.AddTorrentSpec(&torrent.TorrentSpec{
		Trackers:    Trackers,
		InfoHash:    metainfo.NewHashFromHex(BlocksSnapshotHash),
		DisplayName: BodiesSnapshotName,
		ChunkSize:   DefaultChunkSize,
		Storage:     storage.NewFileWithCompletion(c.datadir+"/"+BodiesSnapshotName,pc),
		InfoBytes: infoBytes,
	})
	peerID:=c.cli.PeerID()
	fmt.Println(common.Bytes2Hex(peerID[:]),new)
	if err!=nil {
		return err
	}
	tm:=time.Now()

gi:
	for {
		select {
		case <-t.GotInfo():
			fmt.Println("got info!!!!!!!!!!!!",time.Since(tm))
			break gi
		default:
			fmt.Println("Wait get info", time.Since(tm), t.PeerConns())
			time.Sleep(time.Minute)
		}
	}
	err=db.Put(dbutils.DatabaseInfoBucket, []byte(BodiesSnapshotName+"_info"), t.Metainfo().InfoBytes)
	if err!=nil {
		return err
	}

	t.AllowDataDownload()
	for i:=range t.Files() {
		t.Files()[i].Download()
	}

	go func() {
		c.cli.WaitAll()
	}()
	tt2:=time.Now()
dwn:
	for {
		if t.Info().TotalLength()-t.BytesCompleted()==0 {
			fmt.Println("Complete!!!!!!!!!!!!!!!!!!")
			break dwn
		} else {
			fmt.Println(t.BytesMissing(),t.BytesCompleted(), t.Info().TotalLength(), time.Since(tt2), t.PeerConns())
			time.Sleep(time.Second*2)
		}

	}
	return nil
}
