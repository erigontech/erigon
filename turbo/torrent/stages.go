package torrent

import (
	"context"
	"errors"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"golang.org/x/sync/errgroup"
	"time"
)

//only for mainnet
const (
	HeadersSnapshotName = "headers"
	BodiesSnapshotName = "bodies"
	StateSnapshotName = "state"
	ReceiptsSnapshotName = "receipts"


	HeadersSnapshotHash = "7f50f7458715169d98e6dc2f02e2bf52098a3307" //11kk block 1mb chunk
	BlocksSnapshotHash = "0fc6f416651385df347fe05eefae1c26469585a2" //11kk block 1mb chunk
	StateSnapshotHash = ""
	ReceiptsSnapshotHash = ""

)

//var (
//	HeadersSnapshotHash =metainfo.NewHashFromHex(HeadersSnapshotHashHex)
//)



func (cli *Client) Run(db ethdb.Database) error  {
	ctx:=context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Minute*10))
	defer cancel()
	eg:=errgroup.Group{}
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
	t,ok:=cli.cli.Torrent(metainfo.NewHashFromHex(HeadersSnapshotHash))
	if !ok {
		return errors.New("uninited torrent "+HeadersSnapshotName)
	}
	t.Files()


	return nil
}

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