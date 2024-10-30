package migrations

import (
	"context"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	reset2 "github.com/erigontech/erigon/v3/core/rawdb/rawdbreset"
)

var ClearBorTables = Migration{
	// migration required due to change of `BorEventNums` to last event ID (https://github.com/erigontech/erigon/v3/commit/13b4b7768485736e54ff5ca3270ebeec5c023ba8)
	Name: "clear_bor_tables",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}

		if err := reset2.ResetBorHeimdall(context.Background(), tx); err != nil {
			return err
		}

		return tx.Commit()
	},
}
