package migrations

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

var ErrTxsBeginEndNoMigration = fmt.Errorf("in this Erigon version DB format was changed: added additional first/last system-txs to blocks. There is no DB migration for this change. Please re-sync or switch to earlier version")

var txsBeginEnd = Migration{
	Name: "txs_begin_end",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		var latestBlock uint64
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			latestBlock, err = stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		if latestBlock > 0 {
			return ErrTxsBeginEndNoMigration
		}
		return db.Update(context.Background(), func(tx kv.RwTx) error {
			return BeforeCommit(tx, nil, true)
		})
	},
}
