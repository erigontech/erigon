package migrations

import (
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
)

var rebuilCallTraceIndex = Migration{
	Name: "rebuild_call_trace_index",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		sm, err := ethdb.GetStorageModeFromDB(db)
		if err != nil {
			return err
		}
		if !sm.CallTraces {
			// Call traces are not on, nothing to migrate
			return nil
		}
		// Find the lowest key in the TraceCallSet table
		return db.RwKV().Update(context.Background(), func(tx ethdb.RwTx) error {
			c, err := tx.RwCursorDupSort(dbutils.CallTraceSet)
			if err != nil {
				return err
			}
			defer c.Close()
			var k []byte
			k, _, err = c.First()
			if err != nil {
				return err
			}
			if k == nil {
				log.Warn("Nothing to rebuild, CallTraceSet table is empty")
				return nil
			}
			blockNum := binary.BigEndian.Uint64((k))
			if blockNum == 0 {
				log.Warn("Nothing to rebuild, CallTraceSet's first record", "number", blockNum)
				return nil
			}
			log.Info("First record in CallTraceTable", "number", blockNum)
			if err = stages.SaveStageUnwind(tx, stages.CallTraces, blockNum-1); err != nil {
				return err
			}
			return nil
		})
	},
}
