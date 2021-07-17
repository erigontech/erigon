package migrations

import (
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
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
			return CommitProgress(db, nil, true)
		}
		// Find the lowest key in the TraceCallSet table
		tx := db.(ethdb.HasTx).Tx().(ethdb.RwTx)
		c, err := tx.CursorDupSort(dbutils.CallTraceSet)
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
			return CommitProgress(db, nil, true)
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum == 0 {
			log.Warn("Nothing to rebuild, CallTraceSet's first record", "number", blockNum)
			return CommitProgress(db, nil, true)
		}
		logPrefix := "db migration rebuild_call_trace_index"
		if err = stagedsync.DoUnwindCallTraces(logPrefix, tx, 999_999_999, blockNum-1, context.Background().Done(), stagedsync.StageCallTracesCfg(nil, 0, tmpdir)); err != nil {
			return err
		}

		log.Info("First record in CallTraceTable", "number", blockNum)
		if err = stages.SaveStageProgress(db, stages.CallTraces, blockNum-1); err != nil {
			return err
		}
		return CommitProgress(db, nil, true)
	},
}
