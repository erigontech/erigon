package migrations

import (
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/log"
)

var rebuilCallTraceIndex = Migration{
	Name: "rebuild_call_trace_index",
	Up: func(db kv.RwKV, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// Find the lowest key in the TraceCallSet table
		c, err := tx.CursorDupSort(kv.CallTraceSet)
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
			return BeforeCommit(tx, nil, true)
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum == 0 {
			log.Warn("Nothing to rebuild, CallTraceSet's first record", "number", blockNum)
			return BeforeCommit(tx, nil, true)
		}
		logPrefix := "db migration rebuild_call_trace_index"

		pm, err := prune.Get(tx)
		if err != nil {
			return err
		}
		if err = stagedsync.DoUnwindCallTraces(logPrefix, tx, 999_999_999, blockNum-1, context.Background(), stagedsync.StageCallTracesCfg(nil, pm, 0, tmpdir)); err != nil {
			return err
		}

		log.Info("First record in CallTraceTable", "number", blockNum)
		if err = stages.SaveStageProgress(tx, stages.CallTraces, blockNum-1); err != nil {
			return err
		}
		if err = BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
