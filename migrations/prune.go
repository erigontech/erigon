package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
)

var storageMode = Migration{
	Name: "storage_mode",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var ( // old db keys
			//StorageModeHistory - does node save history.
			StorageModeHistory = []byte("smHistory")
			//StorageModeReceipts - does node save receipts.
			StorageModeReceipts = []byte("smReceipts")
			//StorageModeTxIndex - does node save transactions index.
			StorageModeTxIndex = []byte("smTxIndex")
			//StorageModeCallTraces - does not build index of call traces
			StorageModeCallTraces = []byte("smCallTraces")
		)
		pm := prune.Mode{Initialised: true}
		castToPruneDistance := func(v []byte) prune.Distance {
			if len(v) == 1 && v[0] == 2 {
				return params.FullImmutabilityThreshold // means, prune enabled
			}
			return math.MaxUint64 // means, prune disabled
		}
		{
			v, err := tx.GetOne(kv.DatabaseInfo, StorageModeHistory)
			if err != nil {
				return err
			}
			if v == nil { // if no records in db - means Erigon just started first time and nothing to migrate. Noop.
				if err := BeforeCommit(tx, nil, true); err != nil {
					return err
				}
				return tx.Commit()
			}
			pm.History = castToPruneDistance(v)
		}
		{
			v, err := tx.GetOne(kv.DatabaseInfo, StorageModeReceipts)
			if err != nil {
				return err
			}
			pm.Receipts = castToPruneDistance(v)
		}
		{
			v, err := tx.GetOne(kv.DatabaseInfo, StorageModeTxIndex)
			if err != nil {
				return err
			}
			pm.TxIndex = castToPruneDistance(v)
		}
		{
			v, err := tx.GetOne(kv.DatabaseInfo, StorageModeCallTraces)
			if err != nil {
				return err
			}
			pm.CallTraces = castToPruneDistance(v)
		}
		{
			v, err := tx.GetOne(kv.DatabaseInfo, kv.StorageModeTEVM)
			if err != nil {
				return err
			}
			pm.Experiments.TEVM = len(v) == 1 && v[0] == 1
		}

		err = prune.SetIfNotExist(tx, pm)
		if err != nil {
			return err
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
