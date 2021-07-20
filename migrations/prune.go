package migrations

import (
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
)

var storageMode = Migration{
	Name: "storage_mode",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
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
			v, err := db.GetOne(dbutils.DatabaseInfoBucket, StorageModeHistory)
			if err != nil {
				return err
			}
			pm.History = castToPruneDistance(v)

		}
		{
			v, err := db.GetOne(dbutils.DatabaseInfoBucket, StorageModeReceipts)
			if err != nil {
				return err
			}
			pm.Receipts = castToPruneDistance(v)
		}
		{
			v, err := db.GetOne(dbutils.DatabaseInfoBucket, StorageModeTxIndex)
			if err != nil {
				return err
			}
			pm.TxIndex = castToPruneDistance(v)
		}
		{
			v, err := db.GetOne(dbutils.DatabaseInfoBucket, StorageModeCallTraces)
			if err != nil {
				return err
			}
			pm.CallTraces = castToPruneDistance(v)
		}
		{
			v, err := db.GetOne(dbutils.DatabaseInfoBucket, dbutils.StorageModeTEVM)
			if err != nil {
				return err
			}
			pm.Experiments.TEVM = len(v) == 1 && v[0] == 1
		}

		err = prune.SetIfNotExist(db, pm)
		if err != nil {
			return err
		}

		return CommitProgress(db, nil, true)
	},
}
