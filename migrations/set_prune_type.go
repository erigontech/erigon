package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
)

var setPruneType = Migration{
	Name: "set_prune_type",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		var pruneTypeKeys = [4][]byte{kv.PruneHistoryType, kv.PruneReceiptsType, kv.PruneTxIndexType, kv.PruneCallTracesType}

		for _, key := range pruneTypeKeys {
			pruneType, getErr := tx.GetOne(kv.DatabaseInfo, key)
			if getErr != nil {
				return getErr
			}

			if pruneType != nil {
				continue
			}

			putErr := tx.Put(kv.DatabaseInfo, key, kv.PruneTypeOlder)
			if putErr != nil {
				return putErr
			}
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
