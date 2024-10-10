package migrations

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

var countersToArray = Migration{
	Name: "migrate counters from map to array",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		// Migrate counters from map to array with cursor over BATCH_COUNTERS
		c, err := tx.Cursor(kv.BATCH_COUNTERS)
		if err != nil {
			return err
		}
		defer c.Close()
		tx.ForEach(kv.BATCH_COUNTERS, []byte{}, func(k, v []byte) error {
			var countersMap map[string]int
			if err := json.Unmarshal(v, &countersMap); err != nil {
				// not a map, so pass on
				return nil
			}

			countersArray := make([]int, vm.CounterTypesCount)
			for counterKey, counterCount := range countersMap {
				keyName := string(counterKey)
				keyIndex := -1
				for i, kn := range vm.CounterKeyNames {
					if string(kn) == keyName {
						keyIndex = i
						break
					}
				}

				if keyIndex == -1 {
					blockNo := hermez_db.BytesToUint64(k)
					return fmt.Errorf("unknown counter key %s for block %d", keyName, blockNo)
				}
				countersArray[keyIndex] = counterCount
			}

			countersArrayBytes, err := json.Marshal(countersArray)
			if err != nil {
				return err
			}
			newKey := make([]byte, len(k))
			copy(newKey, k)
			if err := tx.Put(kv.BATCH_COUNTERS, newKey, countersArrayBytes); err != nil {
				return err
			}

			return nil
		})

		// This migration is no-op, but it forces the migration mechanism to apply it and thus write the DB schema version info
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
