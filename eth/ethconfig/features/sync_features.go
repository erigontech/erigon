package features

import (
	"context"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcfg"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/eth/ethconfig"
)

func EnableSyncCfg(chainDB kv.RoDB, syncCfg ethconfig.Sync) (ethconfig.Sync, error) {
	err := chainDB.View(context.Background(), func(tx kv.Tx) (err error) {
		syncCfg.KeepExecutionProofs, _, err = rawdb.ReadDBCommitmentHistoryEnabled(tx)
		if err != nil {
			return err
		}
		if syncCfg.KeepExecutionProofs {
			state.EnableHistoricalCommitment()
		}
		syncCfg.PersistReceiptsCacheV2, err = kvcfg.PersistReceipts.Enabled(tx)
		if err != nil {
			return err
		}
		if syncCfg.PersistReceiptsCacheV2 {
			state.EnableHistoricalRCache()
		}
		return nil
	})
	return syncCfg, err
}
