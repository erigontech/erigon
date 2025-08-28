package features

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/eth/ethconfig"
)

func EnableSyncCfg(chainDB kv.RoDB, syncCfg ethconfig.Sync) (ethconfig.Sync, error) {
	err := chainDB.View(context.Background(), func(tx kv.Tx) (err error) {
		syncCfg.KeepExecutionProofs, _, err = rawdb.ReadDBCommitmentHistoryEnabled(tx)
		if err != nil {
			return err
		}
		if syncCfg.KeepExecutionProofs {
			statecfg.EnableHistoricalCommitment()
		}
		syncCfg.PersistReceiptsCacheV2, err = kvcfg.PersistReceipts.Enabled(tx)
		if err != nil {
			return err
		}
		if syncCfg.PersistReceiptsCacheV2 {
			statecfg.EnableHistoricalRCache()
		}
		return nil
	})
	return syncCfg, err
}
