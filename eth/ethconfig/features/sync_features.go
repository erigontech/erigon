package features

import (
	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcfg"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/eth/ethconfig"
)

func EnableSyncCfg(tx kv.Tx, syncCfg ethconfig.Sync) (ethconfig.Sync, error) {
	var err error
	syncCfg.KeepExecutionProofs, _, err = rawdb.ReadDBCommitmentHistoryEnabled(tx)
	if err != nil {
		return syncCfg, err
	}
	if syncCfg.KeepExecutionProofs {
		state.EnableHistoricalCommitment()
	}
	syncCfg.PersistReceiptsCacheV2, err = kvcfg.PersistReceipts.Enabled(tx)
	if err != nil {
		return syncCfg, err
	}
	if syncCfg.PersistReceiptsCacheV2 {
		state.EnableHistoricalRCache()
	}
	return syncCfg, nil
}
