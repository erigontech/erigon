package rawdbreset

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func ResetState(db kv.RwDB, ctx context.Context, chain string) error {
	// don't reset senders here
	if err := db.Update(ctx, stagedsync.ResetHashState); err != nil {
		return err
	}
	if err := db.Update(ctx, stagedsync.ResetIH); err != nil {
		return err
	}
	if err := db.Update(ctx, ResetHistory); err != nil {
		return err
	}
	if err := db.Update(ctx, ResetLogIndex); err != nil {
		return err
	}
	if err := db.Update(ctx, ResetCallTraces); err != nil {
		return err
	}
	if err := db.Update(ctx, ResetTxLookup); err != nil {
		return err
	}
	if err := db.Update(ctx, ResetFinish); err != nil {
		return err
	}

	if err := db.Update(ctx, func(tx kv.RwTx) error { return ResetExec(tx, chain) }); err != nil {
		return err
	}
	return nil
}

func ResetBlocks(tx kv.RwTx) error {
	// keep Genesis
	if err := rawdb.TruncateBlocks(context.Background(), tx, 1); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, 1); err != nil {
		return fmt.Errorf("saving Bodies progress failed: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, 1); err != nil {
		return fmt.Errorf("saving Bodies progress failed: %w", err)
	}

	// remove all canonical markers from this point
	if err := rawdb.TruncateCanonicalHash(tx, 1, false); err != nil {
		return err
	}
	if err := rawdb.TruncateTd(tx, 1); err != nil {
		return err
	}
	hash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return err
	}
	if err = rawdb.WriteHeadHeaderHash(tx, hash); err != nil {
		return err
	}

	// ensure no garbage records left (it may happen if db is inconsistent)
	if err := tx.ForEach(kv.BlockBody, dbutils.EncodeBlockNumber(2), func(k, _ []byte) error { return tx.Delete(kv.BlockBody, k) }); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.NonCanonicalTxs); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.EthTx); err != nil {
		return err
	}
	if err := rawdb.ResetSequence(tx, kv.EthTx, 0); err != nil {
		return err
	}
	if err := rawdb.ResetSequence(tx, kv.NonCanonicalTxs, 0); err != nil {
		return err
	}

	return nil
}
func ResetSenders(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.Senders); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Senders, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.Senders, 0); err != nil {
		return err
	}
	return nil
}

func ResetExec(tx kv.RwTx, chain string) (err error) {
	if err = stages.SaveStageProgress(tx, stages.Execution, 0); err != nil {
		return err
	}
	if err = stages.SaveStagePruneProgress(tx, stages.Execution, 0); err != nil {
		return err
	}
	if err = stages.SaveStageProgress(tx, stages.HashState, 0); err != nil {
		return err
	}
	if err = stages.SaveStagePruneProgress(tx, stages.HashState, 0); err != nil {
		return err
	}
	if err = stages.SaveStageProgress(tx, stages.IntermediateHashes, 0); err != nil {
		return err
	}
	if err = stages.SaveStagePruneProgress(tx, stages.IntermediateHashes, 0); err != nil {
		return err
	}

	stateBuckets := []string{
		kv.PlainState, kv.HashedAccounts, kv.HashedStorage, kv.TrieOfAccounts, kv.TrieOfStorage,
		kv.Epoch, kv.PendingEpoch, kv.BorReceipts,
		kv.Code, kv.PlainContractCode, kv.ContractCode, kv.IncarnationMap,
	}
	for _, b := range stateBuckets {
		if err := tx.ClearBucket(b); err != nil {
			return err
		}
	}

	historyV2, err := rawdb.HistoryV2.Enabled(tx)
	if err != nil {
		return err
	}
	if historyV2 {
		buckets := []string{
			kv.AccountHistoryKeys, kv.AccountIdx, kv.AccountHistoryVals, kv.AccountSettings,
			kv.StorageKeys, kv.StorageVals, kv.StorageHistoryKeys, kv.StorageHistoryVals, kv.StorageSettings, kv.StorageIdx,
			kv.CodeKeys, kv.CodeVals, kv.CodeHistoryKeys, kv.CodeHistoryVals, kv.CodeSettings, kv.CodeIdx,
			kv.AccountHistoryKeys, kv.AccountIdx, kv.AccountHistoryVals, kv.AccountSettings,
			kv.StorageHistoryKeys, kv.StorageIdx, kv.StorageHistoryVals, kv.StorageSettings,
			kv.CodeHistoryKeys, kv.CodeIdx, kv.CodeHistoryVals, kv.CodeSettings,
			kv.LogAddressKeys, kv.LogAddressIdx,
			kv.LogTopicsKeys, kv.LogTopicsIdx,
			kv.TracesFromKeys, kv.TracesFromIdx,
			kv.TracesToKeys, kv.TracesToIdx,
		}
		for _, b := range buckets {
			if err := tx.ClearBucket(b); err != nil {
				return err
			}
		}
	} else {
		if err := tx.ClearBucket(kv.AccountChangeSet); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.StorageChangeSet); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.Receipts); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.Log); err != nil {
			return err
		}
		if err := tx.ClearBucket(kv.CallTraceSet); err != nil {
			return err
		}

		genesis := core.DefaultGenesisBlockByChainName(chain)
		if _, _, err := genesis.WriteGenesisState(tx); err != nil {
			return err
		}
	}

	return nil
}

func ResetHistory(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.AccountsHistory); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.StorageHistory); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.AccountHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.StorageHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.AccountHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.StorageHistoryIndex, 0); err != nil {
		return err
	}

	return nil
}

func ResetLogIndex(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.LogAddressIndex); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.LogTopicIndex); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.LogIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.LogIndex, 0); err != nil {
		return err
	}
	return nil
}

func ResetCallTraces(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.CallFromIndex); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.CallToIndex); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.CallTraces, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.CallTraces, 0); err != nil {
		return err
	}
	return nil
}

func ResetTxLookup(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.TxLookup); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	return nil
}

func ResetFinish(tx kv.RwTx) error {
	if err := stages.SaveStageProgress(tx, stages.Finish, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.Finish, 0); err != nil {
		return err
	}
	return nil
}
