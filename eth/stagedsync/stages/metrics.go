package stages

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/metrics"
)

var SyncStages = map[string]SyncStage{
	"snapshots":             Snapshots,
	"headers":               Headers,
	"bor_heimdall":          BorHeimdall,
	"block_hashes":          BlockHashes,
	"bodies":                Bodies,
	"senders":               Senders,
	"execution":             Execution,
	"translation":           Translation,
	"hash_state":            HashState,
	"intermediate_hashes":   IntermediateHashes,
	"account_history_index": AccountHistoryIndex,
	"storage_history_index": StorageHistoryIndex,
	"log_index":             LogIndex,
	"call_traces":           CallTraces,
	"tx_lookup":             TxLookup,
	"finish":                Finish,
}

var SyncStagesZk = map[string]SyncStage{
	"l1_syncer":                      L1Syncer,
	"l1_sequencer_sender":            L1SequencerSyncer,
	"l1_info_tree":                   L1InfoTree,
	"l1_block_sync":                  L1BlockSync,
	"l1_verification_batch_no":       L1VerificationsBatchNo,
	"l1_sequencer_sync":              L1SequencerSync,
	"verifications_state_root_check": VerificationsStateRootCheck,
	"batches":                        Batches,
	"highest_hashable_l2_block_no":   HighestHashableL2BlockNo,
	"highest_seen_batch_no":          HighestSeenBatchNumber,
	"data_stream":                    DataStream,
	"witness":                        Witness,
	"fork_id":                        ForkId,
	"sequence_executor_verify":       SequenceExecutorVerify,
}

var SyncMetrics = map[SyncStage]metrics.Gauge{}

func init() {
	for i, v := range SyncStages {
		SyncMetrics[v] = metrics.GetOrCreateGauge(fmt.Sprintf(`sync{stage="%s"}`, i))
	}
	for i, v := range SyncStagesZk {
		SyncMetrics[v] = metrics.GetOrCreateGauge(fmt.Sprintf(`zk_sync{zk_stage="%s"}`, i))
	}
}

// UpdateMetrics - need update metrics manually because current "metrics" package doesn't support labels
// need to fix it in future
func UpdateMetrics(tx kv.Tx) error {
	for id, m := range SyncMetrics {
		progress, err := GetStageProgress(tx, id)
		if err != nil {
			return err
		}
		m.SetUint64(progress)
	}
	return nil
}
