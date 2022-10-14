package stagedsync

import (
	"fmt"

	"github.com/VictoriaMetrics/metrics"
	"github.com/huandu/xstrings"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

var syncMetrics = map[stages.SyncStage]*metrics.Counter{
	stages.Headers:   metrics.GetOrCreateCounter(`sync{stage="headers"}`),
	stages.Execution: metrics.GetOrCreateCounter(`sync{stage="execution"}`),
	stages.Finish:    metrics.GetOrCreateCounter(`sync{stage="finish"}`),
	// additional
	stages.Snapshots:           makeSyncMetric(stages.Snapshots),
	stages.CumulativeIndex:     makeSyncMetric(stages.CumulativeIndex),
	stages.BlockHashes:         makeSyncMetric(stages.BlockHashes),
	stages.Bodies:              makeSyncMetric(stages.Bodies),
	stages.Senders:             makeSyncMetric(stages.Senders),
	stages.Translation:         makeSyncMetric(stages.Translation),
	stages.VerkleTrie:          makeSyncMetric(stages.VerkleTrie),
	stages.IntermediateHashes:  makeSyncMetric(stages.IntermediateHashes),
	stages.HashState:           makeSyncMetric(stages.HashState),
	stages.AccountHistoryIndex: makeSyncMetric(stages.AccountHistoryIndex),
	stages.StorageHistoryIndex: makeSyncMetric(stages.StorageHistoryIndex),
	stages.LogIndex:            makeSyncMetric(stages.LogIndex),
	stages.CallTraces:          makeSyncMetric(stages.CallTraces),
	stages.TxLookup:            makeSyncMetric(stages.TxLookup),
	stages.Issuance:            makeSyncMetric(stages.Issuance),
}

func makeSyncMetric(stage stages.SyncStage) *metrics.Counter {
	return metrics.GetOrCreateCounter(fmt.Sprintf(`sync{stage="%s"}`, xstrings.ToSnakeCase(string(stage))))
}

// UpdateMetrics - need update metrics manually because current "metrics" package doesn't support labels
// need to fix it in future
func UpdateMetrics(tx kv.Tx) error {
	for id, m := range syncMetrics {
		progress, err := stages.GetStageProgress(tx, id)
		if err != nil {
			return err
		}
		m.Set(progress)
	}
	return nil
}
