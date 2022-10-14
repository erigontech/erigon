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
}

func init() {
	for _, v := range []stages.SyncStage{
		stages.Snapshots,
		stages.CumulativeIndex,
		stages.BlockHashes,
		stages.Bodies,
		stages.Senders,
		stages.Translation,
		stages.VerkleTrie,
		stages.IntermediateHashes,
		stages.HashState,
		stages.AccountHistoryIndex,
		stages.StorageHistoryIndex,
		stages.LogIndex,
		stages.CallTraces,
		stages.TxLookup,
		stages.Issuance,
	} {
		syncMetrics[v] = metrics.GetOrCreateCounter(
			fmt.Sprintf(
				`sync{stage="%s"}`,
				xstrings.ToSnakeCase(string(v)),
			),
		)
	}
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
