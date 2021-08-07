package stagedsync

import (
	"github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

var syncMetrics = map[stages.SyncStage]*metrics.Counter{
	stages.Headers:   metrics.GetOrCreateCounter(`sync{stage="headers"}`),
	stages.Execution: metrics.GetOrCreateCounter(`sync{stage="execution"}`),
	stages.Finish:    metrics.GetOrCreateCounter(`sync{stage="finish"}`),
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
