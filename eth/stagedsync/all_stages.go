package stagedsync

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

// UpdateMetrics - need update metrics manually because current "metrics" package doesn't support labels
// need to fix it in future
func UpdateMetrics(tx kv.Tx) error {
	var progress uint64
	var err error
	progress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	stageHeadersGauge.Set(progress)

	progress, err = stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	stageExecutionGauge.Set(progress)

	return nil
}
