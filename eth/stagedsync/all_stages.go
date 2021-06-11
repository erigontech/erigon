package stagedsync

import (
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
)

// UpdateMetrics - need update metrics manually because current "metrics" package doesn't support labels
// need to fix it in future
func UpdateMetrics(tx ethdb.Tx) error {
	var progress uint64
	var err error
	progress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	stageHeadersGauge.Update(int64(progress))

	progress, err = stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return err
	}
	stageBodiesGauge.Update(int64(progress))

	progress, err = stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	stageExecutionGauge.Update(int64(progress))

	progress, err = stages.GetStageProgress(tx, stages.Translation)
	if err != nil {
		return err
	}
	stageTranspileGauge.Update(int64(progress))
	return nil
}
