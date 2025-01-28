package stages

import (
	"context"
	"fmt"
	"github.com/huandu/xstrings"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/metrics"
	stages2 "github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/sequencer"
)

var ZkSequencerStagesMetrics = []stages2.SyncStage{
	stages2.L1Syncer,
	stages2.L1SequencerSyncer,
	stages2.L1InfoTree,
	stages2.L1BlockSync,
	stages2.Execution,
	stages2.HighestHashableL2BlockNo,
	stages2.HighestSeenBatchNumber,
}

var ZkDefaultStagesMetrics = []stages2.SyncStage{
	stages2.L1Syncer,
	stages2.L1InfoTree,
	stages2.Batches,
	stages2.BlockHashes,
	stages2.Senders,
	stages2.Execution,
	stages2.DataStream,
	stages2.Witness,
}

var ZkSyncMetrics = map[stages2.SyncStage]metrics.Gauge{}

func init() {
	if sequencer.IsSequencer() {
		for _, v := range ZkSequencerStagesMetrics {
			ZkSyncMetrics[v] = metrics.GetOrCreateGauge(
				fmt.Sprintf(
					`zk_sync_sequencer{zk_stage="%s"}`,
					xstrings.ToSnakeCase(string(v)),
				),
			)
		}
	} else {
		for _, v := range ZkDefaultStagesMetrics {
			ZkSyncMetrics[v] = metrics.GetOrCreateGauge(
				fmt.Sprintf(
					`zk_sync_default{zk_stage="%s"}`,
					xstrings.ToSnakeCase(string(v)),
				),
			)
		}
	}
}

func UpdateZkSyncMetrics(ctx context.Context, db kv.RwDB) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return fmt.Errorf("db.BeginRo: %w", err)
	}
	defer tx.Rollback()
	for id, m := range ZkSyncMetrics {
		progress, err := stages2.GetStageProgress(tx, id)
		if err != nil {
			return err
		}
		m.SetUint64(progress)
	}
	return nil
}
