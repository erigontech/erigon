package stages

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

func resequence(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	ctx context.Context,
	cfg SequenceBlockCfg,
	historyCfg stagedsync.HistoryCfg,
	lastBatch, highestBatchInDs uint64,
) (err error) {
	if !cfg.zk.SequencerResequence {
		panic(fmt.Sprintf("[%s] The node need re-sequencing but this option is disabled.", s.LogPrefix()))
	}

	log.Info(fmt.Sprintf("[%s] Last batch %d is lower than highest batch in datastream %d, resequencing...", s.LogPrefix(), lastBatch, highestBatchInDs))

	batches, err := cfg.datastreamServer.ReadBatches(lastBatch+1, highestBatchInDs)
	if err != nil {
		return err
	}

	if err = cfg.datastreamServer.UnwindToBatchStart(lastBatch + 1); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Resequence from batch %d to %d in data stream", s.LogPrefix(), lastBatch+1, highestBatchInDs))
	for _, batch := range batches {
		batchJob := NewResequenceBatchJob(batch)
		subBatchCount := 0
		for batchJob.HasMoreBlockToProcess() {
			if err = sequencingBatchStep(s, u, ctx, cfg, historyCfg, batchJob); err != nil {
				return err
			}

			subBatchCount += 1
		}

		log.Info(fmt.Sprintf("[%s] Resequenced original batch %d with %d batches", s.LogPrefix(), batchJob.batchToProcess[0].BatchNumber, subBatchCount))
		if cfg.zk.SequencerResequenceStrict && subBatchCount != 1 {
			return fmt.Errorf("strict mode enabled, but resequenced batch %d has %d sub-batches", batchJob.batchToProcess[0].BatchNumber, subBatchCount)
		}
	}

	return nil
}
