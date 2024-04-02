package stages

import (
	"context"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"sort"
	"github.com/ledgerwatch/log/v3"
)

type SequencerExecutorVerifyCfg struct {
	db       kv.RwDB
	verifier *legacy_executor_verifier.LegacyExecutorVerifier
	txPool   *txpool.TxPool
}

func StageSequencerExecutorVerifyCfg(
	db kv.RwDB,
	verifier *legacy_executor_verifier.LegacyExecutorVerifier,
) SequencerExecutorVerifyCfg {
	return SequencerExecutorVerifyCfg{
		db:       db,
		verifier: verifier,
	}
}

func SpawnSequencerExecutorVerifyStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerExecutorVerifyCfg,
	initialCycle bool,
	quiet bool,
) error {
	var err error
	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hermezDb := hermez_db.NewHermezDb(tx)

	// progress here is at the batch level
	progress, err := stages.GetStageProgress(tx, stages.SequenceExecutorVerify)
	if err != nil {
		return err
	}

	// progress here is at the block level
	intersProgress, err := stages.GetStageProgress(tx, stages.IntermediateHashes)
	if err != nil {
		return err
	}

	// we need to get the batch number for the latest block, so we can search for new batches to send for
	// verification
	intersBatch, err := hermezDb.GetBatchNoByL2Block(intersProgress)
	if err != nil {
		return err
	}

	// we could be running in a state with no executors so we need instant response that we are in an
	// ok state to save lag in the data stream !!Dragons: there will be no witnesses stored running in
	// this mode of operation
	canVerify := cfg.verifier.HasExecutors()
	if !canVerify {
		if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, intersBatch); err != nil {
			return err
		}
		if freshTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}
		return nil
	}

	// get the latest responses from the verifier then sort them, so we can make sure we're handling verifications
	// in order
	responses := cfg.verifier.GetAllResponses()

	// sort responses by batch number in ascending order
	sort.Slice(responses, func(i, j int) bool {
		return responses[i].BatchNumber < responses[j].BatchNumber
	})

	for _, response := range responses {
		// ensure that the first response is the next batch based on the current stage progress
		// otherwise just return early until we get it
		if response.BatchNumber != progress+1 {
			return nil
		}

		// now check that we are indeed in a good state to continue
		if !response.Valid {
			// now we need to rollback and handle the error
			// todo [zkevm]!

			// todo: remove any witnesses for batches higher than the one failing (including the failing one)
		}

		// all good so just update the stage progress for now
		if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, response.BatchNumber); err != nil {
			return err
		}

		// store the witness
		errWitness := hermezDb.WriteWitness(response.BatchNumber, response.Witness)
		if errWitness != nil {
			log.Warn("Failed to write witness", "batch", response.BatchNumber, "err", errWitness)
		}

		// now let the verifier know we have got this message, so it can release it
		cfg.verifier.RemoveResponse(response.BatchNumber)
		progress = response.BatchNumber
	}

	// send off the new batches to the verifier to be processed
	for batch := progress + 1; batch <= intersBatch; batch++ {
		// we do not need to verify batch 1 as this is the injected batch so just updated progress and move on
		if batch == injectedBatchNumber {
			if err = stages.SaveStageProgress(tx, stages.SequenceExecutorVerify, injectedBatchNumber); err != nil {
				return err
			}
		} else {
			// we need the state root of the last block in the batch to send to the executor
			blocks, err := hermezDb.GetL2BlockNosByBatch(batch)
			if err != nil {
				return err
			}
			sort.Slice(blocks, func(i, j int) bool {
				return blocks[i] > blocks[j]
			})
			lastBlockNumber := blocks[0]
			block, err := rawdb.ReadBlockByNumber(tx, lastBlockNumber)
			if err != nil {
				return err
			}

			cfg.verifier.AddRequest(&legacy_executor_verifier.VerifierRequest{BatchNumber: batch, StateRoot: block.Root()})
		}
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindSequencerExecutorVerifyStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerExecutorVerifyCfg,
	initialCycle bool,
) error {
	return nil
}

func PruneSequencerExecutorVerifyStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg SequencerExecutorVerifyCfg,
	ctx context.Context,
	initialCycle bool,
) error {
	return nil
}
