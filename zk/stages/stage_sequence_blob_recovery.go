package stages

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/zk/da"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/syncer"
	"time"
)

type SequencerBlobRecoveryCfg struct {
	db       kv.RwDB
	zkCfg    *ethconfig.Zk
	chainCfg *chain.Config
	syncer   *syncer.L1Syncer
}

func StageSequencerBlobRecoveryCfg(db kv.RwDB, zkCfg *ethconfig.Zk, chainCfg *chain.Config, syncer *syncer.L1Syncer) SequencerBlobRecoveryCfg {
	return SequencerBlobRecoveryCfg{
		db:       db,
		zkCfg:    zkCfg,
		chainCfg: chainCfg,
		syncer:   syncer,
	}
}

func SpawnSequencerBlobRecoveryStage(s *stagedsync.StageState, u stagedsync.Unwinder, ctx context.Context, tx kv.RwTx, cfg SequencerBlobRecoveryCfg, logger log.Logger) error {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting blob recovery stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished blob recovery stage", logPrefix))

	if cfg.zkCfg.IsL1Recovery() {
		log.Info(fmt.Sprintf("[%s] Skipping blob recovery stage because L1 recovery is enabled", logPrefix))
		return nil
	}

	if !cfg.zkCfg.IsBlobRecovery() {
		log.Info(fmt.Sprintf("[%s] Skipping blob recovery because it is not enabled", logPrefix))
		return nil
	}

	var err error
	freshTx := false
	if tx == nil {
		freshTx = true
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hermezDb := hermez_db.NewHermezDb(tx)

	highestBatch, err := stages.GetStageProgress(tx, stages.HighestSeenBatchNumber)
	if err != nil {
		return err
	}

	highestKnownBatch, err := hermezDb.GetLastL1BatchData()
	if err != nil {
		return err
	}

	if cfg.zkCfg.RecoveryStopBatch > 0 {
		// stop completely if we have executed past the stop batch in config
		if highestKnownBatch > cfg.zkCfg.RecoveryStopBatch {
			log.Info(fmt.Sprintf("[%s] Stopping blob recovery stage based on configured stop batch", logPrefix), "config", cfg.zkCfg.RecoveryStopBatch, "highest-known", highestKnownBatch)
			time.Sleep(1 * time.Second)
			return nil
		}

		hasEverything, err := haveAllBatchesInDb(highestBatch, cfg.zkCfg, hermezDb)
		if err != nil {
			return err
		}

		if hasEverything {
			log.Info(fmt.Sprintf("[%s] Stopping blob recovery stage based on configured stop batch", logPrefix), "config", cfg.zkCfg.RecoveryStopBatch, "highest-known", highestKnownBatch)
			time.Sleep(1 * time.Second)
			return nil
		}
	}

	// check if execution has caught up to the tip of the chain
	if highestBatch > 0 && highestKnownBatch == highestBatch {
		log.Info(fmt.Sprintf("[%s] Blob recovery has completed!", logPrefix), "batch", highestBatch)
		time.Sleep(5 * time.Second)
		return nil
	}

	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()
	start := time.Now()

	// starting limit and offset for pagination
	offset := uint64(0)
	limit := cfg.zkCfg.BlobRecoveryBlobLimit

	indexToRoots, err := hermezDb.GetL1InfoTreeIndexToRoots()
	if err != nil {
		return nil
	}

	irt := da.NewInfoRootTracker(indexToRoots)

	for {
		select {
		case <-logTicker.C:
			log.Info(fmt.Sprintf("[%s] Syncing batches from Blob DA", logPrefix), "elapsed/s", time.Since(start).Seconds())
		default:
		}

		ocb, finished, err := da.GetOffChainBlobs(cfg.zkCfg.BlobDAUrl, limit, offset)
		if err != nil {
			return err
		}

		if finished {
			log.Info(fmt.Sprintf("[%s] Finished syncing batches from blobs in Blob DA", logPrefix))
			break
		}

		for _, blob := range ocb {
			for _, input := range blob.BlobInputs {
				batchNumber, batchL1Data, err := da.CreateL1BatchDataFromBlobInput(hermezDb, input, cfg.zkCfg, cfg.chainCfg.IsNormalcy(s.BlockNumber), irt)
				if err != nil {
					return err
				}

				if err = hermezDb.WriteL1BatchData(batchNumber, batchL1Data); err != nil {
					return err
				}
			}

			log.Info(fmt.Sprintf("[%s] Processed batches from blob DA", logPrefix), "startBatch", blob.StartBatch, "endBatch", blob.EndBatch)
		}

		// increment the offset for the next iteration
		offset += limit
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindSequencerBlobRecoveryStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg SequencerBlobRecoveryCfg, ctx context.Context, logger log.Logger) error {
	return nil
}

func PruneSequencerBlobRecoveryStage(s *stagedsync.PruneState, tx kv.RwTx, cfg SequencerBlobRecoveryCfg, ctx context.Context, logger log.Logger) error {
	return nil
}
