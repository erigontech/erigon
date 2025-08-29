package stages

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	ethTypes "github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/zk/contracts"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/l1infotree"
	"github.com/erigontech/erigon/zk/types"
)

type L1SequencerSyncCfg struct {
	db     kv.RwDB
	zkCfg  *ethconfig.Zk
	syncer l1infotree.Syncer
}

func StageL1SequencerSyncCfg(db kv.RwDB, zkCfg *ethconfig.Zk, sync l1infotree.Syncer) L1SequencerSyncCfg {
	return L1SequencerSyncCfg{
		db:     db,
		zkCfg:  zkCfg,
		syncer: sync,
	}
}

func SpawnL1SequencerSyncStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	cfg L1SequencerSyncCfg,
	ctx context.Context,
	logger log.Logger,
) (funcErr error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting L1 Sequencer sync stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished L1 Sequencer sync stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	progress, err := stages.GetStageProgress(tx, stages.L1SequencerSync)
	if err != nil {
		return err
	}
	if progress == 0 {
		progress = cfg.zkCfg.L1FirstBlock - 1
	}

	// if the flag is set - wait for that block to be finalized on L1 before continuing
	if progress <= cfg.zkCfg.L1FinalizedBlockRequirement && cfg.zkCfg.L1FinalizedBlockRequirement > 0 {
		for {
			finalized, finalizedBn, err := cfg.syncer.CheckL1BlockFinalized(cfg.zkCfg.L1FinalizedBlockRequirement)
			if err != nil {
				// we shouldn't just throw the error, because it could be a timeout, or "too many requests" error and we could jsut retry
				log.Error(fmt.Sprintf("[%s] Error checking if L1 block %v is finalized: %v", logPrefix, cfg.zkCfg.L1FinalizedBlockRequirement, err))
			}

			if finalized {
				break
			}
			log.Info(fmt.Sprintf("[%s] Waiting for L1 block %v to be correctly checked for \"finalized\" before continuing. Current finalized is %d", logPrefix, cfg.zkCfg.L1FinalizedBlockRequirement, finalizedBn))
			time.Sleep(1 * time.Minute) // sleep could be even bigger since finalization takes more than 10 minutes
		}
	}

	hermezDb := hermez_db.NewHermezDb(tx)

	if !cfg.syncer.IsSyncStarted() {
		cfg.syncer.RunQueryBlocks(progress)
		defer func() {
			if funcErr != nil {
				cfg.syncer.StopQueryBlocks()
				cfg.syncer.ConsumeQueryBlocks()
				cfg.syncer.WaitQueryBlocksToFinish()
			}
		}()
	}

	logChan := cfg.syncer.GetLogsChan()
	progressChan := cfg.syncer.GetProgressMessageChan()
	defer cfg.syncer.ClearHeaderCache()

	idleTicker := time.NewTimer(10 * time.Second)
	latestActivity := time.Now()

Loop:
	for {
		select {
		case logs, ok := <-logChan:
			if !ok {
				break Loop
			}

			latestActivity = time.Now()

			for _, l := range logs {
				switch l.Topics[0] {
				case contracts.InitialSequenceBatchesTopic:
					if funcErr = HandleInitialSequenceBatches(cfg.syncer, hermezDb, l); funcErr != nil {
						return funcErr
					}
				case contracts.AddNewRollupTypeTopic, contracts.AddNewRollupTypeTopicBanana:
					rollupType := l.Topics[1].Big().Uint64()
					forkIdBytes := l.Data[64:96] // 3rd positioned item in the log data
					forkId := new(big.Int).SetBytes(forkIdBytes).Uint64()

					// now that we support fep -> pp migrations we only want to accept and handle fork changes that are related to
					// StateTransition type rollup ids anything else we can write as a PP rollup type and ignore fork changes for these events
					verifierType := l.Data[96:128] // 4th positioned item in the log data
					verifierBig := new(big.Int).SetBytes(verifierType)
					if verifierBig.Uint64() == 0 {
						if funcErr = hermezDb.WriteRollupType(rollupType, forkId); funcErr != nil {
							return funcErr
						}
					} else {
						if err := hermezDb.WritePPRollupType(rollupType); err != nil {
							return err
						}
					}
				case contracts.CreateNewRollupTopic:
					rollupId := l.Topics[1].Big().Uint64()
					if rollupId != cfg.zkCfg.L1RollupId {
						continue
					}
					rollupTypeBytes := l.Data[0:32]
					rollupType := new(big.Int).SetBytes(rollupTypeBytes).Uint64()
					fork, err := hermezDb.GetForkFromRollupType(rollupType)
					if err != nil {
						funcErr = err
						return funcErr
					}
					if fork == 0 {
						log.Warn("received CreateNewRollupTopic for unknown rollup type", "rollupType", rollupType)
					}
					if funcErr = hermezDb.WriteNewForkHistory(fork, 0); funcErr != nil {
						return funcErr
					}
				case contracts.UpdateRollupTopic:
					rollupId := l.Topics[1].Big().Uint64()
					if rollupId != cfg.zkCfg.L1RollupId {
						continue
					}
					newRollupBytes := l.Data[0:32]
					newRollup := new(big.Int).SetBytes(newRollupBytes).Uint64()
					fork, err := hermezDb.GetForkFromRollupType(newRollup)
					if err != nil {
						funcErr = err
						return funcErr
					}
					if fork == 0 {
						// this might be a PP rollup type that we need to effecitvely ignore as it won't affect the fork history
						isPP, err := hermezDb.IsPPRollupType(newRollup)
						if err != nil {
							return err
						}
						if isPP {
							log.Info("received UpdateRollupTopic for PP rollup type, ignoring", "rollupType", newRollup)
							continue
						}
						funcErr = fmt.Errorf("received UpdateRollupTopic for unknown rollup type: %v", newRollup)
						return funcErr
					}
					latestVerifiedBytes := l.Data[32:64]
					latestVerified := new(big.Int).SetBytes(latestVerifiedBytes).Uint64()
					if funcErr = hermezDb.WriteNewForkHistory(fork, latestVerified); funcErr != nil {
						return funcErr
					}
				default:
					log.Warn("received unexpected topic from l1 sequencer sync stage", "topic", l.Topics[0])
				}
			}
		case progMsg := <-progressChan:
			log.Info(fmt.Sprintf("[%s] %s", logPrefix, progMsg))
		case <-ctx.Done():
			break Loop
		case <-idleTicker.C:
			if time.Since(latestActivity) > noActivityTimeout {
				log.Warn(fmt.Sprintf("[%s] No activity for %s", logPrefix, noActivityTimeout))

				cfg.syncer.StopQueryBlocks()
				cfg.syncer.ConsumeQueryBlocks()
				cfg.syncer.WaitQueryBlocksToFinish()
				break Loop
			}
		}
	}

	progress = cfg.syncer.GetLastCheckedL1Block()
	if progress >= cfg.zkCfg.L1FirstBlock {
		// do not save progress if progress less than L1FirstBlock
		if funcErr = stages.SaveStageProgress(tx, stages.L1SequencerSync, progress); funcErr != nil {
			return funcErr
		}
	}

	log.Info(fmt.Sprintf("[%s] L1 Sequencer sync finished", logPrefix))

	if freshTx {
		if funcErr = tx.Commit(); funcErr != nil {
			return funcErr
		}
	}

	return nil
}

const (
	injectedBatchLogTransactionStartByte = 128
	injectedBatchLastGerStartByte        = 32
	injectedBatchLastGerEndByte          = 64
	injectedBatchSequencerStartByte      = 76
	injectedBatchSequencerEndByte        = 96
)

func HandleInitialSequenceBatches(
	syncer l1infotree.Syncer,
	db *hermez_db.HermezDb,
	l ethTypes.Log,
) error {
	var err error

	header, err := syncer.GetHeader(l.BlockNumber)
	if err != nil {
		return err
	}

	// the log appears to have some trailing some bytes of all 0s in it.  Not sure why but we can't handle the
	// TX without trimming these off
	injectedBatchLogTrailingBytes := getTrailingCutoffLen(l.Data)
	trailingCutoff := len(l.Data) - injectedBatchLogTrailingBytes
	log.Debug(fmt.Sprintf("Handle initial sequence batches, trail len:%v, log data: %v", injectedBatchLogTrailingBytes, l.Data))

	txData := l.Data[injectedBatchLogTransactionStartByte:trailingCutoff]

	ib := &types.L1InjectedBatch{
		L1BlockNumber:      l.BlockNumber,
		Timestamp:          header.Time,
		L1BlockHash:        header.Hash(),
		L1ParentHash:       header.ParentHash,
		LastGlobalExitRoot: common.BytesToHash(l.Data[injectedBatchLastGerStartByte:injectedBatchLastGerEndByte]),
		Sequencer:          common.BytesToAddress(l.Data[injectedBatchSequencerStartByte:injectedBatchSequencerEndByte]),
		Transaction:        txData,
	}

	return db.WriteL1InjectedBatch(ib)
}

func UnwindL1SequencerSyncStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg L1SequencerSyncCfg, ctx context.Context) error {
	return nil
}

func PruneL1SequencerSyncStage(s *stagedsync.PruneState, tx kv.RwTx, cfg L1SequencerSyncCfg, ctx context.Context) error {
	return nil
}

func getTrailingCutoffLen(logData []byte) int {
	for i := len(logData) - 1; i >= 0; i-- {
		if logData[i] != 0 {
			return len(logData) - i - 1
		}
	}
	return 0
}
