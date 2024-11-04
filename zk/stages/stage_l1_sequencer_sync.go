package stages

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/log/v3"
)

type L1SequencerSyncCfg struct {
	db     kv.RwDB
	zkCfg  *ethconfig.Zk
	syncer IL1Syncer
}

func StageL1SequencerSyncCfg(db kv.RwDB, zkCfg *ethconfig.Zk, sync IL1Syncer) L1SequencerSyncCfg {
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
	if progress > 0 {
		// if we have progress then we can assume that we have the single injected batch already so can just return here
		return nil
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

Loop:
	for {
		select {
		case logs := <-logChan:
			headersMap, err := cfg.syncer.L1QueryHeaders(logs)
			if err != nil {
				funcErr = err
				return funcErr
			}

			for _, l := range logs {
				header := headersMap[l.BlockNumber]
				switch l.Topics[0] {
				case contracts.InitialSequenceBatchesTopic:
					if funcErr = HandleInitialSequenceBatches(cfg.syncer, hermezDb, l, header); funcErr != nil {
						return funcErr
					}
				case contracts.AddNewRollupTypeTopic:
					fallthrough
				case contracts.AddNewRollupTypeTopicBanana:
					rollupType := l.Topics[1].Big().Uint64()
					forkIdBytes := l.Data[64:96] // 3rd positioned item in the log data
					forkId := new(big.Int).SetBytes(forkIdBytes).Uint64()
					if funcErr = hermezDb.WriteRollupType(rollupType, forkId); funcErr != nil {
						return funcErr
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
						log.Error("received CreateNewRollupTopic for unknown rollup type", "rollupType", rollupType)
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
		default:
			if !cfg.syncer.IsDownloading() {
				break Loop
			}
			time.Sleep(10 * time.Millisecond)
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
	injectedBatchLastGerStartByte        = 31
	injectedBatchLastGerEndByte          = 64
	injectedBatchSequencerStartByte      = 76
	injectedBatchSequencerEndByte        = 96
)

func HandleInitialSequenceBatches(
	syncer IL1Syncer,
	db *hermez_db.HermezDb,
	l ethTypes.Log,
	header *ethTypes.Header,
) error {
	var err error

	if header == nil {
		header, err = syncer.GetHeader(l.BlockNumber)
		if err != nil {
			return err
		}
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
