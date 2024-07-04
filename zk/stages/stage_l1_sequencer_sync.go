package stages

import (
	"context"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/iden3/go-iden3-crypto/keccak256"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/log/v3"
	"math/big"
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
	initialCycle bool,
	quiet bool,
) (err error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting L1 Sequencer sync stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished L1 Sequencer sync stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
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

	hermezDb := hermez_db.NewHermezDb(tx)

	if !cfg.syncer.IsSyncStarted() {
		cfg.syncer.Run(progress)
	}

	logChan := cfg.syncer.GetLogsChan()
	progressChan := cfg.syncer.GetProgressMessageChan()

Loop:
	for {
		select {
		case logs := <-logChan:
			headersMap, err := cfg.syncer.L1QueryHeaders(logs)
			if err != nil {
				return err
			}

			for _, l := range logs {
				header := headersMap[l.BlockNumber]
				switch l.Topics[0] {
				case contracts.InitialSequenceBatchesTopic:
					if err := HandleInitialSequenceBatches(cfg.syncer, hermezDb, l, header); err != nil {
						return err
					}
				case contracts.AddNewRollupTypeTopic:
					rollupType := l.Topics[1].Big().Uint64()
					forkIdBytes := l.Data[64:96] // 3rd positioned item in the log data
					forkId := new(big.Int).SetBytes(forkIdBytes).Uint64()
					if err := hermezDb.WriteRollupType(rollupType, forkId); err != nil {
						return err
					}
				case contracts.CreateNewRollupTopic:
					rollupId := l.Topics[1].Big().Uint64()
					if rollupId != cfg.zkCfg.L1RollupId {
						return fmt.Errorf("received CreateNewRollupTopic for unknown rollup id: %v", rollupId)
					}
					rollupTypeBytes := l.Data[0:32]
					rollupType := new(big.Int).SetBytes(rollupTypeBytes).Uint64()
					fork, err := hermezDb.GetForkFromRollupType(rollupType)
					if err != nil {
						return err
					}
					if fork == 0 {
						log.Error("received CreateNewRollupTopic for unknown rollup type", "rollupType", rollupType)
					}
					if err := hermezDb.WriteNewForkHistory(fork, 0); err != nil {
						return err
					}
				case contracts.UpdateRollupTopic:
					rollupId := l.Topics[1].Big().Uint64()
					if rollupId != cfg.zkCfg.L1RollupId {
						log.Warn("received UpdateRollupTopic for unknown rollup id", "rollupId", rollupId)
						continue
					}
					newRollupBytes := l.Data[0:32]
					newRollup := new(big.Int).SetBytes(newRollupBytes).Uint64()
					fork, err := hermezDb.GetForkFromRollupType(newRollup)
					if err != nil {
						return err
					}
					if fork == 0 {
						return fmt.Errorf("received UpdateRollupTopic for unknown rollup type: %v", newRollup)
					}
					latestVerifiedBytes := l.Data[32:64]
					latestVerified := new(big.Int).SetBytes(latestVerifiedBytes).Uint64()
					if err := hermezDb.WriteNewForkHistory(fork, latestVerified); err != nil {
						return err
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
		}
	}

	cfg.syncer.Stop()

	progress = cfg.syncer.GetLastCheckedL1Block()
	if progress >= cfg.zkCfg.L1FirstBlock {
		// do not save progress if progress less than L1FirstBlock
		if err = stages.SaveStageProgress(tx, stages.L1SequencerSync, progress); err != nil {
			return err
		}
	}

	log.Info(fmt.Sprintf("[%s] L1 Sequencer sync finished", logPrefix))

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func HandleL1InfoTreeUpdate(
	syncer IL1Syncer,
	hermezDb *hermez_db.HermezDb,
	l ethTypes.Log,
	latestUpdate *types.L1InfoTreeUpdate,
	found bool,
	header *ethTypes.Header,
) (*types.L1InfoTreeUpdate, error) {
	if len(l.Topics) != 3 {
		log.Warn("Received log for info tree that did not have 3 topics")
		return nil, nil
	}
	var err error

	mainnetExitRoot := l.Topics[1]
	rollupExitRoot := l.Topics[2]
	combined := append(mainnetExitRoot.Bytes(), rollupExitRoot.Bytes()...)
	ger := keccak256.Hash(combined)
	update := &types.L1InfoTreeUpdate{
		GER:             common.BytesToHash(ger),
		MainnetExitRoot: mainnetExitRoot,
		RollupExitRoot:  rollupExitRoot,
	}

	if !found {
		// this is a special case, so we need to start at index 0
		update.Index = 0
	} else {
		// increment the index from the previous entry
		update.Index = latestUpdate.Index + 1
	}

	// now we need the block timestamp and the parent hash information for the block tied
	// to this event
	if header == nil {
		header, err = syncer.GetHeader(l.BlockNumber)
		if err != nil {
			return nil, err
		}
	}
	update.ParentHash = header.ParentHash
	update.Timestamp = header.Time
	update.BlockNumber = l.BlockNumber

	if err = hermezDb.WriteL1InfoTreeUpdate(update); err != nil {
		return nil, err
	}
	if err = hermezDb.WriteL1InfoTreeUpdateToGer(update); err != nil {
		return nil, err
	}
	return update, nil
}

const (
	injectedBatchLogTrailingBytes        = 24
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

	// the log appears to have some trailing 24 bytes of all 0s in it.  Not sure why but we can't handle the
	// TX without trimming these off
	trailingCutoff := len(l.Data) - injectedBatchLogTrailingBytes

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

	if err = db.WriteL1InjectedBatch(ib); err != nil {
		return err
	}

	return nil
}

func UnwindL1SequencerSyncStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg L1SequencerSyncCfg, ctx context.Context) error {
	return nil
}

func PruneL1SequencerSyncStage(s *stagedsync.PruneState, tx kv.RwTx, cfg L1SequencerSyncCfg, ctx context.Context) error {
	return nil
}
