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
	log.Info(fmt.Sprintf("[%s] Starting L1 Info Tree stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished L1 Info Tree stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	hermezDb := hermez_db.NewHermezDb(tx)

	progress, err := stages.GetStageProgress(tx, stages.L1InfoTree)
	if err != nil {
		return err
	}
	if progress == 0 {
		progress = cfg.zkCfg.L1FirstBlock - 1
	}

	latestUpdate, found, err := hermezDb.GetLatestL1InfoTreeUpdate()
	if err != nil {
		return err
	}

	// because the info tree updates are used by the RPC node and the sequencer, and we can switch between
	// the two, we need to ensure consistency when migrating from RPC to sequencer modes of operation,
	// so we keep track of these block numbers outside of the usual stage progress
	latestL1InfoTreeBlockNumber, err := hermezDb.GetL1InfoTreeHighestBlock()
	if err != nil {
		return err
	}

	if !cfg.syncer.IsSyncStarted() {
		cfg.syncer.Run(progress)
	}

	logChan := cfg.syncer.GetLogsChan()
	progressChan := cfg.syncer.GetProgressMessageChan()
	infoTreeUpdates := 0

Loop:
	for {
		select {
		case logs := <-logChan:
			blocksMap, err := cfg.syncer.L1QueryBlocks(logPrefix, logs)
			if err != nil {
				return err
			}

			for _, l := range logs {
				block := blocksMap[l.BlockNumber]
				switch l.Topics[0] {
				case contracts.UpdateL1InfoTreeTopic:
					if latestL1InfoTreeBlockNumber > 0 && l.BlockNumber <= latestL1InfoTreeBlockNumber {
						continue
					}
					latestUpdate, err = HandleL1InfoTreeUpdate(cfg.syncer, hermezDb, l, latestUpdate, found, block)
					if err != nil {
						return err
					}
					found = true
					infoTreeUpdates++
					latestL1InfoTreeBlockNumber = l.BlockNumber
				case contracts.InitialSequenceBatchesTopic:
					if err := HandleInitialSequenceBatches(cfg.syncer, hermezDb, l, block); err != nil {
						return err
					}
				default:
					log.Warn("received unexpected topic from l1 sync stage", "topic", l.Topics[0])
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

	if err = hermezDb.WriteL1InfoTreeHighestBlock(latestL1InfoTreeBlockNumber); err != nil {
		return err
	}

	progress = cfg.syncer.GetLastCheckedL1Block()
	if err = stages.SaveStageProgress(tx, stages.L1InfoTree, progress); err != nil {
		return err
	}

	log.Info(fmt.Sprintf("[%s] Info tree updates", logPrefix), "count", infoTreeUpdates)

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
	block *ethTypes.Block,
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
		// starting from a fresh db here, so we need to create index 0
		update.Index = 0
	} else {
		// increment the index from the previous entry
		update.Index = latestUpdate.Index + 1
	}

	// now we need the block timestamp and the parent hash information for the block tied
	// to this event
	if block == nil {
		block, err = syncer.GetBlock(l.BlockNumber)
		if err != nil {
			return nil, err
		}
	}
	update.ParentHash = block.ParentHash()
	update.Timestamp = block.Time()
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
	l1Block *ethTypes.Block,
) error {
	var err error

	if l1Block == nil {
		l1Block, err = syncer.GetBlock(l.BlockNumber)
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
		Timestamp:          l1Block.Time(),
		L1BlockHash:        l1Block.Hash(),
		L1ParentHash:       l1Block.ParentHash(),
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
