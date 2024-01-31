package stages

import (
	"context"
	"fmt"
	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/log/v3"
)

type L1SequencerSyncCfg struct {
	db     kv.RwDB
	zkCfg  *ethconfig.Zk
	syncer *syncer.L1Syncer
}

func StageL1SequencerSyncCfg(db kv.RwDB, zkCfg *ethconfig.Zk, sync *syncer.L1Syncer) L1SequencerSyncCfg {
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

	hermezDb, err := hermez_db.NewHermezDb(tx)
	if err != nil {
		return err
	}

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

	if !cfg.syncer.IsSyncStarted() {
		cfg.syncer.Run(progress)
	}

	logChan := cfg.syncer.GetLogsChan()
	progressChan := cfg.syncer.GetProgressMessageChan()

Loop:
	for {
		select {
		case l := <-logChan:
			switch l.Topics[0] {
			case contracts.UpdateL1InfoTreeTopic:
				if err := handleL1InfoTreeUpdate(cfg, hermezDb, l, latestUpdate, found); err != nil {
					return err
				}
			case contracts.InitialSequenceBatchesTopic:
				if err := handleInitialSequenceBatches(cfg, hermezDb, l); err != nil {
					return err
				}
				// todo: [zkevm] handle the writing of this information for use in execution
			default:
				log.Warn("received unexpected topic from l1 sync stage", "topic", l.Topics[0])
			}

		case progMsg := <-progressChan:
			log.Info(fmt.Sprintf("[%s] %s", logPrefix, progMsg))
		default:
			if !cfg.syncer.IsDownloading() {
				break Loop
			}
		}
	}

	progress = cfg.syncer.GetLastCheckedL1Block()
	if err = stages.SaveStageProgress(tx, stages.L1InfoTree, progress); err != nil {
		return err
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func handleL1InfoTreeUpdate(
	cfg L1SequencerSyncCfg,
	hermezDb *hermez_db.HermezDb,
	l ethTypes.Log,
	latestUpdate *types.L1InfoTreeUpdate,
	found bool,
) error {
	if len(l.Topics) != 3 {
		log.Warn("Received log for info tree that did not have 3 topics")
		return nil
	}
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
		found = true
	} else {
		// increment the index from the previous entry
		update.Index = latestUpdate.Index + 1
	}

	// now we need the block timestamp and the parent hash information for the block tied
	// to this event
	block, err := cfg.syncer.GetBlock(l.BlockNumber)
	if err != nil {
		return err
	}
	update.ParentHash = block.ParentHash()
	update.Timestamp = block.Time()

	latestUpdate = update

	if err = hermezDb.WriteL1InfoTreeUpdate(update); err != nil {
		return err
	}
	return nil
}

func handleInitialSequenceBatches(
	cfg L1SequencerSyncCfg,
	db *hermez_db.HermezDb,
	l ethTypes.Log,
) error {
	// todo: [zkevm] figure out what to store here and how.  We need the tx information from this log for injecting
	// the initial batch and the associated GER for the first block + timestamp
	return nil
}

func UnwindL1SequencerSyncStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg L1SequencerSyncCfg, ctx context.Context) error {
	return nil
}

func PruneL1SequencerSyncStage(s *stagedsync.PruneState, tx kv.RwTx, cfg L1SequencerSyncCfg, ctx context.Context) error {
	return nil
}
