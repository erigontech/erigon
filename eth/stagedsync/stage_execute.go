// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/silkworm"
)

const (
	logInterval = 30 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000
)

type headerDownloader interface {
	ReportBadHeaderPoS(badHeader, lastValidAncestor common.Hash)
	POSSync() bool
}

type ExecuteBlockCfg struct {
	db            kv.RwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	chainConfig   *chain.Config
	notifications *shards.Notifications
	engine        consensus.Engine
	vmConfig      *vm.Config
	badBlockHalt  bool
	stateStream   bool
	blockReader   services.FullBlockReader
	hd            headerDownloader
	author        *common.Address
	// last valid number of the stage

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis

	silkworm            *silkworm.Silkworm
	blockProduction     bool
	polygonExtraReceipt bool

	applyWorker, applyWorkerMining *exec3.Worker
}

func StageExecuteBlocksCfg(
	db kv.RwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	chainConfig *chain.Config,
	engine consensus.Engine,
	vmConfig *vm.Config,
	notifications *shards.Notifications,
	stateStream bool,
	badBlockHalt bool,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	hd headerDownloader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	silkworm *silkworm.Silkworm,
	polygonExtraReceipt bool,
) ExecuteBlockCfg {
	if dirs.SnapDomain == "" {
		panic("empty `dirs` variable")
	}

	return ExecuteBlockCfg{
		db:                  db,
		prune:               pm,
		batchSize:           batchSize,
		chainConfig:         chainConfig,
		engine:              engine,
		vmConfig:            vmConfig,
		dirs:                dirs,
		notifications:       notifications,
		stateStream:         stateStream,
		badBlockHalt:        badBlockHalt,
		blockReader:         blockReader,
		hd:                  hd,
		genesis:             genesis,
		historyV3:           true,
		syncCfg:             syncCfg,
		silkworm:            silkworm,
		polygonExtraReceipt: polygonExtraReceipt,
		applyWorker:         exec3.NewWorker(nil, log.Root(), context.Background(), false, db, nil, blockReader, chainConfig, genesis, nil, engine, dirs, false),
		applyWorkerMining:   exec3.NewWorker(nil, log.Root(), context.Background(), false, db, nil, blockReader, chainConfig, genesis, nil, engine, dirs, true),
	}
}

// ================ Erigon3 ================

func ExecBlockV3(s *StageState, u Unwinder, txc wrap.TxContainer, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, initialCycle bool, logger log.Logger, isMining bool) (err error) {
	workersCount := cfg.syncCfg.ExecWorkerCount
	if !initialCycle {
		workersCount = 1
	}

	prevStageProgress, err := stageProgress(txc.Tx, cfg.db, stages.Senders)
	if err != nil {
		return err
	}

	var to = prevStageProgress
	if toBlock > 0 {
		to = min(prevStageProgress, toBlock)
	}
	if to < s.BlockNumber {
		return nil
	}

	parallel := txc.Tx == nil
	if err := ExecV3(ctx, s, u, workersCount, cfg, txc, parallel, to, logger, initialCycle, isMining); err != nil {
		return err
	}
	return nil
}

var ErrTooDeepUnwind = errors.New("too deep unwind")

func unwindExec3(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, accumulator *shards.Accumulator, logger log.Logger) (err error) {
	br := cfg.blockReader
	var domains *libstate.SharedDomains
	if txc.Doms == nil {
		domains, err = libstate.NewSharedDomains(txc.Tx, logger)
		if err != nil {
			return err
		}
		defer domains.Close()
	} else {
		domains = txc.Doms
	}
	rs := state.NewStateV3(domains, cfg.syncCfg, cfg.chainConfig.Bor != nil, logger)

	txNumsReader := br.TxnumReader(ctx)

	// unwind all txs of u.UnwindPoint block. 1 txn in begin/end of block - system txs
	txNum, err := txNumsReader.Min(txc.Tx, u.UnwindPoint+1)
	if err != nil {
		return err
	}

	t := time.Now()
	var changeset *[kv.DomainLen][]kv.DomainEntryDiff
	for currentBlock := u.CurrentBlockNumber; currentBlock > u.UnwindPoint; currentBlock-- {
		currentHash, ok, err := br.CanonicalHash(ctx, txc.Tx, currentBlock)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("canonical hash not found %d", currentBlock)
		}
		var currentKeys [kv.DomainLen][]kv.DomainEntryDiff
		currentKeys, ok, err = domains.GetDiffset(txc.Tx, currentHash, currentBlock)
		if !ok {
			return fmt.Errorf("domains.GetDiffset(%d, %s): not found", currentBlock, currentHash)
		}
		if err != nil {
			return err
		}
		if changeset == nil {
			changeset = &currentKeys
		} else {
			for i := range currentKeys {
				changeset[i] = libstate.MergeDiffSets(changeset[i], currentKeys[i])
			}
		}
	}
	if err := rs.Unwind(ctx, txc.Tx, u.UnwindPoint, txNum, accumulator, changeset); err != nil {
		return fmt.Errorf("StateV3.Unwind(%d->%d): %w, took %s", s.BlockNumber, u.UnwindPoint, err, time.Since(t))
	}
	if err := rawdb.DeleteNewerEpochs(txc.Tx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("delete newer epochs: %w", err)
	}
	return nil
}

func stageProgress(tx kv.Tx, db kv.RoDB, stage stages.SyncStage) (prevStageProgress uint64, err error) {
	if tx != nil {
		prevStageProgress, err = stages.GetStageProgress(tx, stage)
		if err != nil {
			return prevStageProgress, err
		}
	} else {
		if err = db.View(context.Background(), func(tx kv.Tx) error {
			prevStageProgress, err = stages.GetStageProgress(tx, stage)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return prevStageProgress, err
		}
	}
	return prevStageProgress, nil
}

// ================ Erigon3 End ================

func SpawnExecuteBlocksStage(s *StageState, u Unwinder, txc wrap.TxContainer, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) (err error) {
	if dbg.StagesOnlyBlocks {
		return nil
	}
	if err = ExecBlockV3(s, u, txc, toBlock, ctx, cfg, s.CurrentSyncCycle.IsInitialCycle, logger, false); err != nil {
		return err
	}
	return nil
}

func UnwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) (err error) {
	//fmt.Printf("unwind: %d -> %d\n", u.CurrentBlockNumber, u.UnwindPoint)
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := txc.Tx != nil
	if !useExternalTx {
		txc.Tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer txc.Tx.Rollback()
	}
	logPrefix := u.LogPrefix()
	logger.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	unwindToLimit, ok, err := txc.Tx.(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).CanUnwindBeforeBlockNum(u.UnwindPoint, txc.Tx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %d < %d", ErrTooDeepUnwind, u.UnwindPoint, unwindToLimit)
	}

	if err = unwindExecutionStage(u, s, txc, ctx, cfg, logger); err != nil {
		return err
	}
	if err = u.Done(txc.Tx); err != nil {
		return err
	}
	//dumpPlainStateDebug(tx, nil)

	if !useExternalTx {
		if err = txc.Tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindExecutionStage(u *UnwindState, s *StageState, txc wrap.TxContainer, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) error {
	var accumulator *shards.Accumulator
	if cfg.stateStream && s.BlockNumber-u.UnwindPoint < stateStreamLimit {
		accumulator = cfg.notifications.Accumulator

		hash, ok, err := cfg.blockReader.CanonicalHash(ctx, txc.Tx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("read canonical hash of unwind point: %w", err)
		}
		if !ok {
			return fmt.Errorf("canonical hash not found %d", u.UnwindPoint)
		}
		txs, err := cfg.blockReader.RawTransactions(ctx, txc.Tx, u.UnwindPoint, s.BlockNumber)
		if err != nil {
			return err
		}
		accumulator.StartChange(u.UnwindPoint, hash, txs, true)
	}

	return unwindExec3(u, s, txc, ctx, cfg, accumulator, logger)
}

func PruneExecutionStage(s *PruneState, tx kv.RwTx, cfg ExecuteBlockCfg, ctx context.Context, logger log.Logger) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if s.ForwardProgress > config3.MaxReorgDepthV3 && !cfg.syncCfg.AlwaysGenerateChangesets {
		// (chunkLen is 8Kb) * (1_000 chunks) = 8mb
		// Some blocks on bor-mainnet have 400 chunks of diff = 3mb
		var pruneDiffsLimitOnChainTip = 1_000
		pruneTimeout := 250 * time.Millisecond
		if s.CurrentSyncCycle.IsInitialCycle {
			pruneDiffsLimitOnChainTip = math.MaxInt
			pruneTimeout = time.Hour
		}
		if err := rawdb.PruneTable(
			tx,
			kv.ChangeSets3,
			s.ForwardProgress-config3.MaxReorgDepthV3,
			ctx,
			pruneDiffsLimitOnChainTip,
			pruneTimeout,
			logger,
			s.LogPrefix(),
		); err != nil {
			return err
		}
	}

	mxExecStepsInDB.Set(rawdbhelpers.IdxStepsCountV3(tx) * 100)

	// on chain-tip:
	//  - can prune only between blocks (without blocking blocks processing)
	//  - need also leave some time to prune blocks
	//  - need keep "fsync" time of db fast
	// Means - the best is:
	//  - sto
	// p prune when `tx.SpaceDirty()` is big
	//  - and set ~500ms timeout
	// because on slow disks - prune is slower. but for now - let's tune for nvme first, and add `tx.SpaceDirty()` check later https://github.com/erigontech/erigon/issues/11635
	pruneTimeout := 250 * time.Millisecond
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneTimeout = 12 * time.Hour

		// allow greedy prune on non-chain-tip
		if err = tx.(*temporal.Tx).AggTx().(*libstate.AggregatorRoTx).GreedyPruneHistory(ctx, kv.CommitmentDomain, tx); err != nil {
			return err
		}
	}
	if _, err := tx.(*temporal.Tx).AggTx().(*libstate.AggregatorRoTx).PruneSmallBatches(ctx, pruneTimeout, tx); err != nil {
		return err
	}

	// prune receipts cache
	if cfg.syncCfg.PersistReceiptsV1 > 0 && s.ForwardProgress > cfg.syncCfg.PersistReceiptsV1 {
		pruneTo := s.ForwardProgress - cfg.syncCfg.PersistReceiptsV1
		pruneLimit := 10
		if s.CurrentSyncCycle.IsInitialCycle {
			pruneLimit = -1
		}
		if err := rawdb.PruneReceiptsCache(tx, pruneTo, pruneLimit); err != nil {
			return err
		}
	}

	if err = s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
