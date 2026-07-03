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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
)

const (
	logInterval = 30 * time.Second

	// stateStreamLimit - don't accumulate state changes if jump is bigger than this amount of blocks
	stateStreamLimit uint64 = 1_000
)

type ExecuteBlockCfg struct {
	db            kv.TemporalRwDB
	batchSize     datasize.ByteSize
	prune         prune.Mode
	chainConfig   *chain.Config
	notifications *shards.Notifications
	engine        rules.Engine
	vmConfig      *vm.Config
	badBlockHalt  bool
	stateStream   bool
	blockReader   services.FullBlockReader
	author        accounts.Address
	// last valid number of the stage

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis

	experimentalBAL bool
	readAheader     *exec.BlockReadAheader
}

func StageExecuteBlocksCfg(
	db kv.TemporalRwDB,
	pm prune.Mode,
	batchSize datasize.ByteSize,
	chainConfig *chain.Config,
	engine rules.Engine,
	vmConfig *vm.Config,
	notifications *shards.Notifications,
	stateStream bool,
	badBlockHalt bool,

	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	experimentalBAL bool,
	readAheader *exec.BlockReadAheader,
) ExecuteBlockCfg {
	if dirs.SnapDomain == "" {
		panic("empty `dirs` variable")
	}

	return ExecuteBlockCfg{
		db:              db,
		prune:           pm,
		batchSize:       batchSize,
		chainConfig:     chainConfig,
		engine:          engine,
		vmConfig:        vmConfig,
		dirs:            dirs,
		notifications:   notifications,
		stateStream:     stateStream,
		badBlockHalt:    badBlockHalt,
		blockReader:     blockReader,
		genesis:         genesis,
		historyV3:       true,
		syncCfg:         syncCfg,
		experimentalBAL: experimentalBAL,
		readAheader:     readAheader,
	}
}

// ChainConfig returns the chain configuration.
func (cfg ExecuteBlockCfg) ChainConfig() *chain.Config { return cfg.chainConfig }

// IsExperimentalBAL returns whether experimental BAL is enabled.
func (cfg ExecuteBlockCfg) IsExperimentalBAL() bool { return cfg.experimentalBAL }

// BlockReader returns the block reader.
func (cfg ExecuteBlockCfg) BlockReader() services.FullBlockReader { return cfg.blockReader }

// DirsDataDir returns the data directory path.
func (cfg ExecuteBlockCfg) DirsDataDir() string { return cfg.dirs.DataDir }

// WithAuthor returns a copy of the config with the author set.
func (cfg ExecuteBlockCfg) WithAuthor(author accounts.Address) ExecuteBlockCfg {
	cfg.author = author
	return cfg
}

// ================ Erigon3 ================

var ErrTooDeepUnwind = errors.New("too deep unwind")

// overlayPruneToTxNum returns the overlay-prune boundary on unwind: the last
// committed txNum, so TemporalMemBatch.Unwind (which keeps txNum <= boundary)
// drops every write of a block that will be re-executed.
func overlayPruneToTxNum(ctx context.Context, cfg ExecuteBlockCfg, tx kv.Tx, committedBlock uint64) (uint64, error) {
	return cfg.blockReader.TxnumReader().Max(ctx, tx, committedBlock)
}

// findExecutedDiffsetAtHeight returns the diffset of the block executed at currentBlock.
// When no canonical hash is recorded at that height (e.g. the block is no longer canonical
// after a reorg) it falls back to the stored header.
func findExecutedDiffsetAtHeight(ctx context.Context, rwTx kv.TemporalRwTx, br services.FullBlockReader, doms *execctx.SharedDomains, currentBlock uint64) (diffSet [kv.DomainLen][]kv.DomainEntryDiff, executedHash common.Hash, found bool, err error) {
	executedHash, ok, err := br.CanonicalHash(ctx, rwTx, currentBlock)
	if err != nil {
		return diffSet, common.Hash{}, false, err
	}
	if !ok {
		// we may have executed blocks which are not in the canonical chain
		nonCanonicalHeaders, err := rawdb.ReadHeadersByNumber(rwTx, currentBlock)
		if err != nil {
			return diffSet, common.Hash{}, false, err
		}
		switch {
		case len(nonCanonicalHeaders) == 0:
			return diffSet, common.Hash{}, false, fmt.Errorf("can't find diffsets for: %d", currentBlock)
		case len(nonCanonicalHeaders) == 1:
			executedHash = nonCanonicalHeaders[0].Hash()
		default:
			return diffSet, common.Hash{}, false, fmt.Errorf("diffsets ambiguous for: %d, have %d headers", currentBlock, len(nonCanonicalHeaders))
		}
	}
	diffSet, found, err = doms.GetDiffset(rwTx, executedHash, currentBlock)
	if err != nil {
		return diffSet, common.Hash{}, false, err
	}
	return diffSet, executedHash, found, nil
}

func unwindExec3(u *UnwindState, s *StageState, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, ctx context.Context, cfg ExecuteBlockCfg, accumulator *shards.Accumulator, logger log.Logger) (err error) {
	br := cfg.blockReader

	// Boundary = last committed txNum (block u.UnwindPoint); re-execution still
	// resumes at u.UnwindPoint+1.
	txNum, err := overlayPruneToTxNum(ctx, cfg, rwTx, u.UnwindPoint)
	if err != nil {
		return err
	}

	t := time.Now()
	var changeSet *[kv.DomainLen][]kv.DomainEntryDiff
	for currentBlock := u.CurrentBlockNumber; currentBlock > u.UnwindPoint; currentBlock-- {
		currentKeys, executedHash, found, err := findExecutedDiffsetAtHeight(ctx, rwTx, br, doms, currentBlock)
		if err != nil {
			return err
		}
		if !found {
			if changeSet == nil {
				// this handles the edge case where we're traversing backwards from
				// the current block and we've not found the first diff yet it just
				// means the current block(s) has not been processed yet so has no
				// state.  This can only happen at the start of the traversal once
				// one processed block has been found there should be diffsets for
				// all previous blocks
				continue
			}
			return fmt.Errorf("domains.GetDiffset(%d, %s): not found", currentBlock, executedHash)
		}
		if changeSet == nil {
			changeSet = &currentKeys
		} else {
			for i := range currentKeys {
				changeSet[i] = changeset.MergeDiffSets(changeSet[i], currentKeys[i])
			}
		}
	}
	// Get the hash of the last executed block (the tip we're unwinding from).
	lastExecHash, _, err := br.CanonicalHash(ctx, rwTx, u.CurrentBlockNumber)
	if err != nil {
		lastExecHash = common.Hash{}
	}
	if err := unwindExec3State(ctx, doms, rwTx, u.UnwindPoint, txNum, accumulator, changeSet, lastExecHash, logger); err != nil {
		return fmt.Errorf("unwindExec3State(%d->%d): %w, took %s", s.BlockNumber, u.UnwindPoint, err, time.Since(t))
	}
	if err := rawdb.DeleteNewerEpochs(rwTx, u.UnwindPoint+1); err != nil {
		return fmt.Errorf("delete newer epochs: %w", err)
	}
	return nil
}

var mxState3Unwind = metrics.GetOrCreateSummary("state3_unwind")

func unwindExec3State(ctx context.Context,
	sd *execctx.SharedDomains, tx kv.TemporalRwTx,
	blockUnwindTo, txUnwindTo uint64,
	accumulator *shards.Accumulator,
	changeset *[kv.DomainLen][]kv.DomainEntryDiff, lastExecutedBlockHash common.Hash, logger log.Logger) error {
	st := time.Now()
	defer mxState3Unwind.ObserveDuration(st)
	var currentInc uint64

	//TODO: why we don't call accumulator.ChangeCode???
	handle := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		//TODO: This is broken - becuase it does not handle the way value changes
		// for previous steps are represented - they will pass nil values here
		// which will look like a delete (12/11/25 - I've not fixed this as it has
		// been here for a while and I'm not sure what if anything recieves these
		// changes at what it does with them)
		if len(k) == length.Addr {
			if len(v) > 0 {
				var account accounts.Account
				if err := accounts.DeserialiseV3(&account, v); err != nil {
					return fmt.Errorf("%w, %x", err, v)
				}
				var address common.Address
				copy(address[:], k)
				newV := accounts.SerialiseV3(&account)
				if accumulator != nil {
					accumulator.ChangeAccount(address, account.Incarnation, newV)
				}
			} else {
				var address common.Address
				copy(address[:], k)
				if accumulator != nil {
					accumulator.DeleteAccount(address)
				}
			}
			return nil
		}

		var address common.Address
		var location common.Hash
		copy(address[:], k[:length.Addr])
		copy(location[:], k[length.Addr:])
		if accumulator != nil {
			accumulator.ChangeStorage(address, currentInc, location, common.Copy(v))
		}
		if dbg.TraceUnwinds && dbg.TraceDomain(uint16(kv.StorageDomain)) {
			if v == nil {
				fmt.Printf("unwind (Block:%d,Tx:%d): storage [%x %x] => [empty]\n", blockUnwindTo, txUnwindTo, address, location)
			} else {
				fmt.Printf("unwind (Block:%d,Tx:%d): storage [%x %x] => [%x]\n", blockUnwindTo, txUnwindTo, address, location, v)
			}
		}
		return nil
	}

	stateChanges := etl.NewCollector("", "", etl.NewOldestEntryBuffer(etl.BufferOptimalSize), logger)
	defer stateChanges.Close()
	stateChanges.SortAndFlushInBackground(true)

	if changeset != nil {
		accountDiffs := changeset[kv.AccountsDomain]
		for _, entry := range accountDiffs {
			if dbg.TraceUnwinds && dbg.TraceDomain(uint16(kv.AccountsDomain)) {
				address := entry.Key[:len(entry.Key)-8]
				keyStep := ^binary.BigEndian.Uint64([]byte(entry.Key[len(entry.Key)-8:]))
				if entry.Value != nil && len(entry.Value) > 0 {
					var account accounts.Account
					if err := accounts.DeserialiseV3(&account, entry.Value); err == nil {
						fmt.Printf("unwind (Block:%d,Tx:%d): acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}, step: %d\n", blockUnwindTo, txUnwindTo, address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash, keyStep)
					}
				} else if entry.Value == nil {
					fmt.Printf("unwind (Block:%d,Tx:%d): acc %x: [different step], step: %d\n", blockUnwindTo, txUnwindTo, address, keyStep)
				} else {
					fmt.Printf("unwind (Block:%d,Tx:%d): del acc: %x, step: %d\n", blockUnwindTo, txUnwindTo, address, keyStep)
				}
			}
			if err := stateChanges.Collect(common.ToBytesZeroCopy(entry.Key)[:length.Addr], entry.Value); err != nil {
				return err
			}
		}
		storageDiffs := changeset[kv.StorageDomain]
		for _, kv := range storageDiffs {
			if err := stateChanges.Collect(common.ToBytesZeroCopy(kv.Key), kv.Value); err != nil {
				return err
			}
		}

		commitmentDiffs := changeset[kv.CommitmentDomain]

		if dbg.TraceUnwinds && dbg.TraceDomain(uint16(kv.CommitmentDomain)) {
			for _, entry := range commitmentDiffs {
				if entry.Value == nil {
					fmt.Printf("unwind (Block:%d,Tx:%d): commitment [%x] => [empty]\n", blockUnwindTo, txUnwindTo, entry.Key[:len(entry.Key)-8])
				} else {
					if entry.Key[:len(entry.Key)-8] == "state" {
						fmt.Printf("unwind (Block:%d,Tx:%d): commitment [%s] => [%x]\n", blockUnwindTo, txUnwindTo, entry.Key[:len(entry.Key)-8], entry.Value)
					} else {
						fmt.Printf("unwind (Block:%d,Tx:%d): commitment [%x] => [%x]\n", blockUnwindTo, txUnwindTo, entry.Key[:len(entry.Key)-8], entry.Value)
					}
				}
			}
		}

		if err := stateChanges.Load(tx, "", handle, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
			return err
		}

	}

	sd.Unwind(txUnwindTo, changeset)
	sd.SetTxNum(txUnwindTo)
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

func SpawnExecuteBlocksStage(s *StageState, u Unwinder, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, toBlock uint64, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) (err error) {
	if dbg.StagesOnlyBlocks {
		return nil
	}

	prevStageProgress, err := stageProgress(rwTx, cfg.db, stages.Senders)
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

	if err := ExecV3(ctx, s, u, cfg, doms, rwTx, dbg.Exec3Parallel || cfg.experimentalBAL, to, logger); err != nil {
		return err
	}
	return nil
}

func UnwindExecutionStage(u *UnwindState, s *StageState, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, ctx context.Context, cfg ExecuteBlockCfg, logger log.Logger) (err error) {
	//fmt.Printf("unwind: %d -> %d\n", u.CurrentBlockNumber, u.UnwindPoint)
	if u.UnwindPoint >= s.BlockNumber {
		// MDBX has nothing above u.UnwindPoint to roll back here, but the in-RAM
		// overlay (reused across the unwind→retry loop) can still hold uncommitted
		// writes for blocks above it — e.g. a block that failed its post-execution
		// gas check before its step was flushed. This early return skips u.Done, so
		// committed progress stays at s.BlockNumber and the whole
		// (s.BlockNumber, u.UnwindPoint] range re-executes from there; prune the
		// overlay to the last committed txNum, Max(s.BlockNumber), so no re-executed
		// block re-reads its own uncommitted write.
		committedTxNum, err := overlayPruneToTxNum(ctx, cfg, rwTx, s.BlockNumber)
		if err != nil {
			return err
		}
		resumeTxNum, err := cfg.blockReader.TxnumReader().Min(ctx, rwTx, s.BlockNumber+1)
		if err != nil {
			return err
		}
		// NB: do NOT ResetPendingUpdates / SeekCommitment here — the overlay and its
		// deferred commitment updates are reused by the in-loop re-execution, so
		// discarding them strands the commitment context and stalls the next block.
		doms.Unwind(committedTxNum, nil)
		doms.SetTxNum(resumeTxNum)
		return nil
	}

	logger.Info(fmt.Sprintf("[%s] Unwind Execution", u.LogPrefix()), "from", s.BlockNumber, "to", u.UnwindPoint)

	// Discard any pending deferred commitment updates from the previous
	// (failed) execution. If left in place, the next ComputeCommitment
	// would flush stale branch data that doesn't match the unwound state.
	doms.ResetPendingUpdates()

	unwindToLimit, ok, err := rawtemporaldb.CanUnwindBeforeBlockNum(u.UnwindPoint, rwTx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %d < %d", ErrTooDeepUnwind, u.UnwindPoint, unwindToLimit)
	}

	var accumulator *shards.Accumulator
	if cfg.stateStream && s.BlockNumber-u.UnwindPoint < stateStreamLimit {
		accumulator = cfg.notifications.Accumulator

		hash, ok, err := cfg.blockReader.CanonicalHash(ctx, rwTx, u.UnwindPoint)
		if err != nil {
			return fmt.Errorf("read canonical hash of unwind point: %w", err)
		}
		if !ok {
			return fmt.Errorf("canonical hash not found %d", u.UnwindPoint)
		}
		header, err := cfg.blockReader.HeaderByHash(ctx, rwTx, hash)
		if err != nil {
			return fmt.Errorf("read canonical header of unwind point: %w", err)
		}
		if header == nil {
			return fmt.Errorf("canonical header for unwind point not found: %s", hash)
		}
		txs, err := cfg.blockReader.RawTransactions(ctx, rwTx, u.UnwindPoint, s.BlockNumber)
		if err != nil {
			return err
		}
		accumulator.StartChange(header, txs, true)
	}

	if err := unwindExec3(u, s, doms, rwTx, ctx, cfg, accumulator, logger); err != nil {
		return err
	}

	if err = u.Done(rwTx); err != nil {
		return err
	}

	// Restore doms' internal commitment state to the unwound tip; a failure here
	// leaves doms inconsistent for the next re-execution, so surface it.
	if _, _, err = doms.SeekCommitment(ctx, rwTx); err != nil {
		return fmt.Errorf("unwind: SeekCommitment after disk unwind: %w", err)
	}
	//dumpPlainStateDebug(tx, nil)
	return nil
}

func PruneExecutionStage(ctx context.Context, s *PruneState, tx kv.RwTx, cfg ExecuteBlockCfg, timeout time.Duration, logger log.Logger) (err error) {
	if dbg.NoPrune() {
		return s.Done(tx)
	}
	// on chain-tip:
	//  - can prune only between blocks (without blocking blocks processing)
	//  - need also leave some time to prune blocks
	//  - need keep "fsync" time of db fast
	// Means - the best is:
	//  - stop prune when `tx.SpaceDirty()` is big
	//  - and set ~500ms timeout
	// because on slow disks - prune is slower. but for now - let's tune for nvme first, and add `tx.SpaceDirty()` check later https://github.com/erigontech/erigon/issues/11635
	// 2026-04: tip-mode commitment-domain prune throughput exceeded the prior
	// /2 budget. Use a base budget of one-third of a slot and extend it
	// adaptively when there is a large prunable backlog, capped at two-thirds
	// of a slot so FCU still has time. The proper fix is a background prune
	// that defers to FCU when work is pending — out of scope here.
	baseTimeout := time.Duration(cfg.chainConfig.SecondsPerSlot()*1000/3) * time.Millisecond
	maxTimeout := time.Duration(cfg.chainConfig.SecondsPerSlot()*2000/3) * time.Millisecond
	stagePruneTimeout := baseTimeout
	if hasAgg, ok := cfg.db.(state.HasAgg); ok {
		if agg, ok := hasAgg.Agg().(*state.Aggregator); ok && agg != nil {
			// Each 100 prunable steps adds 200ms. 1000-step backlog -> +2s.
			extra := time.Duration(agg.MaxPrunableStepsBacklog()/100) * 200 * time.Millisecond
			stagePruneTimeout = baseTimeout + extra
			if stagePruneTimeout > maxTimeout {
				stagePruneTimeout = maxTimeout
			}
		}
	}
	if timeout > 0 && timeout > stagePruneTimeout {
		stagePruneTimeout = timeout
	}

	pruneDiffsLimit := 200_000
	pruneBalLimit := 10_000
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneDiffsLimit = math.MaxInt
		pruneBalLimit = math.MaxInt
		stagePruneTimeout = 12 * time.Hour
	}

	stagePruneStartTime := time.Now()
	remainingPruneTimeout := func() time.Duration {
		// Initial-cycle pruning is aggressive: there is no FCU to leave time for, so
		// each prune step gets the full long budget instead of sharing it. Sharing
		// here would let earlier steps eat into the budget and drop PruneSmallBatches
		// below the furious-prune threshold (>5h) or skip it entirely.
		if s.CurrentSyncCycle.IsInitialCycle {
			return stagePruneTimeout
		}
		remaining := stagePruneTimeout - time.Since(stagePruneStartTime)
		if remaining <= 0 {
			return 0
		}
		return remaining
	}

	// AlwaysGenerateChangesets disables this prune so the node retains
	// changesets for unwinds deeper than MaxReorgDepth (debug / integration
	// tool / explicit --experimental.always-generate-changesets flag).
	// Without the guard, the flag still controls *generation* but every
	// generated changeset is pruned 96 blocks later, defeating the point.
	if s.ForwardProgress > cfg.syncCfg.MaxReorgDepth && !cfg.syncCfg.AlwaysGenerateChangesets {
		// (chunkLen is 8Kb) * (1_000 chunks) = 8mb
		// Some blocks on bor-mainnet have 400 chunks of diff = 3mb
		if pruneChangeSetsTimeout := remainingPruneTimeout(); pruneChangeSetsTimeout > 0 {
			pruneChangeSetsStartTime := time.Now()
			if err := rawdb.PruneTable(
				tx,
				kv.ChangeSets3,
				s.ForwardProgress-cfg.syncCfg.MaxReorgDepth,
				ctx,
				pruneDiffsLimit,
				pruneChangeSetsTimeout,
				logger,
				s.LogPrefix(),
			); err != nil {
				return err
			}
			if duration := time.Since(pruneChangeSetsStartTime); duration > pruneChangeSetsTimeout {
				logger.Debug(
					fmt.Sprintf("[%s] prune changesets timing", s.LogPrefix()),
					"duration", duration,
					"timeout", pruneChangeSetsTimeout,
					"initialCycle", s.CurrentSyncCycle.IsInitialCycle,
				)
			}
		}
	}

	if s.ForwardProgress > cfg.syncCfg.MaxReorgDepth {
		if pruneTimeout := remainingPruneTimeout(); pruneTimeout > 0 {
			if err := rawdb.PruneTable(
				tx,
				kv.BlockAccessList,
				s.ForwardProgress-cfg.syncCfg.MaxReorgDepth,
				ctx,
				pruneBalLimit,
				pruneTimeout,
				logger,
				s.LogPrefix(),
			); err != nil {
				return err
			}
		}
	}

	agg := cfg.db.(state.HasAgg).Agg().(*state.Aggregator)
	mxExecStepsInDB.Set(rawdbhelpers.IdxStepsCountV3(tx, agg.StepSize()) * 100)

	if pruneTimeout := remainingPruneTimeout(); pruneTimeout > 0 {
		if _, err := tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, pruneTimeout); err != nil {
			return err
		}
	}
	if duration := time.Since(stagePruneStartTime); duration > stagePruneTimeout {
		logger.Debug(
			fmt.Sprintf("[%s] prune execution timing", s.LogPrefix()),
			"duration", duration,
			"timeout", stagePruneTimeout,
			"initialCycle", s.CurrentSyncCycle.IsInitialCycle,
		)
	}
	if err = s.Done(tx); err != nil {
		return err
	}
	return nil
}
