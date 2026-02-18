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
	"unsafe"

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
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/node/silkworm"
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
	hd            headerDownloader
	author        accounts.Address
	// last valid number of the stage

	dirs      datadir.Dirs
	historyV3 bool
	syncCfg   ethconfig.Sync
	genesis   *types.Genesis

	silkworm        *silkworm.Silkworm
	experimentalBAL bool
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
	hd headerDownloader,
	genesis *types.Genesis,
	syncCfg ethconfig.Sync,
	silkworm *silkworm.Silkworm,
	experimentalBAL bool,
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
		hd:              hd,
		genesis:         genesis,
		historyV3:       true,
		syncCfg:         syncCfg,
		silkworm:        silkworm,
		experimentalBAL: experimentalBAL,
	}
}

// ================ Erigon3 ================

var ErrTooDeepUnwind = errors.New("too deep unwind")

func unwindExec3(u *UnwindState, s *StageState, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, ctx context.Context, cfg ExecuteBlockCfg, accumulator *shards.Accumulator, logger log.Logger) (err error) {
	br := cfg.blockReader
	txNumsReader := br.TxnumReader()

	// unwind all txs of u.UnwindPoint block. 1 txn in begin/end of block - system txs
	txNum, err := txNumsReader.Min(ctx, rwTx, u.UnwindPoint+1)
	if err != nil {
		return err
	}

	t := time.Now()
	var changeSet *[kv.DomainLen][]kv.DomainEntryDiff
	for currentBlock := u.CurrentBlockNumber; currentBlock > u.UnwindPoint; currentBlock-- {
		currentHash, ok, err := br.CanonicalHash(ctx, rwTx, currentBlock)
		if err != nil {
			return err
		}
		if !ok {
			// we may have executed blocks which are not in the canonical chain
			nonCanonicalHeaders, err := rawdb.ReadHeadersByNumber(rwTx, currentBlock)
			if err != nil {
				return err
			}
			switch {
			case len(nonCanonicalHeaders) == 0:
				return fmt.Errorf("can't find diffsets for: %d", currentBlock)
			case len(nonCanonicalHeaders) == 1:
				currentHash = nonCanonicalHeaders[0].Hash()
			default:
				return fmt.Errorf("diffsets ambiguous for: %d, have %d headers", currentBlock, len(nonCanonicalHeaders))
			}
		}
		var currentKeys [kv.DomainLen][]kv.DomainEntryDiff
		currentKeys, ok, err = doms.GetDiffset(rwTx, currentHash, currentBlock)
		if !ok {
			if changeSet == nil {
				// this handles the edge case where we're traversing backwards from
				// the current block and we've not found the first diff yet it just
				// means the current block(s) has not been processed yet so has no
				// state.  This can only happen at the start of the traversal once
				// one processed block has been found there should be diffsets for
				// all previous blocks
				continue
			}
			return fmt.Errorf("domains.GetDiffset(%d, %s): not found", currentBlock, currentHash)
		}
		if err != nil {
			return err
		}
		if changeSet == nil {
			changeSet = &currentKeys
		} else {
			for i := range currentKeys {
				changeSet[i] = changeset.MergeDiffSets(changeSet[i], currentKeys[i])
			}
		}
	}
	// Get the hash of the last executed block (the tip we're unwinding from)
	// so RevertWithDiffset can detect if the cache was modified by a rolled-back tx.
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

	// Invalidate state cache entries affected by the unwind.
	// Pass the hash of the last executed block so RevertWithDiffset can detect
	// if the cache was modified by a rolled-back tx (e.g. ValidatePayload).
	if stateCache := sd.GetStateCache(); stateCache != nil {
		unwindToHash, err := rawdb.ReadCanonicalHash(tx, blockUnwindTo)
		if err != nil {
			logger.Warn("failed to read canonical hash for cache update", "block", blockUnwindTo, "err", err)
			unwindToHash = common.Hash{}
		}
		stateCache.RevertWithDiffset(changeset, lastExecutedBlockHash, unwindToHash)
	}
	if changeset != nil {
		accountDiffs := changeset[kv.AccountsDomain]
		for _, entry := range accountDiffs {
			if dbg.TraceUnwinds && dbg.TraceDomain(uint16(kv.AccountsDomain)) {
				address := entry.Key[:len(entry.Key)-8]
				keyStep := ^binary.BigEndian.Uint64([]byte(entry.Key[len(entry.Key)-8:]))
				prevStep := ^binary.BigEndian.Uint64(entry.PrevStepBytes)
				if len(entry.Value) > 0 {
					var account accounts.Account
					if err := accounts.DeserialiseV3(&account, entry.Value); err == nil {
						fmt.Printf("unwind (Block:%d,Tx:%d): acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}, step: %d, prev: %d\n", blockUnwindTo, txUnwindTo, address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash, keyStep, prevStep)
					}
				} else {
					if keyStep != prevStep {
						if prevStep == 0 {
							fmt.Printf("unwind (Block:%d,Tx:%d): acc %x: [empty], step: %d\n", blockUnwindTo, txUnwindTo, address, keyStep)
						} else {
							fmt.Printf("unwind (Block:%d,Tx:%d): acc: %x, in prev step: {key: %d, prev: %d}\n", blockUnwindTo, txUnwindTo, address, keyStep, prevStep)
						}
					} else {
						fmt.Printf("unwind (Block:%d,Tx:%d): del acc: %x, step: %d\n", blockUnwindTo, txUnwindTo, address, keyStep)
					}
				}
			}
			if err := stateChanges.Collect(toBytesZeroCopy(entry.Key)[:length.Addr], entry.Value); err != nil {
				return err
			}
		}
		storageDiffs := changeset[kv.StorageDomain]
		for _, kv := range storageDiffs {
			if err := stateChanges.Collect(toBytesZeroCopy(kv.Key), kv.Value); err != nil {
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
	sd.SetBlockNum(blockUnwindTo)
	return nil
}

func toBytesZeroCopy(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }

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
		return nil
	}

	logger.Info(fmt.Sprintf("[%s] Unwind Execution", u.LogPrefix()), "from", s.BlockNumber, "to", u.UnwindPoint, "stack", dbg.Stack())

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

	_, _, _ = doms.SeekCommitment(ctx, rwTx) // ensure internal state of `doms` is set
	//dumpPlainStateDebug(tx, nil)
	return nil
}

func PruneExecutionStage(ctx context.Context, s *PruneState, tx kv.RwTx, cfg ExecuteBlockCfg, timeout time.Duration, logger log.Logger) (err error) {
	// on chain-tip:
	//  - can prune only between blocks (without blocking blocks processing)
	//  - need also leave some time to prune blocks
	//  - need keep "fsync" time of db fast
	// Means - the best is:
	//  - stop prune when `tx.SpaceDirty()` is big
	//  - and set ~500ms timeout
	// because on slow disks - prune is slower. but for now - let's tune for nvme first, and add `tx.SpaceDirty()` check later https://github.com/erigontech/erigon/issues/11635
	quickPruneTimeout := time.Duration(cfg.chainConfig.SecondsPerSlot()*1000/3) * time.Millisecond / 2

	if timeout > 0 && timeout > quickPruneTimeout {
		quickPruneTimeout = timeout
	}

	if s.ForwardProgress > cfg.syncCfg.MaxReorgDepth && !cfg.syncCfg.AlwaysGenerateChangesets {
		// (chunkLen is 8Kb) * (1_000 chunks) = 8mb
		// Some blocks on bor-mainnet have 400 chunks of diff = 3mb
		var pruneDiffsLimitOnChainTip = 1_000
		pruneTimeout := quickPruneTimeout
		if s.CurrentSyncCycle.IsInitialCycle {
			pruneDiffsLimitOnChainTip = math.MaxInt
			pruneTimeout = time.Hour
		}
		pruneChangeSetsStartTime := time.Now()
		if err := rawdb.PruneTable(
			tx,
			kv.ChangeSets3,
			s.ForwardProgress-cfg.syncCfg.MaxReorgDepth,
			ctx,
			pruneDiffsLimitOnChainTip,
			pruneTimeout,
			logger,
			s.LogPrefix(),
		); err != nil {
			return err
		}
		if duration := time.Since(pruneChangeSetsStartTime); duration > quickPruneTimeout {
			logger.Debug(
				fmt.Sprintf("[%s] prune changesets timing", s.LogPrefix()),
				"duration", duration,
				"initialCycle", s.CurrentSyncCycle.IsInitialCycle,
			)
		}
	}

	if s.ForwardProgress > cfg.syncCfg.MaxReorgDepth {
		pruneBalLimit := 10_000
		pruneTimeout := quickPruneTimeout
		if s.CurrentSyncCycle.IsInitialCycle {
			pruneBalLimit = math.MaxInt
			pruneTimeout = time.Hour
		}
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

	agg := cfg.db.(state.HasAgg).Agg().(*state.Aggregator)
	mxExecStepsInDB.Set(rawdbhelpers.IdxStepsCountV3(tx, agg.StepSize()) * 100)

	pruneTimeout := quickPruneTimeout
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneTimeout = 12 * time.Hour
	}

	pruneSmallBatchesStartTime := time.Now()
	if _, err := tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, pruneTimeout); err != nil {
		return err
	}
	if duration := time.Since(pruneSmallBatchesStartTime); duration > quickPruneTimeout {
		logger.Debug(
			fmt.Sprintf("[%s] prune small batches timing", s.LogPrefix()),
			"duration", duration,
			"initialCycle", s.CurrentSyncCycle.IsInitialCycle,
		)
	}
	if err = s.Done(tx); err != nil {
		return err
	}
	return nil
}
