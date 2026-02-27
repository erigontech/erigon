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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/cmp"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/seboost"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/shards"
)

// Cases:
//  1. Snapshots > ExecutionStage: snapshots can have half-block data `10.4`. Get right txNum from SharedDomains (after SeekCommitment)
//  2. ExecutionStage > Snapshots: no half-block data possible. Rely on DB.
func restoreTxNum(ctx context.Context, cfg *ExecuteBlockCfg, applyTx kv.Tx, currentTxNum uint64, maxBlockNum uint64) (
	inputTxNum uint64, maxTxNum uint64, offsetFromBlockBeginning uint64, blockNum uint64, err error) {

	txNumsReader := cfg.blockReader.TxnumReader()

	inputTxNum = currentTxNum

	lastBlockNum, lastTxNum, err := txNumsReader.Last(applyTx)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if lastTxNum == inputTxNum {
		// nothing to exec - return last committed block so caller can sync stage progress
		return 0, 0, 0, lastBlockNum, nil
	}

	maxTxNum, err = txNumsReader.Max(ctx, applyTx, maxBlockNum)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	blockNum, ok, err := txNumsReader.FindBlockNum(ctx, applyTx, currentTxNum)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if !ok {
		lb, lt, _ := txNumsReader.Last(applyTx)
		fb, ft, _ := txNumsReader.First(applyTx)
		return 0, 0, 0, 0, fmt.Errorf("seems broken TxNums index not filled. can't find blockNum of txNum=%d; in db: (%d-%d, %d-%d)", inputTxNum, fb, lb, ft, lt)
	}
	{
		max, _ := txNumsReader.Max(ctx, applyTx, blockNum)
		if currentTxNum == max {
			blockNum++
		}
	}

	min, err := txNumsReader.Min(ctx, applyTx, blockNum)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	if currentTxNum > min {
		// if stopped in the middle of the block: start from beginning of block.
		// first part will be executed in HistoryExecution mode
		offsetFromBlockBeginning = currentTxNum - min
	}

	inputTxNum = min

	//_max, _ := txNumsReader.Max(applyTx, blockNum)
	//fmt.Printf("[commitment] found domain.txn %d, inputTxn %d, offset %d. DB found block %d {%d, %d}\n", currentTxNum, inputTxNum, offsetFromBlockBeginning, blockNum, _min, _max)
	return inputTxNum, maxTxNum, offsetFromBlockBeginning, blockNum, nil
}

func ExecV3(ctx context.Context,
	execStage *StageState, u Unwinder, cfg ExecuteBlockCfg,
	doms *execctx.SharedDomains, rwTx kv.TemporalRwTx,
	parallel bool, //nolint
	maxBlockNum uint64,
	logger log.Logger) (execErr error) {
	isBlockProduction := execStage.SyncMode() == stages.ModeBlockProduction
	isForkValidation := execStage.SyncMode() == stages.ModeForkValidation

	isApplyingBlocks := execStage.SyncMode() == stages.ModeApplyingBlocks
	initialCycle := execStage.CurrentSyncCycle.IsInitialCycle
	hooks := cfg.vmConfig.Tracer
	applyTx := rwTx
	initialTxNum, blockNum, err := doms.SeekCommitment(ctx, applyTx)
	if err != nil {
		return err
	}

	agg := cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	if isApplyingBlocks {
		if initialCycle {
			agg.SetCollateAndBuildWorkers(dbg.CollateWorkers) //TODO: Need always set to CollateWorkers=2 (on ChainTip too). But need more tests first
			agg.SetCompressWorkers(dbg.CompressWorkers)
			agg.SetMergeWorkers(dbg.MergeWorkers) //TODO: Need always set to CollateWorkers=2 (on ChainTip too). But need more tests first
		} else {
			agg.SetCollateAndBuildWorkers(1)
			agg.SetCompressWorkers(dbg.CompressWorkers)
			agg.SetMergeWorkers(dbg.MergeWorkers) //TODO: Need always set to CollateWorkers=2 (on ChainTip too). But need more tests first
		}
	}

	if maxBlockNum < blockNum {
		return nil
	}

	if execStage.SyncMode() == stages.ModeApplyingBlocks {
		agg.BuildFilesInBackground(initialTxNum)
	}

	var (
		inputTxNum               uint64
		offsetFromBlockBeginning uint64
		maxTxNum                 uint64
	)

	if inputTxNum, maxTxNum, offsetFromBlockBeginning, blockNum, err = restoreTxNum(ctx, &cfg, applyTx, initialTxNum, maxBlockNum); err != nil {
		return err
	}

	if maxTxNum == 0 {
		// nothing to exec, make sure the stage is in sync with the sd
		if execStage.BlockNumber < blockNum {
			return execStage.Update(rwTx, blockNum)
		}
		return nil
	}

	shouldReportToTxPool := cfg.notifications != nil && !isBlockProduction && maxBlockNum <= blockNum+64
	var accumulator *shards.Accumulator
	if shouldReportToTxPool {
		accumulator = cfg.notifications.Accumulator
		if accumulator == nil {
			accumulator = shards.NewAccumulator()
		}
	}
	rs := state.NewStateV3Buffered(state.NewStateV3(doms, cfg.syncCfg, logger))

	commitThreshold := cfg.batchSize.Bytes()

	logInterval := 20 * time.Second
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	defer resetExecGauges(ctx)
	defer resetCommitmentGauges(ctx)
	defer resetDomainGauges(ctx)

	stepsInDb := rawdbhelpers.IdxStepsCountV3(applyTx, applyTx.Debug().StepSize())

	if maxBlockNum < blockNum {
		return nil
	}

	var lastHeader *types.Header

	var readAhead chan uint64

	startBlockNum := blockNum
	blockLimit := uint64(cfg.syncCfg.LoopBlockLimit)

	var lastCommittedTxNum uint64
	var lastCommittedBlockNum uint64

	doms.EnableParaTrieDB(cfg.db)
	doms.EnableTrieWarmup(true)
	isChainTip := maxBlockNum == startBlockNum
	// Do it only for chain-tip blocks!
	doms.EnableWarmupCache(isChainTip)
	//log.Debug("Warmup Cache", "enabled", isChainTip)
	postValidator := newBlockPostExecutionValidator()
	doms.SetDeferCommitmentUpdates(false)
	if isChainTip {
		postValidator = newParallelBlockPostExecutionValidator()
	}
	// Enable deferred commitment updates only for serial execution (fork validation).
	// The parallel executor has a subtle interaction with deferred updates that causes
	// intermittent trie root mismatches during re-org validation.
	if !parallel && isForkValidation {
		doms.SetDeferCommitmentUpdates(true)
	}
	defer doms.SetDeferCommitmentUpdates(false)
	// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
	// can't use OS-level ReadAhead - because Data >> RAM
	// it also warmsup state a bit - by touching senders/coninbase accounts and code
	if !execStage.CurrentSyncCycle.IsInitialCycle && !isChainTip {
		var clean func()

		readAhead, clean = exec.BlocksReadAhead(ctx, 2, cfg.db, cfg.engine, cfg.blockReader)
		defer clean()
	}
	if parallel {
		pe := &parallelExecutor{
			txExecutor: txExecutor{
				cfg:               cfg,
				rs:                rs,
				doms:              doms,
				agg:               agg,
				isBlockProduction: isBlockProduction,
				isForkValidation:  isForkValidation,
				isApplyingBlocks:  isApplyingBlocks,
				logger:            logger,
				logPrefix:         execStage.LogPrefix(),
				progress:          NewProgress(blockNum, inputTxNum, commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey: execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:             hooks,
				postValidator:     postValidator,
			},
			workerCount: cfg.syncCfg.ExecWorkerCount,
		}
		pe.lastCommittedTxNum.Store(inputTxNum)
		// blockNum is the next block to execute (from doms.BlockNum()), so the last
		// committed block is blockNum-1. LogCommitments uses Add to accumulate deltas
		// on top of this value, so initializing to blockNum would double-count.
		if blockNum > 0 {
			pe.lastCommittedBlockNum.Store(blockNum - 1)
		}

		// seboost txdeps consumption
		if os.Getenv("SEBOOST_LOAD") == "txdeps" {
			seboostDir := os.Getenv("SEBOOST_DIR")
			if seboostDir == "" {
				seboostDir = filepath.Join(cfg.dirs.DataDir, "seboost_txdeps")
			}
			sr := seboost.NewReader(seboostDir, logger)
			pe.seboostReader = sr
			defer sr.Close()
			logger.Info("seboost: consumption enabled", "dir", seboostDir)
		}

		// seboost txdeps generation
		if os.Getenv("SEBOOST_GENERATE") == "txdeps" {
			seboostDir := os.Getenv("SEBOOST_DIR")
			if seboostDir == "" {
				seboostDir = filepath.Join(cfg.dirs.DataDir, "seboost_txdeps")
			}
			homeDir, _ := os.UserHomeDir()
			csvPath := filepath.Join(homeDir, "seboost-stats", "txdeps-sizes.csv")
			sw, err := seboost.NewWriter(seboostDir, csvPath, logger)
			if err != nil {
				logger.Warn("seboost: failed to create writer", "err", err)
			} else {
				pe.seboostWriter = sw
				defer sw.Close()
				logger.Info("seboost: generation enabled", "dir", seboostDir, "csv", csvPath)
			}
		}

		defer func() {
			pe.LogComplete(stepsInDb)
		}()

		lastHeader, applyTx, execErr = pe.exec(ctx, execStage, u, startBlockNum, offsetFromBlockBeginning, maxBlockNum, blockLimit,
			initialTxNum, inputTxNum, initialCycle, applyTx, stepsInDb, accumulator, readAhead, logEvery)

		lastCommittedBlockNum = pe.lastCommittedBlockNum.Load()
		lastCommittedTxNum = pe.lastCommittedTxNum.Load()
	} else {
		se := &serialExecutor{
			txExecutor: txExecutor{
				cfg:               cfg,
				rs:                rs,
				doms:              doms,
				agg:               agg,
				u:                 u,
				isBlockProduction: isBlockProduction,
				isForkValidation:  isForkValidation,
				isApplyingBlocks:  isApplyingBlocks,
				applyTx:           applyTx,
				logger:            logger,
				logPrefix:         execStage.LogPrefix(),
				progress:          NewProgress(blockNum, inputTxNum, commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey: execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:             hooks,
				postValidator:     postValidator,
			}}
		se.lastCommittedTxNum.Store(inputTxNum)
		se.lastCommittedBlockNum.Store(blockNum)

		defer func() {
			if !isChainTip {
				se.LogComplete(stepsInDb)
			}
		}()

		lastHeader, applyTx, execErr = se.exec(ctx, execStage, u, startBlockNum, offsetFromBlockBeginning, maxBlockNum, blockLimit,
			initialTxNum, inputTxNum, initialCycle, applyTx, accumulator, readAhead, logEvery)

		if u != nil && !u.HasUnwindPoint() {
			if lastHeader != nil {
				switch {
				case execErr == nil || errors.Is(execErr, &ErrLoopExhausted{}):
					_, _, err = computeAndCheckCommitmentV3(ctx, lastHeader, applyTx, se.domains(), cfg, execStage, parallel, logger, u, isBlockProduction)
					if err != nil {
						return err
					}

					if err := se.getPostValidator().Wait(); err != nil {
						return err
					}

					se.lastCommittedBlockNum.Store(lastHeader.Number.Uint64())
					// Get current txNum from the last executed block
					currentTxNum, err := cfg.blockReader.TxnumReader().Max(ctx, applyTx, lastHeader.Number.Uint64())
					if err != nil {
						return err
					}
					committedTransactions := currentTxNum - se.lastCommittedTxNum.Load()
					se.lastCommittedTxNum.Store(currentTxNum)

					commitStart := time.Now()
					stepsInDb = rawdbhelpers.IdxStepsCountV3(applyTx, applyTx.Debug().StepSize())

					if initialCycle {
						se.LogCommitments(commitStart, 0, committedTransactions, 0, stepsInDb, commitment.CommitProgress{})
					}
				case errors.Is(execErr, ErrWrongTrieRoot):
					execErr = handleIncorrectRootHashError(
						lastHeader.Number.Uint64(), lastHeader.Hash(), lastHeader.ParentHash, applyTx, cfg, execStage, logger, u)
				default:
					return execErr
				}
			} else {
				if execErr != nil {
					switch {
					case errors.Is(execErr, ErrWrongTrieRoot):
						return fmt.Errorf("can't handle incorrect root err: %w", execErr)
					case errors.Is(execErr, &ErrLoopExhausted{}):
						break
					default:
						return execErr
					}
				} else {
					return fmt.Errorf("last processed block unexpectedly nil")
				}
			}
		}

		lastCommittedBlockNum = se.lastCommittedBlockNum.Load()
		lastCommittedTxNum = se.lastCommittedTxNum.Load()
	}

	if false && !isForkValidation {
		dumpPlainStateDebug(applyTx, doms)
	}

	lastCommitedStep := kv.Step((lastCommittedTxNum) / doms.StepSize())
	lastFrozenStep := applyTx.StepsInFiles(kv.CommitmentDomain)

	if lastCommitedStep > 0 && lastCommitedStep <= lastFrozenStep && !dbg.DiscardCommitment() {
		logger.Warn("["+execStage.LogPrefix()+"] can't persist comittement: txn step frozen",
			"block", lastCommittedBlockNum, "txNum", lastCommittedTxNum, "step", lastCommitedStep,
			"lastFrozenStep", lastFrozenStep, "lastFrozenTxNum", ((lastFrozenStep+1)*kv.Step(doms.StepSize()))-1)
		return fmt.Errorf("can't persist comittement for blockNum %d, txNum %d: step %d is frozen",
			lastCommittedBlockNum, lastCommittedTxNum, lastCommitedStep)
	}

	if !shouldReportToTxPool && cfg.notifications != nil && cfg.notifications.Accumulator != nil && !isBlockProduction && lastHeader != nil {
		// No reporting to the txn pool has been done since we are not within the "state-stream" window.
		// However, we should still at the very least report the last block number to it, so it can update its block progress.
		// Otherwise, we can get in a deadlock situation when there is a block building request in environments where
		// the Erigon process is the only block builder (e.g. some Hive tests, kurtosis testnets with one erigon block builder, etc.)
		cfg.notifications.Accumulator.StartChange(lastHeader, nil, false /* unwind */)
	}

	return execErr
}

func dumpTxIODebug(blockNum uint64, txIO *state.VersionedIO) {
	maxTxIndex := len(txIO.Inputs()) - 1

	for txIndex := -1; txIndex < maxTxIndex; txIndex++ {
		txIncarnation := txIO.ReadSetIncarnation(txIndex)

		fmt.Println(
			fmt.Sprintf("%d (%d.%d) RD", blockNum, txIndex, txIncarnation), txIO.ReadSet(txIndex).Len(),
			"WRT", len(txIO.WriteSet(txIndex)))

		var reads []*state.VersionedRead
		txIO.ReadSet(txIndex).Scan(func(vr *state.VersionedRead) bool {
			reads = append(reads, vr)
			return true
		})

		slices.SortFunc(reads, func(a, b *state.VersionedRead) int { return a.Address.Cmp(b.Address) })

		for _, vr := range reads {
			fmt.Println(fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation), "RD", vr.String())
		}

		writeSet := txIO.WriteSet(txIndex)
		writes := make([]*state.VersionedWrite, 0, len(writeSet))

		for _, vw := range writeSet {
			writes = append(writes, vw)
		}

		slices.SortFunc(writes, func(a, b *state.VersionedWrite) int { return a.Address.Cmp(b.Address) })

		for _, vw := range writes {
			fmt.Println(fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation), "WRT", vw.String())
		}
	}
}

type txExecutor struct {
	sync.RWMutex
	cfg               ExecuteBlockCfg
	agg               *dbstate.Aggregator
	rs                *state.StateV3Buffered
	doms              *execctx.SharedDomains
	u                 Unwinder
	isBlockProduction bool
	isForkValidation  bool
	isApplyingBlocks  bool
	applyTx           kv.TemporalTx
	logger            log.Logger
	logPrefix         string
	progress          *Progress
	taskExecMetrics   *exec.WorkerMetrics
	blockExecMetrics  *blockExecMetrics
	hooks             *tracing.Hooks

	lastExecutedBlockNum  atomic.Int64
	lastExecutedTxNum     atomic.Int64
	executedGas           atomic.Int64
	lastCommittedBlockNum atomic.Uint64
	lastCommittedTxNum    atomic.Uint64
	committedGas          atomic.Int64

	execLoopGroup *errgroup.Group

	execRequests chan *execRequest
	execCount    atomic.Int64
	abortCount   atomic.Int64
	invalidCount atomic.Int64
	readCount    atomic.Int64
	writeCount   atomic.Int64

	enableChaosMonkey bool
	postValidator     BlockPostExecutionValidator
	seboostReader     *seboost.Reader
}

func (te *txExecutor) readState() *state.StateV3Buffered {
	return te.rs
}

func (te *txExecutor) getPostValidator() BlockPostExecutionValidator {
	if te.postValidator == nil {
		return newBlockPostExecutionValidator()
	}
	return te.postValidator
}

func (te *txExecutor) domains() *execctx.SharedDomains {
	return te.doms
}

func (te *txExecutor) getHeader(ctx context.Context, hash common.Hash, number uint64) (h *types.Header, err error) {
	if te.applyTx != nil {
		err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
			h, err = te.cfg.blockReader.Header(ctx, te.applyTx, hash, number)
			return err
		})

		if err != nil {
			return nil, err
		}
	} else {
		if err := te.cfg.db.View(ctx, func(tx kv.Tx) (err error) {
			h, err = te.cfg.blockReader.Header(ctx, tx, hash, number)
			return err
		}); err != nil {
			return nil, err
		}
	}

	return h, nil
}

func (te *txExecutor) onBlockStart(ctx context.Context, blockNum uint64, blockHash common.Hash) {
	defer func() {
		if rec := recover(); rec != nil {
			te.logger.Warn("hook paniced: %s", rec, "stack", dbg.Stack())
		}
	}()

	if te.hooks == nil {
		return
	}

	if blockHash == (common.Hash{}) {
		te.logger.Warn("hooks ignored: zero block hash")
		return
	}

	if blockNum == 0 {
		if te.hooks.OnGenesisBlock != nil {
			var b *types.Block
			if err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
				b, err = te.cfg.blockReader.BlockByHash(ctx, tx, blockHash)
				return err
			}); err != nil {
				te.logger.Warn("hook: OnGenesisBlock: abandoned", "err", err)
			}
			te.hooks.OnGenesisBlock(b, te.cfg.genesis.Alloc)
		}
	} else {
		if te.hooks.OnBlockStart != nil {
			var b *types.Block
			var td *big.Int
			var finalized *types.Header
			var safe *types.Header

			if err := te.applyTx.Apply(ctx, func(tx kv.Tx) (err error) {
				b, err = te.cfg.blockReader.BlockByHash(ctx, tx, blockHash)
				if err != nil {
					return err
				}
				chainReader := exec.NewChainReader(te.cfg.chainConfig, te.applyTx, te.cfg.blockReader, te.logger)
				td = chainReader.GetTd(b.ParentHash(), b.NumberU64()-1)
				finalized = chainReader.CurrentFinalizedHeader()
				safe = chainReader.CurrentSafeHeader()
				return nil
			}); err != nil {
				te.logger.Warn("hook: OnBlockStart: abandoned", "err", err)
			}

			te.hooks.OnBlockStart(tracing.BlockEvent{
				Block:     b,
				TD:        td,
				Finalized: finalized,
				Safe:      safe,
			})
		}
	}
}

func (te *txExecutor) executeBlocks(ctx context.Context, tx kv.TemporalTx, startBlockNum uint64, maxBlockNum uint64, blockLimit uint64, initialTxNum uint64, inputTxNum uint64, readAhead chan uint64, initialCycle bool, applyResults chan applyResult) error {
	if te.execLoopGroup == nil {
		return errors.New("no exec group")
	}

	te.execLoopGroup.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("exec blocks panic: %s", rec)
			} else if err != nil && !errors.Is(err, context.Canceled) {
				err = fmt.Errorf("exec blocks error: %w", err)
			} else {
				te.logger.Debug("[" + te.logPrefix + "] exec blocks exit")
			}
		}()

		lastFrozenStep := tx.StepsInFiles(kv.CommitmentDomain)

		var lastFrozenTxNum uint64
		if lastFrozenStep > 0 {
			lastFrozenTxNum = uint64((lastFrozenStep+1)*kv.Step(te.doms.StepSize())) - 1
		}

		for blockNum := startBlockNum; blockNum <= maxBlockNum; blockNum++ {
			select {
			case readAhead <- blockNum:
			default:
			}

			var canonicalHash common.Hash
			if err = tx.Apply(ctx, func(applyTx kv.Tx) error {
				var e error
				canonicalHash, e = rawdb.ReadCanonicalHash(applyTx, blockNum)
				return e
			}); err != nil {
				return err
			}
			b, ok := exec.ReadBlockWithSendersFromGlobalReadAheader(canonicalHash)
			if b == nil || !ok {
				err = tx.Apply(ctx, func(tx kv.Tx) error {
					b, err = exec.BlockWithSenders(ctx, te.cfg.db, tx, te.cfg.blockReader, blockNum)
					return err
				})
			}
			if err != nil {
				return err
			}
			if b == nil {
				return fmt.Errorf("nil block %d", blockNum)
			}
			go warmTxsHashes(b)

			var dbBAL types.BlockAccessList
			var data []byte
			if err = tx.Apply(ctx, func(applyTx kv.Tx) error {
				var e error
				data, e = rawdb.ReadBlockAccessListBytes(applyTx, b.Hash(), blockNum)
				return e
			}); err != nil {
				return err
			}
			if len(data) > 0 {
				dbBAL, err = types.DecodeBlockAccessListBytes(data)
				if err != nil {
					return fmt.Errorf("decode block access list: %w", err)
				}
				if err := dbBAL.Validate(); err != nil {
					return fmt.Errorf("invalid block access list: %w", err)
				}
			}

			txs := b.Transactions()
			header := b.HeaderNoCopy()
			getHashFnMutex := sync.Mutex{}

			blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, func(hash common.Hash, number uint64) (h *types.Header, err error) {
				getHashFnMutex.Lock()
				defer getHashFnMutex.Unlock()
				err = tx.Apply(ctx, func(tx kv.Tx) (err error) {
					h, err = te.cfg.blockReader.Header(ctx, tx, hash, number)
					return err
				})

				if err != nil {
					return nil, err
				}

				return h, err
			}), te.cfg.engine, te.cfg.author, te.cfg.chainConfig)

			var txTasks []exec.Task

			// load seboost txdeps for this block if available
			var blockDeps [][]int
			if te.seboostReader != nil {
				blockDeps, _ = te.seboostReader.GetDeps(blockNum)
			}

			for txIndex := -1; txIndex <= len(txs); txIndex++ {
				if inputTxNum > 0 && inputTxNum <= initialTxNum {
					inputTxNum++
					continue
				}

				// Do not oversend, wait for the result heap to go under certain size
				txTask := &exec.TxTask{
					TxNum:           inputTxNum,
					TxIndex:         txIndex,
					Header:          header,
					Uncles:          b.Uncles(),
					Txs:             txs,
					EvmBlockContext: blockContext,
					Withdrawals:     b.Withdrawals(),
					// use history reader instead of state reader to catch up to the tx where we left off
					HistoryExecution: lastFrozenTxNum > 0 && inputTxNum <= lastFrozenTxNum,
					Config:           te.cfg.chainConfig,
					Engine:           te.cfg.engine,
					Trace:            dbg.TraceTx(blockNum, txIndex),
					Hooks:            te.hooks,
					Logger:           te.logger,
				}

				// inject seboost txdeps if available
				if blockDeps != nil {
					depIndex := txIndex + 1 // 0=system tx, 1=user tx 0, etc.
					if depIndex >= 0 && depIndex < len(blockDeps) && len(blockDeps[depIndex]) > 0 {
						txTask.SetDependencies(blockDeps[depIndex])
						te.logger.Trace("seboost: injecting deps", "block", blockNum, "txIndex", txIndex, "depCount", len(blockDeps[depIndex]))
					}
				}

				txTasks = append(txTasks, txTask)
				inputTxNum++
			}

			lastExecutedStep := kv.Step(inputTxNum / te.doms.StepSize())

			// if we're in the initialCycle before we consider the blockLimit we need to make sure we keep executing
			// until we reach a transaction whose comittement which is writable to the db, otherwise the update will get lost
			var exhausted *ErrLoopExhausted
			if !initialCycle || lastExecutedStep > 0 && lastExecutedStep > lastFrozenStep && !dbg.DiscardCommitment() {
				if blockLimit > 0 && blockNum-startBlockNum+1 >= blockLimit && blockNum != maxBlockNum {
					exhausted = &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block limit reached"}
				}
			}

			te.execRequests <- &execRequest{
				b.NumberU64(), b.Hash(),
				protocol.NewGasPool(b.GasLimit(), te.cfg.chainConfig.GetMaxBlobGasPerBlock(b.Time())),
				dbBAL, txTasks, applyResults, false, exhausted,
			}

			mxExecBlocks.Add(1)

			if exhausted != nil {
				break
			}
		}

		return nil
	})

	return nil
}

// nolint
func dumpPlainStateDebug(tx kv.TemporalRwTx, doms *execctx.SharedDomains) {
	if doms != nil {
		doms.Flush(context.Background(), tx)
	}

	{
		it, err := tx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, -1)
		if err != nil {
			panic(err)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			a := accounts.NewAccount()
			accounts.DeserialiseV3(&a, v)
			fmt.Printf("%x, %d, %d, %d, %x\n", k, &a.Balance, a.Nonce, a.Incarnation, a.CodeHash)
		}
	}
	{
		it, err := tx.Debug().RangeLatest(kv.StorageDomain, nil, nil, -1)
		if err != nil {
			panic(1)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%x, %x\n", k, v)
		}
	}
	{
		it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, -1)
		if err != nil {
			panic(1)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%x, %x\n", k, v)
			if bytes.Equal(k, []byte("state")) {
				fmt.Printf("state: t=%d b=%d\n", binary.BigEndian.Uint64(v[:8]), binary.BigEndian.Uint64(v[8:]))
			}
		}
	}
}

func handleIncorrectRootHashError(blockNumber uint64, blockHash common.Hash, parentHash common.Hash, applyTx kv.TemporalRwTx, cfg ExecuteBlockCfg, s *StageState, logger log.Logger, u Unwinder) error {
	if cfg.badBlockHalt {
		return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, blockNumber)
	}
	if cfg.hd != nil && cfg.hd.POSSync() {
		cfg.hd.ReportBadHeaderPoS(blockHash, parentHash)
	}
	minBlockNum := s.BlockNumber
	if blockNumber <= minBlockNum {
		return nil
	}

	unwindToLimit, err := rawtemporaldb.CanUnwindToBlockNum(applyTx)
	if err != nil {
		return err
	}
	minBlockNum = max(minBlockNum, unwindToLimit)

	// Binary search, but not too deep
	jump := cmp.InRange(1, maxUnwindJumpAllowance, (blockNumber-minBlockNum)/2)
	unwindTo := blockNumber - jump

	// protect from too far unwind
	allowedUnwindTo, ok, err := rawtemporaldb.CanUnwindBeforeBlockNum(unwindTo, applyTx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: requested=%d, minAllowed=%d", ErrTooDeepUnwind, unwindTo, allowedUnwindTo)
	}
	logger.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
	if u != nil {
		if err := u.UnwindTo(allowedUnwindTo, BadBlock(blockHash, ErrInvalidStateRootHash), applyTx); err != nil {
			return err
		}
	}
	return nil
}

type FlushAndComputeCommitmentTimes struct {
	Flush             time.Duration
	ComputeCommitment time.Duration
}

// computeAndCheckCommitmentV3 - does write state to db and then check commitment
func computeAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.TemporalRwTx, doms *execctx.SharedDomains, cfg ExecuteBlockCfg, e *StageState, parallel bool, logger log.Logger, u Unwinder, isBlockProduction bool) (ok bool, times FlushAndComputeCommitmentTimes, err error) {
	if header == nil {
		return false, times, errors.New("header is nil")
	}

	start := time.Now()
	// E2 state root check was in another stage - means we did flush state even if state root will not match
	// And Unwind expecting it
	if !parallel {
		if err := e.Update(applyTx, header.Number.Uint64()); err != nil {
			return false, times, err
		}
		if _, err := rawdb.IncrementStateVersion(applyTx); err != nil {
			return false, times, fmt.Errorf("writing plain state version: %w", err)
		}
	}

	if dbg.DiscardCommitment() {
		return true, times, nil
	}

	// Get current txNum from the block being committed
	txNumsReader := cfg.blockReader.TxnumReader()
	blockTxNum, err := txNumsReader.Max(ctx, applyTx, header.Number.Uint64())
	if err != nil {
		return false, times, err
	}
	computedRootHash, err := doms.ComputeCommitment(ctx, applyTx, true, header.Number.Uint64(), blockTxNum, e.LogPrefix(), nil)

	times.ComputeCommitment = time.Since(start)
	if err != nil {
		return false, times, fmt.Errorf("compute commitment: %w", err)
	}

	if isBlockProduction {
		header.Root = common.BytesToHash(computedRootHash)
		return true, times, nil
	}
	if !bytes.Equal(computedRootHash, header.Root.Bytes()) {
		logger.Warn(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", e.LogPrefix(), header.Number.Uint64(), computedRootHash, header.Root.Bytes(), header.Hash()))
		err = handleIncorrectRootHashError(header.Number.Uint64(), header.Hash(), header.ParentHash, applyTx, cfg, e, logger, u)
		return false, times, err
	}
	return true, times, nil

}

func shouldGenerateChangeSets(cfg ExecuteBlockCfg, blockNum, maxBlockNum uint64, initialCycle bool) bool {
	if cfg.syncCfg.AlwaysGenerateChangesets {
		return true
	}
	if blockNum < cfg.blockReader.FrozenBlocks() {
		return false
	}
	if initialCycle {
		return false
	}
	// once past the initial cycle, make sure to generate changesets for the last blocks that fall in the reorg window
	return blockNum+cfg.syncCfg.MaxReorgDepth >= maxBlockNum
}
