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

package execmodule

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

type forkchoiceOutcome struct {
	result ForkChoiceResult
	err    error
}

func sendForkchoiceResultWithoutWaiting(ch chan forkchoiceOutcome, result ForkChoiceResult, alreadySent bool) error {
	if alreadySent {
		return nil
	}
	select {
	case ch <- forkchoiceOutcome{result: result}:
	default:
	}
	return nil
}

func sendForkchoiceErrorWithoutWaiting(logger log.Logger, ch chan forkchoiceOutcome, err error, alreadySent bool) error {
	if alreadySent {
		logger.Warn("forkchoice: error received after result was sent", "error", err)
		return fmt.Errorf("duplicate fcu error: %w", err)
	}
	select {
	case ch <- forkchoiceOutcome{err: err}:
	default:
	}
	return err
}

// verifyForkchoiceHashes verifies the finalized and safe hash of the forkchoice state
func (e *ExecModule) verifyForkchoiceHashes(ctx context.Context, tx kv.Tx, blockHash, finalizedHash, safeHash common.Hash) (bool, error) {
	// Client software MUST return -38002: Invalid forkchoice state error if the payload referenced by
	// forkchoiceState.headBlockHash is VALID and a payload referenced by either forkchoiceState.finalizedBlockHash or
	// forkchoiceState.safeBlockHash does not belong to the chain defined by forkchoiceState.headBlockHash
	headNumber, err := e.blockReader.HeaderNumber(ctx, tx, blockHash)
	if err != nil {
		return false, err
	}
	finalizedNumber, err := e.blockReader.HeaderNumber(ctx, tx, finalizedHash)
	if err != nil {
		return false, err
	}
	safeNumber, err := e.blockReader.HeaderNumber(ctx, tx, safeHash)
	if err != nil {
		return false, err
	}

	if finalizedHash != (common.Hash{}) && finalizedHash != blockHash {
		canonical, err := e.isCanonicalHash(ctx, tx, finalizedHash)
		if err != nil {
			return false, err
		}
		if !canonical || *headNumber <= *finalizedNumber {
			return false, nil
		}

	}
	if safeHash != (common.Hash{}) && safeHash != blockHash {
		canonical, err := e.isCanonicalHash(ctx, tx, safeHash)
		if err != nil {
			return false, err
		}

		if !canonical || *headNumber <= *safeNumber {
			return false, nil
		}
	}
	return true, nil
}

func (e *ExecModule) UpdateForkChoice(ctx context.Context, headHash, safeHash, finalizedHash common.Hash) (ForkChoiceResult, error) {
	outcomeCh := make(chan forkchoiceOutcome, 1)
	// done is closed when the goroutine fully returns — after all defers
	// (shared-state cleanup, semaphore release) have run. When we receive
	// a non-Busy result, we wait on done so the caller can safely acquire
	// the semaphore for a follow-up operation like AssembleBlock.
	done := make(chan struct{})

	// Spawn the actual forkchoice work using the module's background context so
	// it is not cancelled when the caller's context times out.
	go func() {
		defer close(done)
		if err := e.updateForkChoice(e.bacgroundCtx, headHash, safeHash, finalizedHash, outcomeCh); err != nil {
			e.logger.Debug("updateforkchoice failed", "err", err)
		}
	}()

	select {
	case outcome := <-outcomeCh:
		<-done
		return outcome.result, outcome.err
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			e.logger.Debug("treating forkChoiceUpdated as asynchronous as it is taking too long")
			return ForkChoiceResult{Status: ExecutionStatusBusy}, nil
		}
		e.logger.Debug("forkChoiceUpdate cancelled")
		return ForkChoiceResult{}, ctx.Err()
	}
}

func writeForkChoiceHashes(tx kv.RwTx, blockHash, safeHash, finalizedHash common.Hash) {
	if finalizedHash != (common.Hash{}) {
		rawdb.WriteForkchoiceFinalized(tx, finalizedHash)
	}
	if safeHash != (common.Hash{}) {
		rawdb.WriteForkchoiceSafe(tx, safeHash)
	}
	rawdb.WriteHeadBlockHash(tx, blockHash)
	rawdb.WriteForkchoiceHead(tx, blockHash)
}

func (e *ExecModule) updateForkChoice(ctx context.Context, originalBlockHash, safeHash, finalizedHash common.Hash, outcomeCh chan forkchoiceOutcome) (err error) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.updateForkChoice: ExecutionStatus_Busy")
		sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
			LatestValidHash: common.Hash{},
			Status:          ExecutionStatusBusy,
		}, false)
		return fmt.Errorf("semaphore timeout")
	}
	shouldReleaseSema := true
	defer func() {
		if shouldReleaseSema {
			e.semaphore.Release(1)
		}
	}()

	defer UpdateForkChoiceDuration(time.Now())
	defer e.forkValidator.ClearWithUnwind()
	defer func() {
		if e.currentContext != nil {
			e.currentContext.ResetPendingUpdates()
		}
	}()

	var validationError string
	type canonicalEntry struct {
		hash   common.Hash
		number uint64
	}

	// Open a RO tx as the base for all reads. Writes accumulate in the block
	// overlay (MemoryMutation) which implements kv.TemporalRwTx. No MDBX write
	// lock is held during pipeline execution — a brief RwTx is opened only at
	// commit time to flush everything atomically.
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}
	defer func() { roTx.Rollback() }() // closure: CommitCycle may reassign roTx

	// Check if InsertBlocks already created a block overlay with data
	// (headers, bodies, TDs, canonical hashes).
	var hasOverlay bool
	if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
		hasOverlay = true
	}

	// Create SharedDomains on the raw RO tx. Domain state (accounts,
	// storage, code, commitment) is read from the committed DB state.
	var isDomainAheadOfBlocks bool
	currentContext, err := execctx.NewSharedDomains(ctx, roTx, e.logger)
	if err != nil {
		// we handle isDomainAheadOfBlocks=true after AppendCanonicalTxNums to allow the node to catch up
		isDomainAheadOfBlocks = errors.Is(err, commitmentdb.ErrBehindCommitment)
		if !isDomainAheadOfBlocks {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
	}
	if currentContext != nil {
		currentContext.SetInMemHistoryReads(inMemHistoryReads)
	}

	defer func() {
		if currentContext != nil {
			// Clear the published overlay before closing the SD, so concurrent
			// readers (e.g. a second FCU calling GetHeaderByHash) don't access
			// a closed SharedDomains via Events.LatestSD().
			if dispatcher := e.pipelineExecutor.Dispatcher(); dispatcher != nil {
				dispatcher.PublishOverlay(nil)
			}
			currentContext.Close()
		}
	}()

	// Initialize the SD's block overlay for table-level writes (canonical
	// hashes, stage progress, forkchoice markers, TxNums, etc.). All
	// pipeline reads cascade through the overlay to the RO tx; writes
	// stay in memory until commit.
	if err := currentContext.InitBlockOverlay(roTx, roTx.Debug().Dirs().Tmp); err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, fmt.Errorf("updateForkChoice: init block overlay: %w", err), false)
	}
	var tx kv.TemporalRwTx = currentContext.BlockOverlay()

	// If InsertBlocks wrote block data, flush it into the block overlay
	// so the pipeline can see it. The InsertBlocks overlay retains its data.
	if hasOverlay {
		e.currentContext.BlockOverlay().UpdateTxn(roTx)
		if err := e.currentContext.BlockOverlay().Flush(ctx, tx); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, fmt.Errorf("updateForkChoice: flush block overlay: %w", err), false)
		}
	}

	blockHash := originalBlockHash

	// Step one, find reconnection point, and mark all of those headers as canonical.
	fcuHeader, err := e.blockReader.HeaderByHash(ctx, tx, originalBlockHash)
	if err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}
	if fcuHeader == nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, fmt.Errorf("forkchoice: block %x not found or was marked invalid", blockHash), false)
	}

	e.hook.LastNewBlockSeen(fcuHeader.Number.Uint64()) // used by eth_syncing

	finishProgressBefore, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}
	headersProgressBefore, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}

	e.logger.Debug("[execmodule] updating fork choice", "number", fcuHeader.Number.Uint64(), "hash", fcuHeader.Hash())
	UpdateForkChoiceArrivalDelay(fcuHeader.Time)
	metrics.UpdateBlockConsumerPreExecutionDelay(fcuHeader.Time, fcuHeader.Number.Uint64(), e.logger)
	defer metrics.UpdateBlockConsumerPostExecutionDelay(fcuHeader.Time, fcuHeader.Number.Uint64(), e.logger)

	var limitedBigJump bool
	limitedBigJumpPadding := uint64(2)
	if e.syncCfg.LoopBlockLimit > 0 && fcuHeader.Number.Uint64() > finishProgressBefore {
		// note fcuHeader.Number.Uint64() may be < finishProgressBefore - protect from underflow by checking it is >
		limitedBigJump = (fcuHeader.Number.Uint64()-finishProgressBefore)+limitedBigJumpPadding > uint64(e.syncCfg.LoopBlockLimit)
	}

	isSynced := finishProgressBefore > 0 && finishProgressBefore > e.blockReader.FrozenBlocks() && finishProgressBefore == headersProgressBefore
	if limitedBigJump {
		isSynced = false
		e.logger.Info("[sync] limited big jump", "from", finishProgressBefore, "to", fcuHeader.Number.Uint64(), "amount", uint64(e.syncCfg.LoopBlockLimit), "padding", limitedBigJumpPadding)
	}

	canonicalHash, err := e.canonicalHash(ctx, tx, fcuHeader.Number.Uint64())
	if err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}

	if fcuHeader.Number.Sign() > 0 {
		if canonicalHash == blockHash && fcuHeader.Number.Uint64() >= finishProgressBefore {
			// if block hash is part of the canonical chain and execution is not ahead, treat as no-op.
			writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)
			valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
			if err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}
			if !valid {
				return sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
					LatestValidHash: common.Hash{},
					Status:          ExecutionStatusInvalidForkchoice,
				}, false)
			}
			sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
				LatestValidHash: blockHash,
				Status:          ExecutionStatusSuccess,
			}, false)
			return err
		}

		currentParentHash := fcuHeader.ParentHash
		currentParentNumber := fcuHeader.Number.Uint64() - 1
		isCanonicalHash, err := e.isCanonicalHash(ctx, tx, currentParentHash)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return err
		}
		// Find such point, and collect all hashes
		newCanonicals := make([]*canonicalEntry, 0, 64)
		newCanonicals = append(newCanonicals, &canonicalEntry{
			hash:   fcuHeader.Hash(),
			number: fcuHeader.Number.Uint64(),
		})
		for !isCanonicalHash {
			newCanonicals = append(newCanonicals, &canonicalEntry{
				hash:   currentParentHash,
				number: currentParentNumber,
			})
			currentHeader, err := e.blockReader.Header(ctx, tx, currentParentHash, currentParentNumber)
			if err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}
			if currentHeader == nil {
				return sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
					LatestValidHash: common.Hash{},
					Status:          ExecutionStatusMissingSegment,
				}, false)
			}
			currentParentHash = currentHeader.ParentHash
			if currentHeader.Number.Sign() == 0 {
				panic("assert:uint64 underflow") //uint-underflow
			}
			currentParentNumber = currentHeader.Number.Uint64() - 1
			isCanonicalHash, err = e.isCanonicalHash(ctx, tx, currentParentHash)
			if err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}
		}

		unwindTarget := currentParentNumber
		minUnwindableBlock, err := rawtemporaldb.CanUnwindToBlockNum(tx)
		if err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		if unwindTarget < minUnwindableBlock {
			e.logger.Info("Reorg requested too low, capping to the minimum unwindable block", "unwindTarget", unwindTarget, "minUnwindableBlock", minUnwindableBlock)
			unwindTarget = minUnwindableBlock
		}

		if err := e.pipelineExecutor.UnwindTo(unwindTarget, stagedsync.ForkChoice, tx); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		if err = e.hook.BeforeRun(tx, isSynced); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		// Run the unwind
		if err := e.pipelineExecutor.RunUnwind(currentContext, tx); err != nil {
			err = fmt.Errorf("updateForkChoice: %w", err)
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}

		UpdateForkChoiceDepth(fcuHeader.Number.Uint64() - 1 - unwindTarget)

		if err := rawdbv3.TxNums.Truncate(tx, currentParentNumber+1); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		// Mark all new canonicals as canonicals
		chainReader := consensuschain.NewReader(e.config, tx, e.blockReader, e.logger)
		for _, canonicalSegment := range newCanonicals {
			b, _, _ := rawdb.ReadBody(tx, canonicalSegment.hash, canonicalSegment.number)
			h := rawdb.ReadHeader(tx, canonicalSegment.hash, canonicalSegment.number)

			if b == nil || h == nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, fmt.Errorf("unexpected chain cap: %d", canonicalSegment.number), false)
			}

			if err := e.engine.VerifyHeader(chainReader, h, true); err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}

			if err := e.engine.VerifyUncles(chainReader, h, b.Uncles); err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}

			if err := rawdb.WriteCanonicalHash(tx, canonicalSegment.hash, canonicalSegment.number); err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}
		}
		if len(newCanonicals) > 0 {
			if err := rawdbv3.TxNums.Truncate(tx, newCanonicals[0].number); err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}
			// make sure we truncate any previous canonical hashes that go beyond the current head height
			// so that AppendCanonicalTxNums does not mess up the txNums index
			if err := rawdb.TruncateCanonicalHash(tx, newCanonicals[0].number+1, false); err != nil {
				return err
			}
			if err := rawdb.AppendCanonicalTxNums(tx, newCanonicals[len(newCanonicals)-1].number); err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}
		}
	}
	if isDomainAheadOfBlocks {
		// Open a brief RwTx to flush accumulated overlay + SD state atomically.
		commitRwTx, err := e.db.BeginTemporalRw(ctx)
		if err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, fmt.Errorf("isDomainAhead: begin rw: %w", err), false)
		}
		defer commitRwTx.Rollback()
		if err := currentContext.Flush(ctx, commitRwTx); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		currentContext.ClearRam(true)
		if err := commitRwTx.Commit(); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		return sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
			LatestValidHash: common.Hash{},
			Status:          ExecutionStatusTooFarAway,
			ValidationError: "domain ahead of blocks",
		}, false)
	}

	// Set Progress for headers and bodies accordingly.
	if err := stages.SaveStageProgress(tx, stages.Headers, fcuHeader.Number.Uint64()); err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}
	if err := stages.SaveStageProgress(tx, stages.BlockHashes, fcuHeader.Number.Uint64()); err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, fcuHeader.Number.Uint64()); err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}
	if err = rawdb.WriteHeadHeaderHash(tx, blockHash); err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}

	mergeExtendingFork := blockHash == e.forkValidator.ExtendingForkHeadHash()
	stateFlushingInParallel := mergeExtendingFork && e.syncCfg.ParallelStateFlushing
	if mergeExtendingFork {
		e.logger.Debug("[updateForkchoice] Fork choice update: flushing in-memory state (built by previous newPayload)")
		if stateFlushingInParallel {
			// Send forkchoice early (We already know the fork is valid)
			sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
				LatestValidHash: blockHash,
				Status:          ExecutionStatusSuccess,
				ValidationError: validationError,
			}, false)
			e.logHeadUpdated(blockHash, fcuHeader, 0, "head validated", false)
		}
		if err := e.forkValidator.MergeExtendingFork(ctx, tx, currentContext, e.accum); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}
		rawdb.WriteHeadBlockHash(tx, blockHash)
	}
	// Run the forkchoice
	initialCycle := limitedBigJump
	firstCycle := false

	tx, err = e.pipelineExecutor.RunLoop(ctx, currentContext, tx, RunLoopConfig{
		InitialCycle:    initialCycle,
		FirstCycle:      firstCycle,
		PruneTimeout:    500 * time.Millisecond,
		BeforeIteration: nil,
		CommitCycle: func(ctx context.Context, sd *execctx.SharedDomains) (kv.TemporalRwTx, error) {
			// Flush SD + overlay to a brief RwTx to relieve memory pressure.
			commitRwTx, err := e.db.BeginTemporalRw(ctx) //nolint:gocritic
			if err != nil {
				return nil, fmt.Errorf("updateForkChoice: begin rw after hasMore: %w", err)
			}
			if err := sd.Flush(ctx, commitRwTx); err != nil {
				commitRwTx.Rollback()
				return nil, fmt.Errorf("updateForkChoice: flush sd after hasMore: %w", err)
			}
			sd.ClearRam(true)
			if err = commitRwTx.Commit(); err != nil {
				return nil, fmt.Errorf("updateForkChoice: tx commit after hasMore: %w", err)
			}
			// Recreate RO tx + block overlay on the fresh committed state.
			roTx.Rollback()
			roTx, err = e.db.BeginTemporalRo(ctx) //nolint:gocritic
			if err != nil {
				return nil, fmt.Errorf("updateForkChoice: begin ro after hasMore: %w", err)
			}
			if err := sd.InitBlockOverlay(roTx, roTx.Debug().Dirs().Tmp); err != nil {
				roTx.Rollback()
				return nil, fmt.Errorf("updateForkChoice: init overlay after hasMore: %w", err)
			}
			newTx := sd.BlockOverlay()
			// Re-flush InsertBlocks data into the fresh overlay.
			if hasOverlay {
				e.currentContext.BlockOverlay().UpdateTxn(roTx)
				if err := e.currentContext.BlockOverlay().Flush(ctx, newTx); err != nil {
					return nil, fmt.Errorf("updateForkChoice: re-flush overlay after hasMore: %w", err)
				}
			}
			return newTx, nil
		},
	})
	if err != nil {
		err = fmt.Errorf("updateForkChoice: %w", err)
		e.logger.Warn("Cannot update chain head", "hash", blockHash, "err", err)
		if errors.Is(err, rules.ErrInvalidBlock) {
			return sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
				Status:          ExecutionStatusBadBlock,
				ValidationError: err.Error(),
				LatestValidHash: rawdb.ReadHeadBlockHash(tx),
			}, stateFlushingInParallel)
		}
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
	}

	// if head hash was set then success otherwise no
	headHash := rawdb.ReadHeadBlockHash(tx)
	headNumber, err := e.blockReader.HeaderNumber(ctx, tx, headHash)
	if err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
	}
	if headNumber != nil {
		e.forkValidator.NotifyCurrentHeight(*headNumber)
	}

	var status ExecutionStatus

	if headHash != blockHash {
		status = ExecutionStatusBadBlock
		blockHashBlockNum, _ := e.blockReader.HeaderNumber(ctx, tx, blockHash)

		validationError = "headHash and blockHash mismatch"
		if headNumber != nil && e.logger != nil {
			headNum := strconv.FormatUint(*headNumber, 10)
			hashBlockNum := "unknown"
			if blockHashBlockNum != nil {
				hashBlockNum = strconv.FormatUint(*blockHashBlockNum, 10)
			}
			e.logger.Warn("bad forkchoice", "head", headHash, "head block", headNum, "hash", blockHash, "hash block", hashBlockNum)
		}
		currentContext.Close()
		currentContext = nil
		if e.fcuBackgroundCommit {
			e.lock.Lock()
			e.currentContext = nil
			e.lock.Unlock()
		}
	} else {
		status = ExecutionStatusSuccess
		// Update forks...
		writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)

		valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
		if err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}
		if !valid {
			return sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
				Status:          ExecutionStatusInvalidForkchoice,
				LatestValidHash: common.Hash{},
			}, stateFlushingInParallel)
		}
		if err := rawdb.TruncateCanonicalChain(ctx, tx, *headNumber+1); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}

		if err := rawdbv3.TxNums.Truncate(tx, *headNumber+1); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}

		txnum, err := rawdbv3.TxNums.Max(ctx, tx, fcuHeader.Number.Uint64())
		if err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}

		e.logHeadUpdated(blockHash, fcuHeader, txnum, "head updated", stateFlushingInParallel)

		// Close the persistent SD (overlay was already flushed into rwTx at
		// the start of updateForkChoice). Clear e.currentContext so InsertBlocks
		// creates a fresh overlay for the next block cycle.
		if hasOverlay {
			e.currentContext.Close()
			e.lock.Lock()
			e.currentContext = nil
			e.lock.Unlock()
		}

		// Dispatch notifications from the SD overlay (before flush/commit).
		// After this, all consumers have the data — the semaphore can be
		// released and flush/commit/prune can proceed without blocking the
		// next FCU.
		e.logger.Debug("[updateForkChoice] dispatching notifications", "head", blockHash, "bgCommit", e.fcuBackgroundCommit)
		if err := e.dispatchNotificationsFromOverlay(currentContext, finishProgressBefore); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, fmt.Errorf("fcu: dispatch notifications: %w", err), stateFlushingInParallel)
		}

		// Flush + commit: foreground by default, background only if
		// fcuBackgroundCommit is explicitly enabled.
		var commitTimings []any
		if e.fcuBackgroundCommit {
			shouldReleaseSema = false
			// Transfer roTx ownership to the goroutine so the outer defer
			// (roTx.Rollback) becomes a no-op on nil.
			bgRoTx := roTx
			roTx = nil
			bgSD := currentContext
			currentContext = nil
			dispatcher := e.pipelineExecutor.Dispatcher()
			go func() {
				defer e.semaphore.Release(1)
				defer bgSD.Close()
				defer bgRoTx.Rollback()
				err := e.runPostForkchoice(bgSD, finishProgressBefore, isSynced, initialCycle)
				if err != nil && !errors.Is(err, context.Canceled) {
					e.logger.Error("Error running background post forkchoice", "err", err)
				}
				// Signal that the DB commit is done — RPC consumers can
				// drop their SD reference and read from committed DB.
				if dispatcher != nil {
					dispatcher.PublishOverlay(nil)
				}
			}()
		} else {
			ct, err := e.runForkchoiceFlushCommit(currentContext, finishProgressBefore, isSynced)
			if err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			}
			commitTimings = ct
		}

		// Prune: background by default (fcuBackgroundPrune=true).
		// Only runs in foreground when both background flags are off.
		if !e.fcuBackgroundCommit {
			if e.fcuBackgroundPrune {
				go func() {
					pruneTimings, err := e.runForkchoicePrune(initialCycle)
					if err != nil && !errors.Is(err, context.Canceled) {
						e.logger.Error("Error running background prune", "err", err)
					}
					if len(pruneTimings) > 0 {
						var m runtime.MemStats
						dbg.ReadMemStats(&m)
						pruneTimings = append(pruneTimings, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
						e.logger.Info("Timings: Background Prune", pruneTimings...)
					}
				}()
			} else {
				pruneTimings, err := e.runForkchoicePrune(initialCycle)
				if err != nil {
					return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
				}
				commitTimings = append(commitTimings, pruneTimings...)
			}
		}

		if len(commitTimings) > 0 {
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			commitTimings = append(commitTimings, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
			e.logger.Info("Timings: Forkchoice", commitTimings...)
		}
	}

	return sendForkchoiceResultWithoutWaiting(outcomeCh, ForkChoiceResult{
		LatestValidHash: headHash,
		Status:          status,
		ValidationError: validationError,
	}, stateFlushingInParallel)
}

// runPostForkchoice runs flush+commit+UpdateHead+prune in a background goroutine.
// Notifications have already been dispatched inline from the overlay.
func (e *ExecModule) runPostForkchoice(sd *execctx.SharedDomains, finishProgressBefore uint64, isSynced bool, initialCycle bool) error {
	var timings []any
	if e.fcuBackgroundCommit && sd != nil {
		commitTimings, err := e.runForkchoiceFlushCommit(sd, finishProgressBefore, isSynced)
		if err != nil {
			return err
		}
		timings = append(timings, commitTimings...)
	}
	if e.fcuBackgroundPrune {
		pruneTimings, err := e.runForkchoicePrune(initialCycle)
		if err != nil {
			return err
		}
		timings = append(timings, pruneTimings...)
	}
	if len(timings) > 0 {
		var m runtime.MemStats
		dbg.ReadMemStats(&m)
		timings = append(timings, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
		e.logger.Info("Timings: Post-Forkchoice", timings...)
	}
	return nil
}

// dispatchNotificationsFromOverlay sends notifications reading from the SD's
// blockOverlay (MemoryMutation). All required data — headers, canonical hashes,
// state version, forkchoice markers — exists in the overlay before flush/commit.
// Called inline (under semaphore) so consumers have the data before the next
// FCU can start.
func (e *ExecModule) dispatchNotificationsFromOverlay(sd *execctx.SharedDomains, finishProgressBefore uint64) error {
	dispatcher := e.pipelineExecutor.Dispatcher()
	if dispatcher == nil || e.accum == nil {
		e.logger.Debug("[dispatchNotifications] skipped: dispatcher or accum nil", "dispatcherNil", dispatcher == nil, "accumNil", e.accum == nil)
		return nil
	}
	overlay := sd.BlockOverlay()
	if overlay == nil {
		e.logger.Debug("[dispatchNotifications] skipped: overlay nil")
		return nil
	}
	finishProgressAfter, err := stages.GetStageProgress(overlay, stages.Finish)
	if err != nil {
		return err
	}
	// Publish the overlay BEFORE dispatching notifications. This ensures
	// the BlockListener (overlay-aware shutter) sees the overlay as active
	// before any StateChangeBatch arrives, so it can buffer events properly.
	e.logger.Debug("[dispatchNotifications] publishing SD")
	dispatcher.PublishOverlay(sd)

	e.logger.Debug("[dispatchNotifications] dispatching", "finishBefore", finishProgressBefore, "finishAfter", finishProgressAfter)
	if err := dispatcher.Dispatch(
		e.bacgroundCtx,
		overlay,
		e.accum.Accumulator,
		e.accum.RecentReceipts,
		finishProgressBefore,
		finishProgressAfter,
		e.pipelineExecutor.Sync().PrevUnwindPoint(),
	); err != nil {
		return err
	}

	return nil
}

// runForkchoiceFlushCommit opens a brief RwTx, flushes the SharedDomains
// (block overlay + domain mem), commits, then updates the sentry head.
func (e *ExecModule) runForkchoiceFlushCommit(sd *execctx.SharedDomains, finishProgressBefore uint64, isSynced bool) ([]any, error) {
	var timings []any

	rwTx, err := e.db.BeginTemporalRw(e.bacgroundCtx)
	if err != nil {
		return nil, fmt.Errorf("fcu flush+commit: begin rw: %w", err)
	}
	defer rwTx.Rollback()
	flushStart := time.Now()
	if err := sd.Flush(e.bacgroundCtx, rwTx); err != nil {
		return nil, err
	}
	timings = append(timings, "flush", common.Round(time.Since(flushStart), 0))
	commitStart := time.Now()
	if err := rwTx.Commit(); err != nil {
		return nil, err
	}
	timings = append(timings, "commit", common.Round(time.Since(commitStart), 0))

	// Update head and announce block range (notifications already dispatched).
	if e.hook != nil {
		if err := e.db.View(e.bacgroundCtx, func(tx kv.Tx) error {
			return e.hook.UpdateHead(tx, finishProgressBefore, isSynced)
		}); err != nil {
			return nil, err
		}
	}
	// Force fsync so data is durable before the next slot.
	if err := e.db.Update(e.bacgroundCtx, func(tx kv.RwTx) error {
		return kv.IncrementKey(tx, kv.DatabaseInfo, []byte("chaindata_force"))
	}); err != nil {
		return nil, err
	}
	return timings, nil
}

func (e *ExecModule) runForkchoicePrune(initialCycle bool) ([]any, error) {
	var timings []any
	pruneStart := time.Now()
	defer UpdateForkChoicePruneDuration(pruneStart)
	if err := e.db.UpdateTemporal(e.bacgroundCtx, func(tx kv.TemporalRwTx) error {
		// check that the current header isn't less than a step, this
		// is mainly to prevent noise in testing on short chains with
		// no snapshots and no need for pruning
		currentHeader := rawdb.ReadCurrentHeader(tx)
		if currentHeader == nil {
			return nil
		}
		maxTxNum, err := rawdbv3.TxNums.Max(e.bacgroundCtx, tx, currentHeader.Number.Uint64())
		if err != nil || maxTxNum < (tx.Debug().StepSize()*5)/4 {
			return nil
		}

		pruneTimeout := time.Duration(e.config.SecondsPerSlot()*1000/3) * time.Millisecond / 2
		if err := e.pipelineExecutor.RunPrune(e.bacgroundCtx, tx, initialCycle, pruneTimeout); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	timings = append(timings, "prune", common.Round(time.Since(pruneStart), 0))
	if len(timings) > 0 {
		timings = append(timings, "initialCycle", initialCycle)
	}
	return timings, nil
}

func (e *ExecModule) logHeadUpdated(blockHash common.Hash, fcuHeader *types.Header, txnum uint64, msg string, debug bool) {
	if e.logger == nil {
		return
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	blockTimings := e.forkValidator.GetTimings(blockHash)

	logArgs := []any{"hash", blockHash, "number", fcuHeader.Number.Uint64()}
	if txnum != 0 {
		logArgs = append(logArgs, "txnum", txnum)
	}
	logArgs = append(logArgs, "age", common.PrettyAge(time.Unix(int64(fcuHeader.Time), 0)),
		"execution", common.Round(blockTimings[BlockTimingsValidationIndex], 0))

	if !debug {
		totalTime := blockTimings[BlockTimingsValidationIndex]
		if !e.syncCfg.ParallelStateFlushing {
			totalTime += blockTimings[BlockTimingsFlushExtendingFork]
		}
		gasUsedMgas := float64(fcuHeader.GasUsed) / 1e6
		mgasPerSec := gasUsedMgas / totalTime.Seconds()
		metrics.ChainTipMgasPerSec.Add(mgasPerSec)

		const blockRange = 30 // ~1 hour
		const alpha = 2.0 / (blockRange + 1)

		if e.avgMgasSec == 0 || e.avgMgasSec == math.Inf(1) {
			e.avgMgasSec = mgasPerSec
		}
		e.avgMgasSec = alpha*mgasPerSec + (1-alpha)*e.avgMgasSec
		// if mgasPerSec or avgMgasPerSec are 0, Inf or -Inf, do not log it but dont return either
		if mgasPerSec > 0 && mgasPerSec != math.Inf(1) && e.avgMgasSec > 0 && e.avgMgasSec != math.Inf(1) {
			logArgs = append(logArgs, "mgas/s", fmt.Sprintf("%.2f", mgasPerSec), "avg mgas/s", fmt.Sprintf("%.2f", e.avgMgasSec))
		}
	}

	logArgs = append(logArgs, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

	dbgLevel := log.LvlInfo
	if debug {
		dbgLevel = log.LvlDebug
	}
	e.logger.Log(dbgLevel, msg, logArgs...)
}
