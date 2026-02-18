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
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/metrics"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
)

type forkchoiceOutcome struct {
	receipt *executionproto.ForkChoiceReceipt
	err     error
}

func sendForkchoiceReceiptWithoutWaiting(ch chan forkchoiceOutcome, receipt *executionproto.ForkChoiceReceipt, alreadySent bool) error {
	if alreadySent {
		return nil
	}
	select {
	case ch <- forkchoiceOutcome{receipt: receipt}:
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
func (e *EthereumExecutionModule) verifyForkchoiceHashes(ctx context.Context, tx kv.Tx, blockHash, finalizedHash, safeHash common.Hash) (bool, error) {
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

func (e *EthereumExecutionModule) UpdateForkChoice(ctx context.Context, req *executionproto.ForkChoice) (*executionproto.ForkChoiceReceipt, error) {
	blockHash := gointerfaces.ConvertH256ToHash(req.HeadBlockHash)
	safeHash := gointerfaces.ConvertH256ToHash(req.SafeBlockHash)
	finalizedHash := gointerfaces.ConvertH256ToHash(req.FinalizedBlockHash)

	outcomeCh := make(chan forkchoiceOutcome, 1)

	// So we wait at most the amount specified by req.Timeout before just sending out
	go func() {
		if err := e.updateForkChoice(e.bacgroundCtx, blockHash, safeHash, finalizedHash, outcomeCh); err != nil {
			e.logger.Debug("updateforkchoice failed", "err", err)
		}
	}()

	if req.Timeout > 0 {
		fcuTimer := time.NewTimer(time.Duration(req.Timeout) * time.Millisecond)

		select {
		case <-fcuTimer.C:
			e.logger.Debug("treating forkChoiceUpdated as asynchronous as it is taking too long")
			return &executionproto.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
				Status:          executionproto.ExecutionStatus_Busy,
			}, nil
		case outcome := <-outcomeCh:
			return outcome.receipt, outcome.err
		case <-ctx.Done():
			e.logger.Debug("forkChoiceUpdate cancelled")
			return nil, ctx.Err()
		}
	}

	select {
	case outcome := <-outcomeCh:
		return outcome.receipt, outcome.err
	case <-ctx.Done():
		e.logger.Debug("forkChoiceUpdate cancelled")
		return nil, ctx.Err()
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

func (e *EthereumExecutionModule) updateForkChoice(ctx context.Context, originalBlockHash, safeHash, finalizedHash common.Hash, outcomeCh chan forkchoiceOutcome) (err error) {
	// Wait up to 2 seconds for the semaphore instead of failing instantly.
	// With parallel state flushing, the previous FcU's background commit may still
	// hold the semaphore. Instant failure (TryAcquire) causes the CL to receive
	// SYNCING status, which can stall finality on fast slot times (e.g. 4s devnets).
	semaCtx, semaCancel := context.WithTimeout(ctx, 2*time.Second)
	defer semaCancel()
	if err := e.semaphore.Acquire(semaCtx, 1); err != nil {
		e.logger.Trace("ethereumExecutionModule.updateForkChoice: ExecutionStatus_Busy", "wait", "2s")
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Status:          executionproto.ExecutionStatus_Busy,
		}, false)
		return fmt.Errorf("semaphore timeout after 2s")
	}
	shouldReleaseSema := true
	defer func() {
		if shouldReleaseSema {
			e.semaphore.Release(1)
		}
	}()

	defer UpdateForkChoiceDuration(time.Now())
	defer e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)
	defer e.currentContext.ResetPendingUpdates()

	var validationError string
	type canonicalEntry struct {
		hash   common.Hash
		number uint64
	}
	tx, err := e.db.BeginTemporalRw(ctx) //nolint
	if err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	}

	rollbackOnReturn := true

	defer func() {
		if rollbackOnReturn {
			tx.Rollback()
		}
	}()

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

	var isDomainAheadOfBlocks bool
	currentContext, err := execctx.NewSharedDomains(ctx, tx, e.logger)
	if err != nil {
		// we handle isDomainAheadOfBlocks=true after AppendCanonicalTxNums to allow the node to catch up
		isDomainAheadOfBlocks = errors.Is(err, commitmentdb.ErrBehindCommitment)
		if !isDomainAheadOfBlocks {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
	}

	defer func() {
		if rollbackOnReturn && currentContext != nil {
			currentContext.Close()
		}
	}()

	if fcuHeader.Number.Sign() > 0 {
		if canonicalHash == blockHash {
			// if block hash is part of the canonical chain treat it as no-op.
			writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)
			valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
			if err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			}
			if !valid {
				return sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
					LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
					Status:          executionproto.ExecutionStatus_InvalidForkchoice,
				}, false)
			}
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(blockHash),
				Status:          executionproto.ExecutionStatus_Success,
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
				return sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
					LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
					Status:          executionproto.ExecutionStatus_MissingSegment,
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

		// if unwindTarget <
		if err := e.executionPipeline.UnwindTo(unwindTarget, stagedsync.ForkChoice, tx); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		if err = e.hook.BeforeRun(tx, isSynced); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		// Run the unwind
		if err := e.executionPipeline.RunUnwind(currentContext, tx); err != nil {
			err = fmt.Errorf("updateForkChoice: %w", err)
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}

		UpdateForkChoiceDepth(fcuHeader.Number.Uint64() - 1 - unwindTarget)

		if err := rawdbv3.TxNums.Truncate(tx, currentParentNumber+1); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		// Mark all new canonicals as canonicals
		for _, canonicalSegment := range newCanonicals {
			chainReader := consensuschain.NewReader(e.config, tx, e.blockReader, e.logger)

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
		if err := currentContext.Flush(ctx, tx); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		currentContext.ClearRam(true)
		if err := tx.Commit(); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		}
		return sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Status:          executionproto.ExecutionStatus_TooFarAway,
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
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(blockHash),
				Status:          executionproto.ExecutionStatus_Success,
				ValidationError: validationError,
			}, false)
			e.logHeadUpdated(blockHash, fcuHeader, 0, "head validated", false)
		}
		if err := e.forkValidator.MergeExtendingFork(ctx, tx, currentContext, e.accumulator, e.recentReceipts); err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}
		rawdb.WriteHeadBlockHash(tx, blockHash)
	}
	// Run the forkchoice
	initialCycle := limitedBigJump
	firstCycle := false

	sendError := func(msg string, err error) error {
		err = fmt.Errorf("%s: %w", msg, err)
		e.logger.Warn("Cannot update chain head", "hash", blockHash, "err", err)
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
	}

	for hasMore := true; hasMore; {
		currentContext.SetStateCache(e.stateCache)
		hasMore, err = e.executionPipeline.Run(currentContext, tx, initialCycle, firstCycle)
		if err != nil {
			err = fmt.Errorf("updateForkChoice: %w", err)
			e.logger.Warn("Cannot update chain head", "hash", blockHash, "err", err)
			if errors.Is(err, rules.ErrInvalidBlock) {
				return sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
					Status:          executionproto.ExecutionStatus_BadBlock,
					ValidationError: err.Error(),
					LatestValidHash: gointerfaces.ConvertHashToH256(rawdb.ReadHeadBlockHash(tx)),
				}, stateFlushingInParallel)
			}
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}
		if hasMore {
			err = e.executionPipeline.RunPrune(ctx, tx, initialCycle, 500*time.Millisecond)
			if err != nil {
				return sendError("updateForkChoice: RunPrune after hasMore", err)
			}
			if err := currentContext.Flush(ctx, tx); err != nil {
				return sendError("updateForkChoice:flush sd after hasMore", err)
			}
			currentContext.ClearRam(true)
			if err = tx.Commit(); err != nil {
				return sendError("updateForkChoice: tx commit after hasMore", err)
			}
			tx, err = e.db.BeginTemporalRw(ctx)
			if err != nil {
				return sendError("updateForkChoice: begin tx after has more", err)
			}
		}
	}

	// if head hash was set then success otherwise no
	headHash := rawdb.ReadHeadBlockHash(tx)
	headNumber, err := e.blockReader.HeaderNumber(ctx, tx, headHash)
	if err != nil {
		return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
	}

	var status executionproto.ExecutionStatus

	if headHash != blockHash {
		status = executionproto.ExecutionStatus_BadBlock
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
		status = executionproto.ExecutionStatus_Success
		// Update forks...
		writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)

		valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
		if err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}
		if !valid {
			return sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
				Status:          executionproto.ExecutionStatus_InvalidForkchoice,
				LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
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
		if e.fcuBackgroundCommit {
			// TODO: (20/12/25) we really want to commit all changes with the shared domains but
			// to do that we need to remove all of the rawdb methods and call them via
			// the domains - which will happen after they transiaiotn to an ExecutionContext
			// for the moment just commit what we have
			if err = tx.Commit(); err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			}
			e.lock.Lock()
			e.currentContext = currentContext
			e.lock.Unlock()
			rollbackOnReturn = false
		} else {
			timings, err := e.runForkchoiceCommit(currentContext, tx, finishProgressBefore, isSynced)
			if err != nil {
				return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			}
			if len(timings) > 0 {
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				timings = append(timings, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
				e.logger.Info("Timings: Forkchoice Commit", timings...)
			}
		}
	}

	if !e.fcuBackgroundPrune {
		timings, err := e.runForkchoicePrune(initialCycle)
		if err != nil {
			return sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		}
		if len(timings) > 0 {
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			timings = append(timings, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
			e.logger.Info("Timings: Forkchoice Prune", timings...)
		}
	}
	backgroundPostForkChoice := e.fcuBackgroundCommit || e.fcuBackgroundPrune
	if backgroundPostForkChoice {
		shouldReleaseSema = false // pass on the semaphore to background goroutine
		go func() {
			defer e.semaphore.Release(1)
			err := e.runPostForkchoice(currentContext, finishProgressBefore, isSynced, initialCycle)
			if err != nil && !errors.Is(err, context.Canceled) {
				e.logger.Error("Error running background post forkchoice", "err", err)
			}
		}()
	}

	return sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
		LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		Status:          status,
		ValidationError: validationError,
	}, stateFlushingInParallel)
}

func (e *EthereumExecutionModule) runPostForkchoice(sd *execctx.SharedDomains, finishProgressBefore uint64, isSynced bool, initialCycle bool) error {
	var timings []any
	if e.fcuBackgroundCommit && sd != nil {
		err := e.db.UpdateTemporal(e.bacgroundCtx, func(tx kv.TemporalRwTx) error {
			commitTimings, err := e.runForkchoiceCommit(sd, tx, finishProgressBefore, isSynced)
			if err != nil {
				return err
			}
			timings = append(timings, commitTimings...)
			return nil
		})
		if err != nil {
			return err
		}
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

func (e *EthereumExecutionModule) runForkchoiceCommit(sd *execctx.SharedDomains, tx kv.TemporalRwTx, finishProgressBefore uint64, isSynced bool) ([]any, error) {
	var timings []any
	flushStart := time.Now()
	if err := sd.Flush(e.bacgroundCtx, tx); err != nil {
		return nil, err
	}
	timings = append(timings, "flush", common.Round(time.Since(flushStart), 0))
	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	timings = append(timings, "commit", common.Round(time.Since(commitStart), 0))
	if e.hook != nil {
		if err := e.db.View(e.bacgroundCtx, func(tx kv.Tx) error {
			return e.hook.AfterRun(tx, finishProgressBefore, isSynced)
		}); err != nil {
			return nil, err
		}
	}
	// force fsync after notifications are sent
	if err := e.db.Update(e.bacgroundCtx, func(tx kv.RwTx) error {
		return kv.IncrementKey(tx, kv.DatabaseInfo, []byte("chaindata_force"))
	}); err != nil {
		return nil, err
	}
	return timings, nil
}

func (e *EthereumExecutionModule) runForkchoicePrune(initialCycle bool) ([]any, error) {
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
		if err := e.executionPipeline.RunPrune(e.bacgroundCtx, tx, initialCycle, pruneTimeout); err != nil {
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

func (e *EthereumExecutionModule) logHeadUpdated(blockHash common.Hash, fcuHeader *types.Header, txnum uint64, msg string, debug bool) {
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
		"execution", common.Round(blockTimings[engine_helpers.BlockTimingsValidationIndex], 0))

	if !debug {
		totalTime := blockTimings[engine_helpers.BlockTimingsValidationIndex]
		if !e.syncCfg.ParallelStateFlushing {
			totalTime += blockTimings[engine_helpers.BlockTimingsFlushExtendingFork]
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
