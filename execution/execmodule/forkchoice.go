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
	"github.com/erigontech/erigon/common/metrics"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
)

const startPruneFrom = 1024

type forkchoiceOutcome struct {
	receipt *executionproto.ForkChoiceReceipt
	err     error
}

func sendForkchoiceReceiptWithoutWaiting(ch chan forkchoiceOutcome, receipt *executionproto.ForkChoiceReceipt, alreadySent bool) {
	if alreadySent {
		return
	}
	select {
	case ch <- forkchoiceOutcome{receipt: receipt}:
	default:
	}
}

func sendForkchoiceErrorWithoutWaiting(logger log.Logger, ch chan forkchoiceOutcome, err error, alreadySent bool) {
	if alreadySent {
		logger.Warn("forkchoice: error received after result was sent", "error", err)
		return
	}

	select {
	case ch <- forkchoiceOutcome{err: err}:
	default:
	}
}

func isDomainAheadOfBlocks(ctx context.Context, tx kv.TemporalRwTx, logger log.Logger) bool {
	doms, err := execctx.NewSharedDomains(ctx, tx, logger)
	if err != nil {
		logger.Debug("domain ahead of blocks", "err", err)
		return errors.Is(err, commitmentdb.ErrBehindCommitment)
	}
	defer doms.Close()
	return false
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
	go e.updateForkChoice(e.bacgroundCtx, blockHash, safeHash, finalizedHash, outcomeCh)

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

func (e *EthereumExecutionModule) updateForkChoice(ctx context.Context, originalBlockHash, safeHash, finalizedHash common.Hash, outcomeCh chan forkchoiceOutcome) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.updateForkChoice: ExecutionStatus_Busy")
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Status:          executionproto.ExecutionStatus_Busy,
		}, false)
		return
	}
	defer e.semaphore.Release(1)

	defer UpdateForkChoiceDuration(time.Now())

	//if err := stages2.ProcessFrozenBlocks(ctx, e.db, e.blockReader, e.executionPipeline); err != nil {
	//	sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	//	e.logger.Warn("ProcessFrozenBlocks", "error", err)
	//	return
	//}
	defer e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)

	var validationError string
	type canonicalEntry struct {
		hash   common.Hash
		number uint64
	}
	tx, err := e.db.BeginTemporalRw(ctx) //nolint
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}
	defer func() {
		tx.Rollback()
	}()

	blockHash := originalBlockHash

	// Step one, find reconnection point, and mark all of those headers as canonical.
	fcuHeader, err := e.blockReader.HeaderByHash(ctx, tx, originalBlockHash)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}
	if fcuHeader == nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, fmt.Errorf("forkchoice: block %x not found or was marked invalid", blockHash), false)
		return
	}

	e.hook.LastNewBlockSeen(fcuHeader.Number.Uint64()) // used by eth_syncing

	finishProgressBefore, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}
	headersProgressBefore, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}

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
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}

	sd, err := execctx.NewSharedDomains(ctx, tx, e.logger)
	if err != nil {
		return
	}
	defer sd.Close()

	if fcuHeader.Number.Sign() > 0 {
		if canonicalHash == blockHash {
			// if block hash is part of the canonical chain treat it as no-op.
			writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)
			valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
			if err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}
			if !valid {
				sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
					LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
					Status:          executionproto.ExecutionStatus_InvalidForkchoice,
				}, false)
				return
			}
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(blockHash),
				Status:          executionproto.ExecutionStatus_Success,
			}, false)
			return
		}

		currentParentHash := fcuHeader.ParentHash
		currentParentNumber := fcuHeader.Number.Uint64() - 1
		isCanonicalHash, err := e.isCanonicalHash(ctx, tx, currentParentHash)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
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
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}
			if currentHeader == nil {
				sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
					LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
					Status:          executionproto.ExecutionStatus_MissingSegment,
				}, false)
				return
			}
			currentParentHash = currentHeader.ParentHash
			if currentHeader.Number.Sign() == 0 {
				panic("assert:uint64 underflow") //uint-underflow
			}
			currentParentNumber = currentHeader.Number.Uint64() - 1
			isCanonicalHash, err = e.isCanonicalHash(ctx, tx, currentParentHash)
			if err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}
		}

		unwindTarget := currentParentNumber
		minUnwindableBlock, err := rawtemporaldb.CanUnwindToBlockNum(tx)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}
		if unwindTarget < minUnwindableBlock {
			e.logger.Info("Reorg requested too low, capping to the minimum unwindable block", "unwindTarget", unwindTarget, "minUnwindableBlock", minUnwindableBlock)
			unwindTarget = minUnwindableBlock
		}

		// if unwindTarget <
		if err := e.executionPipeline.UnwindTo(unwindTarget, stagedsync.ForkChoice, tx); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}
		if err = e.hook.BeforeRun(tx, isSynced); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}
		// Run the unwind
		if err := e.executionPipeline.RunUnwind(e.db, sd, tx); err != nil {
			err = fmt.Errorf("updateForkChoice: %w", err)
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}

		if err := sd.Flush(ctx, tx); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}
		sd.ClearRam(true)

		UpdateForkChoiceDepth(fcuHeader.Number.Uint64() - 1 - unwindTarget)

		if err := rawdbv3.TxNums.Truncate(tx, currentParentNumber+1); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}
		// Mark all new canonicals as canonicals
		for _, canonicalSegment := range newCanonicals {
			chainReader := consensuschain.NewReader(e.config, tx, e.blockReader, e.logger)

			b, _, _ := rawdb.ReadBody(tx, canonicalSegment.hash, canonicalSegment.number)
			h := rawdb.ReadHeader(tx, canonicalSegment.hash, canonicalSegment.number)

			if b == nil || h == nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, fmt.Errorf("unexpected chain cap: %d", canonicalSegment.number), false)
				return
			}

			if err := e.engine.VerifyHeader(chainReader, h, true); err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}

			if err := e.engine.VerifyUncles(chainReader, h, b.Uncles); err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}

			if err := rawdb.WriteCanonicalHash(tx, canonicalSegment.hash, canonicalSegment.number); err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}
		}
		if len(newCanonicals) > 0 {
			if err := rawdbv3.TxNums.Truncate(tx, newCanonicals[0].number); err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}
			if err := rawdb.AppendCanonicalTxNums(tx, newCanonicals[len(newCanonicals)-1].number); err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}
		}
	}
	if isDomainAheadOfBlocks(ctx, tx, e.logger) {
		if err := sd.Flush(ctx, tx); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}
		sd.ClearRam(true)
		if err := tx.Commit(); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Status:          executionproto.ExecutionStatus_TooFarAway,
			ValidationError: "domain ahead of blocks",
		}, false)
		return
	}

	// Set Progress for headers and bodies accordingly.
	if err := stages.SaveStageProgress(tx, stages.Headers, fcuHeader.Number.Uint64()); err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}
	if err := stages.SaveStageProgress(tx, stages.BlockHashes, fcuHeader.Number.Uint64()); err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, fcuHeader.Number.Uint64()); err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}
	if err = rawdb.WriteHeadHeaderHash(tx, blockHash); err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
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
			e.logHeadUpdated(blockHash, fcuHeader, 0, time.Duration(0), "head validated", false)
		}
		if err := e.forkValidator.FlushExtendingFork(tx, e.accumulator, e.recentReceipts); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}

		sd.SeekCommitment(ctx, tx)
	}
	// Run the forkchoice
	initialCycle := limitedBigJump
	firstCycle := false

	sendError := func(msg string, err error) {
		err = fmt.Errorf("%s: %w", msg, err)
		e.logger.Warn("Cannot update chain head", "hash", blockHash, "err", err)
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
	}

	commitAndReopenTx := func() bool {
		if err = tx.Commit(); err != nil {
			sendError("updateForkChoice: tx commit after hasMore", err)
			return false
		}
		tx, err = e.db.BeginTemporalRw(ctx) //nolint:gocritic
		// note we already have defer tx.Rollback() on stack from earlier
		if err != nil {
			sendError("updateForkChoice: begin tx after has more", err)
			return false
		}
		return true
	}

	// Make sure that any reorgs are in before execution.
	// parallel warmup would fail otherwise!
	if !commitAndReopenTx() {
		return
	}

	for hasMore := true; hasMore; {
		hasMore, err = e.executionPipeline.Run(e.db, sd, tx, initialCycle, firstCycle)
		if err != nil {
			err = fmt.Errorf("updateForkChoice: %w", err)
			e.logger.Warn("Cannot update chain head", "hash", blockHash, "err", err)
			if errors.Is(err, rules.ErrInvalidBlock) {
				sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
					Status:          executionproto.ExecutionStatus_BadBlock,
					ValidationError: err.Error(),
					LatestValidHash: gointerfaces.ConvertHashToH256(rawdb.ReadHeadBlockHash(tx)),
				}, stateFlushingInParallel)
				return
			}
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}
		if hasMore {
			err = e.executionPipeline.RunPrune(e.db, tx, initialCycle)
			if err != nil {
				sendError("updateForkChoice: RunPrune after hasMore", err)
				return
			}
			if err := sd.Flush(ctx, tx); err != nil {
				sendError("updateForkChoice:flush sd after hasMore", err)
				return
			}
			sd.ClearRam(true)
			if !commitAndReopenTx() {
				return
			}
		}
	}

	// if head hash was set then success otherwise no
	headHash := rawdb.ReadHeadBlockHash(tx)
	headNumber, err := e.blockReader.HeaderNumber(ctx, tx, headHash)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		return
	}

	log := headNumber != nil && e.logger != nil
	// Update forks...
	writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)
	status := executionproto.ExecutionStatus_Success

	if headHash != blockHash {
		blockHashBlockNum, _ := e.blockReader.HeaderNumber(ctx, tx, blockHash)

		status = executionproto.ExecutionStatus_BadBlock
		validationError = "headHash and blockHash mismatch"
		if log {
			headNum := "unknown"
			if headNumber != nil {
				headNum = strconv.FormatUint(*headNumber, 10)
			}
			hashBlockNum := "unknown"
			if blockHashBlockNum != nil {
				hashBlockNum = strconv.FormatUint(*blockHashBlockNum, 10)
			}
			e.logger.Warn("bad forkchoice", "head", headHash, "head block", headNum, "hash", blockHash, "hash block", hashBlockNum)
		}
	} else {
		valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}
		if !valid {
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
				Status:          executionproto.ExecutionStatus_InvalidForkchoice,
				LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			}, stateFlushingInParallel)
			return
		}
		if err := rawdb.TruncateCanonicalChain(ctx, tx, *headNumber+1); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}

		if err := rawdbv3.TxNums.Truncate(tx, *headNumber+1); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}
		commitStart := time.Now()
		txnum, err := rawdbv3.TxNums.Max(tx, fcuHeader.Number.Uint64())
		if err != nil {
			e.logger.Warn("Failed to get txnum", "err", err)
		}
		if err := sd.Flush(ctx, tx); err != nil {
			sendError("updateForkChoice:flush sd after hasMore", err)
			return
		}
		sd.ClearRam(true)
		if err := tx.Commit(); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}
		commitTime := time.Since(commitStart)

		if e.hook != nil {
			if err := e.db.View(ctx, func(tx kv.Tx) error {
				return e.hook.AfterRun(tx, finishProgressBefore, isSynced)
			}); err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
				return
			}
		}

		// force fsync after notifications are sent
		if err := e.db.Update(ctx, func(tx kv.RwTx) error {
			return kv.IncrementKey(tx, kv.DatabaseInfo, []byte("chaindata_force"))
		}); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}

		if log {
			e.logHeadUpdated(blockHash, fcuHeader, txnum, commitTime, "head updated", stateFlushingInParallel)
		}
	}
	if *headNumber >= startPruneFrom {
		e.runPostForkchoiceInBackground(initialCycle)
	}

	sendForkchoiceReceiptWithoutWaiting(outcomeCh, &executionproto.ForkChoiceReceipt{
		LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		Status:          status,
		ValidationError: validationError,
	}, stateFlushingInParallel)
}

func (e *EthereumExecutionModule) runPostForkchoiceInBackground(initialCycle bool) {
	if !e.doingPostForkchoice.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer e.doingPostForkchoice.Store(false)
		var timings []interface{}
		// Wait for semaphore to be available
		if e.semaphore.Acquire(e.bacgroundCtx, 1) != nil {
			return
		}
		defer e.semaphore.Release(1)
		pruneStart := time.Now()
		defer UpdateForkChoicePruneDuration(pruneStart)

		if err := e.db.Update(e.bacgroundCtx, func(tx kv.RwTx) error {
			if err := e.executionPipeline.RunPrune(e.db, tx, initialCycle); err != nil {
				return err
			}
			if pruneTimings := e.executionPipeline.PrintTimings(); len(pruneTimings) > 0 {
				timings = append(timings, pruneTimings...)
			}

			return nil
		}); err != nil {
			e.logger.Error("runPostForkchoiceInBackground", "error", err)
			return
		}

		if len(timings) > 0 {
			timings = append(timings, "initialCycle", initialCycle)
			e.logger.Info("Timings: Post-Forkchoice", timings...)
		}
	}()
}

func (e *EthereumExecutionModule) logHeadUpdated(blockHash common.Hash, fcuHeader *types.Header, txnum uint64, commitTime time.Duration, msg string, debug bool) {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	blockTimings := e.forkValidator.GetTimings(blockHash)

	logArgs := []interface{}{"hash", blockHash, "number", fcuHeader.Number.Uint64()}
	if txnum != 0 {
		logArgs = append(logArgs, "txnum", txnum)
	}
	logArgs = append(logArgs, "age", common.PrettyAge(time.Unix(int64(fcuHeader.Time), 0)),
		"execution", blockTimings[engine_helpers.BlockTimingsValidationIndex])

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
	if commitTime > 0 {
		logArgs = append(logArgs, "commit", commitTime)
	}
	if !e.syncCfg.ParallelStateFlushing {
		logArgs = append(logArgs, "flushing", blockTimings[engine_helpers.BlockTimingsFlushExtendingFork])
	}

	logArgs = append(logArgs, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

	dbgLevel := log.LvlInfo
	if debug {
		dbgLevel = log.LvlDebug
	}
	e.logger.Log(dbgLevel, msg, logArgs...)
}
