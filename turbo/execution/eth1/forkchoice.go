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

package eth1

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/gointerfaces"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/engineapi/engine_helpers"
)

const startPruneFrom = 1024

type forkchoiceOutcome struct {
	receipt *execution.ForkChoiceReceipt
	err     error
}

func sendForkchoiceReceiptWithoutWaiting(ch chan forkchoiceOutcome, receipt *execution.ForkChoiceReceipt, alreadySent bool) {
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

func isDomainAheadOfBlocks(tx kv.RwTx) bool {
	doms, err := state.NewSharedDomains(tx, log.New())
	if err != nil {
		return errors.Is(err, state.ErrBehindCommitment)
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

func (e *EthereumExecutionModule) UpdateForkChoice(ctx context.Context, req *execution.ForkChoice) (*execution.ForkChoiceReceipt, error) {
	blockHash := gointerfaces.ConvertH256ToHash(req.HeadBlockHash)
	safeHash := gointerfaces.ConvertH256ToHash(req.SafeBlockHash)
	finalizedHash := gointerfaces.ConvertH256ToHash(req.FinalizedBlockHash)

	outcomeCh := make(chan forkchoiceOutcome, 1)

	// So we wait at most the amount specified by req.Timeout before just sending out
	go e.updateForkChoice(e.bacgroundCtx, blockHash, safeHash, finalizedHash, outcomeCh)
	fcuTimer := time.NewTimer(time.Duration(req.Timeout) * time.Millisecond)

	select {
	case <-fcuTimer.C:
		e.logger.Debug("treating forkChoiceUpdated as asynchronous as it is taking too long")
		return &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Status:          execution.ExecutionStatus_Busy,
		}, nil
	case outcome := <-outcomeCh:
		return outcome.receipt, outcome.err
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

func minUnwindableBlock(tx kv.Tx, number uint64) (uint64, error) {
	casted, ok := tx.(state.HasAggTx)
	if !ok {
		return 0, errors.New("tx does not support state.HasAggTx")
	}
	return casted.AggTx().(*state.AggregatorRoTx).CanUnwindToBlockNum(tx)

}

func (e *EthereumExecutionModule) updateForkChoice(ctx context.Context, originalBlockHash, safeHash, finalizedHash common.Hash, outcomeCh chan forkchoiceOutcome) {
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.updateForkChoice: ExecutionStatus_Busy")
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Status:          execution.ExecutionStatus_Busy,
		}, false)
		return
	}
	defer e.semaphore.Release(1)

	//if err := stages2.ProcessFrozenBlocks(ctx, e.db, e.blockReader, e.executionPipeline); err != nil {
	//	sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
	//	e.logger.Warn("ProcessFrozenBlocks", "error", err)
	//	return
	//}
	defer e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)

	// Update the last new block seen.
	// This is used by eth_syncing as an heuristic to determine if the node is syncing or not.
	if err := e.db.Update(ctx, func(tx kv.RwTx) error {
		num, err := e.blockReader.HeaderNumber(ctx, tx, originalBlockHash)
		if err != nil {
			return err
		}
		return rawdb.WriteLastNewBlockSeen(tx, *num)
	}); err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}

	var validationError string
	type canonicalEntry struct {
		hash   common.Hash
		number uint64
	}
	tx, err := e.db.BeginRwNosync(ctx)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}
	defer tx.Rollback()

	blockHash := originalBlockHash

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

	limitedBigJump := e.syncCfg.LoopBlockLimit > 0 && finishProgressBefore > 0 && fcuHeader.Number.Uint64()-finishProgressBefore > uint64(e.syncCfg.LoopBlockLimit-2)
	isSynced := finishProgressBefore > 0 && finishProgressBefore > e.blockReader.FrozenBlocks() && finishProgressBefore == headersProgressBefore
	if limitedBigJump {
		isSynced = false
		log.Info("[sync] limited big jump", "from", finishProgressBefore, "amount", uint64(e.syncCfg.LoopBlockLimit))
	}

	canonicalHash, err := e.canonicalHash(ctx, tx, fcuHeader.Number.Uint64())
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
		return
	}
	if fcuHeader.Number.Uint64() > 0 {
		if canonicalHash == blockHash {
			// if block hash is part of the canonical chain treat it as no-op.
			writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)
			valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
			if err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}
			if !valid {
				sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
					LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
					Status:          execution.ExecutionStatus_InvalidForkchoice,
				}, false)
				return
			}
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(blockHash),
				Status:          execution.ExecutionStatus_Success,
			}, false)
			return
		}

		// If we don't have it, too bad
		if fcuHeader == nil {
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
				Status:          execution.ExecutionStatus_MissingSegment,
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
				sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
					LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
					Status:          execution.ExecutionStatus_MissingSegment,
				}, false)
				return
			}
			currentParentHash = currentHeader.ParentHash
			if currentHeader.Number.Uint64() == 0 {
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
		minUnwindableBlock, err := minUnwindableBlock(tx, unwindTarget)
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
		if e.hook != nil {
			if err = e.hook.BeforeRun(tx, isSynced); err != nil {
				sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
				return
			}
		}
		// Run the unwind
		if err := e.executionPipeline.RunUnwind(e.db, wrap.TxContainer{Tx: tx}); err != nil {
			err = fmt.Errorf("updateForkChoice: %w", err)
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}

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
	if isDomainAheadOfBlocks(tx) {
		if err := tx.Commit(); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, false)
			return
		}
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Status:          execution.ExecutionStatus_TooFarAway,
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

	flushExtendingFork := blockHash == e.forkValidator.ExtendingForkHeadHash()
	stateFlushingInParallel := flushExtendingFork && e.syncCfg.ParallelStateFlushing
	if flushExtendingFork {
		e.logger.Debug("[updateForkchoice] Fork choice update: flushing in-memory state (built by previous newPayload)")
		if stateFlushingInParallel {
			// Send forkchoice early (We already know the fork is valid)
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(blockHash),
				Status:          execution.ExecutionStatus_Success,
				ValidationError: validationError,
			}, false)
		}
		if err := e.forkValidator.FlushExtendingFork(tx, e.accumulator); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}
	}
	// Run the forkchoice
	initialCycle := limitedBigJump
	firstCycle := false
	if _, err := e.executionPipeline.Run(e.db, wrap.TxContainer{Tx: tx}, initialCycle, firstCycle); err != nil {
		err = fmt.Errorf("updateForkChoice: %w", err)
		e.logger.Warn("Cannot update chain head", "hash", blockHash, "err", err)
		sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
		return
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
	status := execution.ExecutionStatus_Success

	if headHash != blockHash {
		status = execution.ExecutionStatus_BadBlock
		validationError = "headHash and blockHash mismatch"
		if log {
			e.logger.Warn("bad forkchoice", "head", headHash, "hash", blockHash)
		}
	} else {
		valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}
		if !valid {
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
				Status:          execution.ExecutionStatus_InvalidForkchoice,
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
		if err := tx.Commit(); err != nil {
			sendForkchoiceErrorWithoutWaiting(e.logger, outcomeCh, err, stateFlushingInParallel)
			return
		}
		commitTime := time.Since(commitStart)

		if e.hook != nil {
			if err := e.db.View(ctx, func(tx kv.Tx) error {
				return e.hook.AfterRun(tx, finishProgressBefore)
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

		var m runtime.MemStats
		dbg.ReadMemStats(&m)
		blockTimings := e.forkValidator.GetTimings(blockHash)
		logArgs := []interface{}{"head", headHash, "hash", blockHash}
		if flushExtendingFork {
			totalTime := blockTimings[engine_helpers.BlockTimingsValidationIndex]
			if !e.syncCfg.ParallelStateFlushing {
				totalTime += blockTimings[engine_helpers.BlockTimingsFlushExtendingFork]
			}
			gasUsedMgas := float64(fcuHeader.GasUsed) / 1e6
			mgasPerSec := gasUsedMgas / totalTime.Seconds()
			e.avgMgasSec = ((e.avgMgasSec * (float64(e.recordedMgasSec))) + mgasPerSec) / float64(e.recordedMgasSec+1)
			e.recordedMgasSec++
			logArgs = append(logArgs, "number", fcuHeader.Number.Uint64(), "execution", blockTimings[engine_helpers.BlockTimingsValidationIndex], "mgas/s", fmt.Sprintf("%.2f", mgasPerSec), "average mgas/s", fmt.Sprintf("%.2f", e.avgMgasSec))
			if !e.syncCfg.ParallelStateFlushing {
				logArgs = append(logArgs, "flushing", blockTimings[engine_helpers.BlockTimingsFlushExtendingFork])
			}
		}
		logArgs = append(logArgs, "commit", commitTime, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
		if log {
			e.logger.Info("head updated", logArgs...)
		}
	}
	if *headNumber >= startPruneFrom {
		e.runPostForkchoiceInBackground(initialCycle)
	}

	sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
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
			e.logger.Info("Timings: Post-Forkchoice (slower than 50ms)", timings...)
		}
	}()
}
