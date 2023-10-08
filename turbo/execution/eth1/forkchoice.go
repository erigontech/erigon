package eth1

import (
	"context"
	"fmt"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

type forkchoiceOutcome struct {
	receipt *execution.ForkChoiceReceipt
	err     error
}

func sendForkchoiceReceiptWithoutWaiting(ch chan forkchoiceOutcome, receipt *execution.ForkChoiceReceipt) {
	select {
	case ch <- forkchoiceOutcome{receipt: receipt}:
	default:
	}
}

func sendForkchoiceErrorWithoutWaiting(ch chan forkchoiceOutcome, err error) {
	select {
	case ch <- forkchoiceOutcome{err: err}:
	default:
	}
}

// verifyForkchoiceHashes verifies the finalized and safe hash of the forkchoice state
func (e *EthereumExecutionModule) verifyForkchoiceHashes(ctx context.Context, tx kv.Tx, blockHash, finalizedHash, safeHash libcommon.Hash) (bool, error) {
	// Client software MUST return -38002: Invalid forkchoice state error if the payload referenced by
	// forkchoiceState.headBlockHash is VALID and a payload referenced by either forkchoiceState.finalizedBlockHash or
	// forkchoiceState.safeBlockHash does not belong to the chain defined by forkchoiceState.headBlockHash
	headNumber := rawdb.ReadHeaderNumber(tx, blockHash)
	finalizedNumber := rawdb.ReadHeaderNumber(tx, finalizedHash)
	safeNumber := rawdb.ReadHeaderNumber(tx, safeHash)

	if finalizedHash != (libcommon.Hash{}) && finalizedHash != blockHash {
		canonical, err := e.isCanonicalHash(ctx, tx, finalizedHash)
		if err != nil {
			return false, err
		}
		if !canonical || *headNumber <= *finalizedNumber {
			return false, nil
		}

	}
	if safeHash != (libcommon.Hash{}) && safeHash != blockHash {
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
	go e.updateForkChoice(ctx, blockHash, safeHash, finalizedHash, outcomeCh)
	fcuTimer := time.NewTimer(time.Duration(req.Timeout) * time.Millisecond)

	select {
	case <-fcuTimer.C:
		e.logger.Debug("treating forkChoiceUpdated as asynchronous as it is taking too long")
		return &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			Status:          execution.ExecutionStatus_Busy,
		}, nil
	case outcome := <-outcomeCh:
		return outcome.receipt, outcome.err
	}

}

func writeForkChoiceHashes(tx kv.RwTx, blockHash, safeHash, finalizedHash libcommon.Hash) {
	if finalizedHash != (libcommon.Hash{}) {
		rawdb.WriteForkchoiceFinalized(tx, finalizedHash)
	}
	if safeHash != (libcommon.Hash{}) {
		rawdb.WriteForkchoiceSafe(tx, safeHash)
	}
	rawdb.WriteHeadBlockHash(tx, blockHash)
	rawdb.WriteForkchoiceHead(tx, blockHash)
}

func (e *EthereumExecutionModule) updateForkChoice(ctx context.Context, blockHash, safeHash, finalizedHash libcommon.Hash, outcomeCh chan forkchoiceOutcome) {
	if !e.semaphore.TryAcquire(1) {
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			Status:          execution.ExecutionStatus_Busy,
		})
		return
	}
	defer e.semaphore.Release(1)

	type canonicalEntry struct {
		hash   libcommon.Hash
		number uint64
	}
	tx, err := e.db.BeginRwNosync(ctx)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	defer tx.Rollback()

	defer e.forkValidator.ClearWithUnwind(e.accumulator, e.stateChangeConsumer)
	// Step one, find reconnection point, and mark all of those headers as canonical.
	fcuHeader, err := e.blockReader.HeaderByHash(ctx, tx, blockHash)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	if fcuHeader == nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, fmt.Errorf("forkchoice: block %x not found or was marked invalid", blockHash))
		return
	}
	canonicalHash, err := e.blockReader.CanonicalHash(ctx, tx, fcuHeader.Number.Uint64())
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}

	if canonicalHash == blockHash {
		// if block hash is part of the canonical chain treat it as no-op.
		writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)
		valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
		if !valid {
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
				Status:          execution.ExecutionStatus_InvalidForkchoice,
			})
			return
		}
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(blockHash),
			Status:          execution.ExecutionStatus_Success,
		})
		return
	}

	// If we don't have it, too bad
	if fcuHeader == nil {
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			Status:          execution.ExecutionStatus_MissingSegment,
		})
		return
	}
	currentParentHash := fcuHeader.ParentHash
	currentParentNumber := fcuHeader.Number.Uint64() - 1
	isCanonicalHash, err := rawdb.IsCanonicalHash(tx, currentParentHash, currentParentNumber)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
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
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
		if currentHeader == nil {
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
				LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
				Status:          execution.ExecutionStatus_MissingSegment,
			})
			return
		}
		currentParentHash = currentHeader.ParentHash
		currentParentNumber = currentHeader.Number.Uint64() - 1
		isCanonicalHash, err = rawdb.IsCanonicalHash(tx, currentParentHash, currentParentNumber)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
	}

	e.executionPipeline.UnwindTo(currentParentNumber, libcommon.Hash{})
	if e.historyV3 {
		if err := rawdbv3.TxNums.Truncate(tx, currentParentNumber); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
	}

	var finishProgressBefore, headersProgressBefore uint64
	if finishProgressBefore, err = stages.GetStageProgress(tx, stages.Finish); err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	if headersProgressBefore, err = stages.GetStageProgress(tx, stages.Headers); err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}

	isSynced := finishProgressBefore > 0 && finishProgressBefore > e.blockReader.FrozenBlocks() && finishProgressBefore == headersProgressBefore
	if e.hook != nil {
		if err = e.hook.BeforeRun(tx, isSynced); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
	}

	// Run the unwind
	if err := e.executionPipeline.RunUnwind(e.db, tx); err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}

	if e.historyV3 {
		if err := rawdbv3.TxNums.Truncate(tx, currentParentNumber); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
	}
	// Mark all new canonicals as canonicals
	for _, canonicalSegment := range newCanonicals {
		chainReader := stagedsync.NewChainReaderImpl(e.config, tx, e.blockReader, e.logger)

		b := rawdb.ReadBlock(tx, canonicalSegment.hash, canonicalSegment.number)

		if b == nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, fmt.Errorf("unexpected chain cap: %d", canonicalSegment.number))
			return
		}

		if err := e.engine.VerifyHeader(chainReader, b.Header(), true); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}

		if err := e.engine.VerifyUncles(chainReader, b.Header(), b.Uncles()); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}

		if err := rawdb.WriteCanonicalHash(tx, canonicalSegment.hash, canonicalSegment.number); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
		if e.historyV3 {
			if err := rawdb.AppendCanonicalTxNums(tx, canonicalSegment.number); err != nil {
				sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
				return
			}
		}
	}
	// Set Progress for headers and bodies accordingly.
	if err := stages.SaveStageProgress(tx, stages.Headers, fcuHeader.Number.Uint64()); err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, fcuHeader.Number.Uint64()); err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	if err = rawdb.WriteHeadHeaderHash(tx, blockHash); err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	if blockHash == e.forkValidator.ExtendingForkHeadHash() {
		e.logger.Info("[updateForkchoice] Fork choice update: flushing in-memory state (built by previous newPayload)")
		if err := e.forkValidator.FlushExtendingFork(tx, e.accumulator); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
	}
	// Run the forkchoice
	if err := e.executionPipeline.Run(e.db, tx, false); err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	// if head hash was set then success otherwise no
	headHash := rawdb.ReadHeadBlockHash(tx)
	headNumber := rawdb.ReadHeaderNumber(tx, headHash)
	log := headNumber != nil && e.logger != nil
	// Update forks...
	writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)
	status := execution.ExecutionStatus_Success
	if headHash != blockHash {
		status = execution.ExecutionStatus_BadBlock
		if log {
			e.logger.Warn("bad forkchoice", "head", headHash, "hash", blockHash)
			h, _ := e.getHeader(ctx, tx, headHash, *headNumber)
			fmt.Println(h.Hash())
		}
	} else {
		valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
		if !valid {
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
				Status:          execution.ExecutionStatus_InvalidForkchoice,
				LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			})
			return
		}
		if err := rawdb.TruncateCanonicalChain(ctx, tx, *headNumber+1); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}

		if err := tx.Commit(); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
		if e.hook != nil {
			if err := e.db.View(ctx, func(tx kv.Tx) error {
				return e.hook.AfterRun(tx, finishProgressBefore)
			}); err != nil {
				sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
				return
			}
		}
		if log {
			e.logger.Info("head updated", "hash", headHash, "number", *headNumber)
		}

		if err := e.db.Update(ctx, func(tx kv.RwTx) error { return e.executionPipeline.RunPrune(e.db, tx, false) }); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
	}

	sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
		LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		Status:          status,
	})
}
