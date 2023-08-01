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

	outcomeCh := make(chan forkchoiceOutcome)

	// So we wait at most the amount specified by req.Timeout before just sending out
	go e.updateForkChoice(ctx, blockHash, safeHash, finalizedHash, outcomeCh)
	fcuTimer := time.NewTimer(time.Duration(req.Timeout) * time.Millisecond)

	select {
	case <-fcuTimer.C:
		e.logger.Debug("treating forkChoiceUpdated as asyncronous as it is taking too long")
		return &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			Status:          execution.ExecutionStatus_Busy,
		}, nil
	case outcome := <-outcomeCh:
		fmt.Println(outcome.receipt)
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
	tx, err := e.db.BeginRw(ctx)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	defer tx.Rollback()

	defer e.forkValidator.ClearWithUnwind(tx, e.accumulator, e.stateChangeConsumer)

	// Step one, find reconnection point, and mark all of those headers as canonical.
	fcuHeader, err := e.blockReader.HeaderByHash(ctx, tx, blockHash)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	canonicalHash, err := e.blockReader.CanonicalHash(ctx, tx, fcuHeader.Nonce.Uint64())
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}

	if canonicalHash == blockHash {
		// if block hash is part of the canononical chain treat it as no-op.
		writeForkChoiceHashes(tx, blockHash, safeHash, finalizedHash)
		valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
		if !valid {
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{Status: execution.ExecutionStatus_InvalidForkchoice})
			return
		}
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(blockHash),
			Status:          execution.ExecutionStatus_Success,
		})
		return
	}

	// If we dont have it, too bad
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
	newCanonicals := make([]*canonicalEntry, 0, 2048)
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
	// Run the forkchoice
	if err := e.executionPipeline.Run(e.db, tx, false); err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	if e.hook != nil {
		if err = e.hook.AfterRun(tx, finishProgressBefore); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
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
			e.logger.Warn("bad forkchoice", "hash", headHash)
		}
	} else {
		valid, err := e.verifyForkchoiceHashes(ctx, tx, blockHash, finalizedHash, safeHash)
		if err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
		if !valid {
			sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{Status: execution.ExecutionStatus_InvalidForkchoice})
			return
		}
		if err := tx.Commit(); err != nil {
			sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
			return
		}
		if log {
			e.logger.Info("head updated", "hash", headHash, "number", *headNumber)
		}
	}

	sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
		LatestValidHash: gointerfaces.ConvertHashToH256(headHash),
		Status:          status,
	})
}
