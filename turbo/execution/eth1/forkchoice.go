package eth1

import (
	"context"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
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
	case ch <- forkchoiceOutcome{
		receipt: &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			Status:          execution.ValidationStatus_Busy,
		},
	}:
	default:
	}
}

func sendForkchoiceErrorWithoutWaiting(ch chan forkchoiceOutcome, err error) {
	select {
	case ch <- forkchoiceOutcome{err: err}:
	default:
	}
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
		return &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			Status:          execution.ValidationStatus_Busy,
		}, nil
	case outcome := <-outcomeCh:
		return outcome.receipt, outcome.err
	}

}

func (e *EthereumExecutionModule) updateForkChoice(ctx context.Context, blockHash, safeHash, finalizedHash libcommon.Hash, outcomeCh chan forkchoiceOutcome) {
	if !e.semaphore.TryAcquire(1) {
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			Status:          execution.ValidationStatus_Busy,
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
	// defer e.forkValidator.ClearWithUnwind(tx, e.notifications.Accumulator, e.notifications.StateChangesConsumer)

	// Step one, find reconnection point, and mark all of those headers as canonical.
	fcuHeader, err := e.blockReader.HeaderByHash(ctx, tx, blockHash)
	if err != nil {
		sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
		return
	}
	// If we dont have it, too bad
	if fcuHeader == nil {
		sendForkchoiceReceiptWithoutWaiting(outcomeCh, &execution.ForkChoiceReceipt{
			LatestValidHash: gointerfaces.ConvertHashToH256(libcommon.Hash{}),
			Status:          execution.ValidationStatus_MissingSegment,
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
				Status:          execution.ValidationStatus_MissingSegment,
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
	if currentParentNumber != fcuHeader.Number.Uint64()-1 {
		e.executionPipeline.UnwindTo(currentParentNumber, libcommon.Hash{})
		if e.historyV3 {
			if err := rawdbv3.TxNums.Truncate(tx, currentParentNumber); err != nil {
				sendForkchoiceErrorWithoutWaiting(outcomeCh, err)
				return
			}
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
	// if head hash was set then success otherwise no
	headHash := rawdb.ReadHeadBlockHash(tx)
	headNumber := rawdb.ReadHeaderNumber(tx, headHash)
	log := headNumber != nil && e.logger != nil
	// Update forks...
	rawdb.WriteForkchoiceFinalized(tx, finalizedHash)
	rawdb.WriteForkchoiceSafe(tx, safeHash)
	rawdb.WriteHeadBlockHash(tx, blockHash)

	status := execution.ValidationStatus_Success
	if headHash != blockHash {
		status = execution.ValidationStatus_BadBlock
		if log {
			e.logger.Warn("bad forkchoice", "hash", headHash)
		}
	} else {
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
