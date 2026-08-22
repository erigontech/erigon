package execmodule

import (
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

// PreExecute is the flashblock PRE-EXECUTION entry, sitting beside ValidateChain and
// sharing the same fork-validator SharedDomains so a subsequent ValidateChain / newPayload
// finalises what it builds (and FCU commits it). It incrementally executes a block's NEW
// transactions into the ONE MAINTAINED SharedDomains, carrying accumulated state forward
// across rounds, firing execobserver.OnTx once per new tx — the generic flashblocks builder
// (any tx source). It runs NO finished-block checks (root/gas belong to the seal/validate).
//
// The single-execution guarantee: because the SD is MAINTAINED (not recreated as ValidateChain
// does), its committed txNum already reflects the prefix already executed, so exec3 resumes at
// the new txs and does not re-run the prefix. PreExecute records the accumulated flashblock tx
// hashes, so ValidateChain(sameBlock) later sees a 100%-prefix match (CheckFlashblockUpdate) and
// validates the root over this SD with ZERO re-execution — one execution total.
//
// The caller InsertBlocks the growing in-progress block (fixed context: number/timestamp/parent
// set once) before each PreExecute, exactly as it would before ValidateChain.
func (e *ExecModule) PreExecute(ctx context.Context, blockHash common.Hash, blockNumber uint64) (ValidationResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return ValidationResult{ValidationStatus: ExecutionStatusBusy}, nil
	}
	defer e.semaphore.Release(1)

	e.currentContext.ResetPendingUpdates()

	var (
		header *types.Header
		body   *types.Body
		err    error
	)
	// Read header/body from the in-progress block overlay (InsertBlocks writes there before flush).
	if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
		overlay := e.currentContext.BlockOverlay()
		roTx, rerr := e.db.BeginTemporalRo(ctx)
		if rerr != nil {
			return ValidationResult{}, rerr
		}
		defer roTx.Rollback()
		overlay.UpdateTxn(roTx)
		if header, err = e.blockReader.Header(ctx, overlay, blockHash, blockNumber); err != nil {
			return ValidationResult{}, err
		}
		if body, err = e.blockReader.BodyWithTransactions(ctx, overlay, blockHash, blockNumber); err != nil {
			return ValidationResult{}, err
		}
	} else {
		if err = e.db.View(ctx, func(tx kv.Tx) error {
			if header, err = e.blockReader.Header(ctx, tx, blockHash, blockNumber); err != nil {
				return err
			}
			body, err = e.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
			return err
		}); err != nil {
			return ValidationResult{}, err
		}
	}
	if header == nil || body == nil {
		return ValidationResult{ValidationStatus: ExecutionStatusMissingSegment}, nil
	}

	// Flashblock prefix detection against the in-progress flashblock.
	flashUpdate := e.forkValidator.CheckFlashblockUpdate(blockNumber, body.Transactions)
	reuse := flashUpdate.IsUpdate && flashUpdate.SD != nil

	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return ValidationResult{}, err
	}
	defer roTx.Rollback()

	var doms *execctx.SharedDomains
	if reuse {
		// CARRY FORWARD: reuse the in-progress flashblock SD. Refresh its overlay's backing tx
		// (the prior round's roTx is gone) while keeping the accumulated in-memory state, so the
		// committed txNum still marks the executed prefix and exec3 resumes at the new txs only.
		doms = flashUpdate.SD
		doms.BlockOverlay().UpdateTxn(roTx)
		// Resuming mid-block re-derives any needed prior state via in-memory history reads (GetAsOf) —
		// enable them on the carried-forward SD.
		doms.SetInMemHistoryReads(true)
		// PRE-EXEC start: tell exec to resume PAST the already-executed prefix instead of at the block
		// start (which SeekCommitment would report, since fork-validation never commits). resume =
		// block-min txNum + PrefixLen — skips the start-system tx and the PrefixLen executed REGULAR
		// txs. The block-end system tx is NOT skipped: it shifts as the body grows and re-runs each
		// round (op-rbuilder revert/reapply). The skip-loop (exec3.go) then creates tasks only for the
		// new txs → prefix not re-executed, OnTx fires once, state comes from the carried-forward SD.
		if minTxNum, merr := e.blockReader.TxnumReader().Min(ctx, roTx, blockNumber); merr == nil {
			doms.SetPreExecStart(minTxNum + uint64(flashUpdate.PrefixLen))
			defer doms.ClearPreExecStart()
		}
	} else {
		// First round: fresh SD + overlay, exactly like ValidateChain opens one. The fresh SD starts
		// with an empty flashblock receipt accumulator, so this block's seal derives the COMPUTED
		// header fields over ONLY this block's body — no explicit reset needed.
		if doms, err = execctx.NewSharedDomains(ctx, roTx, e.logger); err != nil {
			return ValidationResult{}, err
		}
		doms.SetInMemHistoryReads(false)
		if err = doms.InitBlockOverlay(roTx, roTx.Debug().Dirs().Tmp); err != nil {
			doms.Close()
			return ValidationResult{}, err
		}
		if e.currentContext != nil {
			doms.SetParent(e.currentContext)
		}
	}

	tx := doms.BlockOverlay()

	// Flush the InsertBlocks overlay (this round's block header/body) into the exec overlay so
	// unwindToCommonCanonical and the parallel exec goroutine see this block's data.
	if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
		if err = e.currentContext.BlockOverlay().Flush(ctx, tx); err != nil {
			if !reuse {
				doms.Close()
			}
			return ValidationResult{}, err
		}
	}
	doms.SetStateCache(e.stateCache)

	if err = e.unwindToCommonCanonical(doms, tx, header); err != nil {
		if !reuse {
			doms.Close()
		}
		return ValidationResult{}, err
	}

	// EXECUTE only the new txs into the maintained SD (offset resumes past the prefix). The
	// finished-block checks (gas/root) are gated off for the fork-validation flow — PreExecute
	// is execution, not validation. ValidatePayload stores doms as fv.sharedDom (the guard there
	// skips closing it when we pass it straight back in).
	status, lvh, validationError, criticalError := e.forkValidator.ValidatePayload(ctx, doms, tx, header, body.RawBody(), e.logger)
	if criticalError != nil {
		return ValidationResult{}, criticalError
	}

	// Record the accumulated tx hashes as the in-progress flashblock so the next PreExecute (and
	// the final ValidateChain) detect the prefix and skip re-execution.
	if status == engine_types.ValidStatus {
		e.forkValidator.RecordFlashblockTxHashes(body.Transactions)
		if dispatcher := e.pipelineExecutor.Dispatcher(); dispatcher != nil && len(body.Transactions) > 0 {
			txHashes := make([]common.Hash, len(body.Transactions))
			for i, t := range body.Transactions {
				txHashes[i] = t.Hash()
			}
			dispatcher.OnTransactionValidated(txHashes)
		}
	}

	validationStatus := ExecutionStatusSuccess
	if status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil {
		validationStatus = ExecutionStatusBadBlock
	}
	res := ValidationResult{ValidationStatus: validationStatus, LatestValidHash: lvh}
	if validationError != nil {
		res.ValidationError = validationError.Error()
	}
	// Surface the root the executor computed this round off the maintained SD (STEP 3b: the seal
	// will build the sealed header from this instead of the deferred Root{}).
	if root := doms.LastComputedRoot(); len(root) > 0 {
		res.ComputedRoot = common.BytesToHash(root)
	}
	// Surface the accumulated flashblock receipt count (== body txs executed so far) — the seal
	// derives ReceiptHash/Bloom/GasUsed from these accumulated receipts (STEP 3b slice 2b).
	res.FlashblockReceiptCount = len(doms.FlashblockReceipts())
	return res, nil
}
