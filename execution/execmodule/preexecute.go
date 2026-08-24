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
		// Frontier SD chain ([[venue_exec_on_round_plan]]): parent this new block's SD against the
		// PREVIOUS block's still-live pre-executed SD when one is parked on the fork validator's
		// auxiliary stack (i.e. the previous block opened but has not canonicalised yet — the
		// decoupled-boundary/under-load case). That carries the previous block's accumulated state
		// forward so this block reads the true frontier, not stale canonical state. Recursive
		// read-through then walks parent→…→DB. Fall back to currentContext (canonical) only when no
		// previous frontier SD is parked — the steady-state case where the previous block already
		// canonicalised. The link is captured ONCE here, at open, and is immutable for this block.
		if parent := e.forkValidator.NewestFrontierSD(); parent != nil {
			doms.SetParent(parent)
		} else if e.currentContext != nil {
			doms.SetParent(e.currentContext)
		}
	}

	// Mark this as a flashblock accumulation round → exec skips the per-round block-END (it belongs
	// to the CLOSE, which runs once via ValidateChain over a fresh SD where this flag is unset).
	doms.SetFlashblockAccumulating(true)

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

	// On a CARRY-FORWARD (reuse) round we are extending the SAME in-progress block with more txs; the
	// maintained SD already holds the accumulated state and its commitment trie the progressive fold.
	// unwindToCommonCanonical unwinds that trie back to the block's PARENT (common canonical ancestor) —
	// correct when validating a fresh fork, but here it DISCARDS the prior round's fold, so each round
	// would fold only its own txs onto the parent and the seal would diverge. Skip it on reuse; run it
	// only on the first (fresh-SD) round to align to the parent before executing the block.
	if !reuse {
		if err = e.unwindToCommonCanonical(doms, tx, header); err != nil {
			doms.Close()
			return ValidationResult{}, err
		}
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
	// Surface the output side ONLY on a successful pre-exec. On a FAILED round (e.g. a nonce gap in the
	// accumulated body) the fork validator CLOSES the SD (fork_validator.go), so GetCommitmentContext()
	// returns nil and reading the trie would nil-deref and CRASH the whole node — a bad round must fail
	// gracefully as BadBlock, not panic. Surface the state root by reading the accumulated SD's commitment
	// trie (same in-tree pattern as exec_module_test.go), and the accumulated flashblock receipt count.
	if validationStatus == ExecutionStatusSuccess {
		if cc := doms.GetCommitmentContext(); cc != nil {
			if root, rerr := cc.Trie().RootHash(); rerr == nil && len(root) > 0 {
				res.ComputedRoot = common.BytesToHash(root)
			}
		}
		res.FlashblockReceiptCount = len(doms.FlashblockReceipts())
	}
	return res, nil
}
