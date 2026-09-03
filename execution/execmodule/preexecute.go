package execmodule

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/state"
)

// copyFrontierChainTables propagates the single frontier chain's raw-table bookkeeping — canonical hashes
// (kv.HeaderCanonical) and the txNum index (kv.MaxTxNum) — from a predecessor frontier block's overlay (src)
// into a successor's overlay (dst), so the successor continues ONE chain from the last committed hash. These
// entries live only in overlay mem for an uncommitted frontier block (never the committed DB), and raw
// tables do not chain through SetParent, so they must be copied. src's own backing tx is per-round and
// already rolled back, so give it a live one (roTx) for the read; only raw index/hash tables are touched
// (no commitment domain), so this does not disturb src's commitment trie.
func copyFrontierChainTables(src, dst *membatchwithdb.MemoryMutation, roTx kv.TemporalTx) error {
	src.UpdateTxn(roTx)
	for _, table := range []string{kv.HeaderCanonical, kv.MaxTxNum} {
		c, err := src.Cursor(table)
		if err != nil {
			return err
		}
		for k, v, cerr := c.First(); k != nil; k, v, cerr = c.Next() {
			if cerr != nil {
				c.Close()
				return cerr
			}
			if putErr := dst.Put(table, common.Copy(k), common.Copy(v)); putErr != nil {
				c.Close()
				return putErr
			}
		}
		c.Close()
	}
	return nil
}

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
	return e.preExecuteLocked(ctx, blockHash, blockNumber)
}

// preExecuteLocked is PreExecute's body with the caller ALREADY holding e.semaphore. PreExecute TryAcquires and
// calls this; the atomic assemble path (opening the successor flashblock inside SealBlock) calls it directly.
func (e *ExecModule) preExecuteLocked(ctx context.Context, blockHash common.Hash, blockNumber uint64) (ValidationResult, error) {
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
	flashUpdate := e.preExec.CheckUpdate(blockNumber, body.Transactions)
	reuse := flashUpdate.IsUpdate && flashUpdate.SD != nil
	prefixLen := 0 // already-executed txs (kept as-is); only the NEW suffix past this is candidate-filtered
	if reuse {
		prefixLen = flashUpdate.PrefixLen
	}

	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return ValidationResult{}, err
	}
	defer roTx.Rollback()

	var doms *execctx.SharedDomains
	var frontierExtension bool // set when this fresh block extends a LIVE frontier parent (parenting)
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
		// Resolve the block's first txNum through the SD's OWN overlay, not the bare DB tx: an in-progress
		// block is not in the DB, so a DB-only read resolves to a STALE lower block and execution resumes
		// inside an ALREADY-SEALED block, replaying its txs ("nonce too low"). The overlay carries this
		// block's txNum index (written at open, and copied forward along the frontier chain).
		resumeTx := kv.TemporalTx(roTx)
		if ov := doms.BlockOverlay(); ov != nil {
			resumeTx = ov
		}
		if minTxNum, merr := e.blockReader.TxnumReader().Min(ctx, resumeTx, blockNumber); merr == nil {
			doms.SetPreExecStart(minTxNum + uint64(flashUpdate.PrefixLen))
			defer doms.ClearPreExecStart()
		}
	} else {
		// Chain onto the predecessor's still-live pre-executed SD, captured BEFORE constructing this block's
		// SD so its initial SeekCommitment resolves through the parent's LIVE commitment rather than the
		// lagging DB (the predecessor has opened but not canonicalised). Falls back to currentContext
		// (canonical) when there is no live ancestor — the first block after a restart.
		frontierParent := e.preExec.ParentFor(blockNumber)
		parent := frontierParent
		if parent == nil {
			parent = e.currentContext
		}
		frontierExtension = frontierParent != nil

		// First round: fresh SD + overlay, exactly like ValidateChain opens one. The fresh SD starts
		// with an empty flashblock receipt accumulator, so this block's seal derives the COMPUTED
		// header fields over ONLY this block's body — no explicit reset needed. WithParent attaches the
		// read-through parent BEFORE the constructor's SeekCommitment so the block positions its trie on the
		// parent's live state (parenting), not the lagging DB ([[consensus_advance_untested_regression]]).
		if doms, err = execctx.NewSharedDomains(ctx, roTx, e.logger, execctx.WithParent(parent)); err != nil {
			return ValidationResult{}, err
		}
		doms.SetInMemHistoryReads(false)

		if err = doms.InitBlockOverlay(roTx, roTx.Debug().Dirs().Tmp); err != nil {
			doms.Close()
			return ValidationResult{}, err
		}

		// SINGLE-CHAIN PROPAGATION ([[consensus_advance_untested_regression]]): the frontier is ONE linear
		// chain from the last committed ("known-good") hash, so FCU sees a single chain. But its per-block
		// bookkeeping — canonical hashes (kv.HeaderCanonical) and the txNum index (kv.MaxTxNum) — lives only
		// in the predecessor's overlay, never the committed DB, and raw tables do NOT chain through SetParent
		// (that read-through only covers domains). Copy that chain state from the frontier parent's overlay
		// into THIS block's overlay so it CONTINUES the one chain (AppendCanonicalTxNums then computes off
		// the parent's true max; the close finds the parent's canonical hash) rather than starting a detached
		// one off stale genesis. Self-contained in this block's mem → survives UpdateTxn, needs no live parent.
		if frontierParent != nil {
			if pov := frontierParent.BlockOverlay(); pov != nil {
				if err = copyFrontierChainTables(pov, doms.BlockOverlay(), roTx); err != nil {
					doms.Close()
					return ValidationResult{}, fmt.Errorf("copy frontier chain tables: %w", err)
				}
			}
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
	//
	// ALSO skip it on a FRONTIER EXTENSION ([[consensus_advance_untested_regression]] merge-vs-parenting):
	// the block already opened positioned on its live frontier PARENT (WithParent → the constructor's
	// SeekCommitment restored the parent's commitment). unwindToCommonCanonical would re-align it to the
	// canonical DB instead — but the frontier parent is NOT yet in the DB (FCU lags), so the unwind reads
	// the predecessor-of-parent's stale state and OVERWRITES the correct parent position (the bug: block N+1
	// reset to block N-1). Parenting is the source of truth here, not the lagging DB.
	if !reuse && !frontierExtension {
		if err = e.unwindToCommonCanonical(doms, tx, header); err != nil {
			doms.Close()
			return ValidationResult{}, err
		}
	}

	// CANDIDATE FILTER (start of the pre-exec cycle): the NEW suffix (past the executed prefix) are CANDIDATES
	// — the DAG can hand us a tx that can't apply against the accumulated state (e.g. a stale/duplicate nonce).
	// Drop those here, against the SAME doms execution is about to use (a fresh, in-cycle read → deterministic),
	// so an invalid candidate is filtered rather than breaking block execution with a "nonce too low" BadBlock.
	// It does NOT address WHY such a tx appears — only that it no longer fails the block. A no-op when every
	// candidate applies (the common case). See filterCandidatesByNonce.
	// Defence-in-depth: PreExecute is now normally fed an ALREADY-filtered body (PreExecuteFlashblock filters
	// the stream before building+inserting), so this is a no-op on the happy path. It stays as a guard for any
	// caller that inserts an unfiltered body directly. Filters only the NEW suffix (past the executed prefix)
	// against the SAME SD execution uses.
	if len(body.Transactions) > prefixLen {
		fr := state.NewReaderV3(doms.AsGetter(tx))
		suffix := body.Transactions[prefixLen:]
		keptSuffix := filterCandidatesByNonce(fr, types.LatestSignerForChainID(e.config.ChainID), suffix)
		if len(keptSuffix) != len(suffix) {
			e.logger.Info("[execmodule] pre-exec filtered inapplicable candidates",
				"block", blockNumber, "dropped", len(suffix)-len(keptSuffix), "kept", len(keptSuffix))
			kept := make([]types.Transaction, 0, prefixLen+len(keptSuffix))
			kept = append(kept, body.Transactions[:prefixLen]...)
			kept = append(kept, keptSuffix...)
			body = &types.Body{Transactions: kept, Uncles: body.Uncles, Withdrawals: body.Withdrawals}
		}
	}

	// EXECUTE only the new txs into the maintained SD (offset resumes past the prefix). PreExecute is
	// EXECUTION, not validation — so it runs the block through ExecuteInto, which touches no validation
	// state. (It used to call ValidatePayload, whose side effect of storing doms as fv.sharedDom is what
	// put producer state in the validation slot and subjected it to newPayload/FCU teardown.)
	status, notifications, validationError, criticalError := e.forkValidator.ExecuteInto(ctx, doms, tx, header, body.RawBody())
	if criticalError != nil {
		return ValidationResult{}, criticalError
	}
	lvh := header.Hash()

	// Record the block in the pre-exec frontier — its own space, keyed by the hash it currently carries
	// (the header re-hashes every round as the body grows, and the seal re-keys it again).
	if status == engine_types.ValidStatus {
		if reuse {
			e.preExec.SetActiveHead(lvh, blockNumber)
			e.preExec.SetActiveNotifications(notifications)
		} else {
			e.preExec.Open(doms, lvh, blockNumber)
			e.preExec.SetActiveNotifications(notifications)
		}
		e.preExec.RecordTxHashes(body.Transactions)
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
