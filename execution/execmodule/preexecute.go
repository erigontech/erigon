package execmodule

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
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
	var stagingBase *execctx.SharedDomains
	var frontierExtension bool // set when this fresh block extends a LIVE frontier parent (parenting)
	if reuse {
		// CARRY FORWARD: the in-progress flashblock SD holds everything this block has executed so far, so its
		// committed txNum still marks the executed prefix and exec3 resumes at the new txs only. Refresh its
		// overlay's backing tx — the prior round's roTx is gone.
		flashUpdate.SD.BlockOverlay().UpdateTxn(roTx)

		// STAGE an INCREMENTAL round rather than executing into that SD directly. The round runs into a child
		// parented on it: reads fall through, so the block stays cumulative and nothing is copied, but the
		// round's domain WRITES land only in the child. MERGE is then the single commit point — a round that
		// overruns its budget or fails is dropped by closing the child, and the block is exactly what it was
		// before the round started.
		//
		// ⚠ KNOWN LIMITATION, not a design boundary. prefixLen==0 is the block's FIRST CONTENT ROUND — the
		// atomic open creates the successor as an empty block at the close, and the first round carries into
		// it with nothing executed yet (see CheckUpdate). A body that does not extend what this SD executed
		// returns IsUpdate=false and never reaches here at all.
		//
		// So this excludes the commonest round in the system from staging, which is exactly the round the
		// valve most needs to cover. Staging it currently fails three marker/tail-drain tests on the driver's
		// sealed-hold accounting (inFlight.FilterFeed re-offers a sealed body tx) — receipts and body agree at
		// the seal, so it is not the receipt fold. UNDIAGNOSED. Remove this guard once it is understood; do
		// not treat it as correct.
		if flashUpdate.PrefixLen > 0 {
			stagingBase = flashUpdate.SD
		}
		if stagingBase != nil {
			if doms, err = execctx.NewSharedDomains(ctx, roTx, e.logger, execctx.WithParent(stagingBase)); err != nil {
				return ValidationResult{}, err
			}
			// The overlay and the COMMITMENT belong to the BLOCK, not the round. Execution advances the
			// commitment trie as it runs, cumulatively across the block's rounds, so the child adopts the
			// block's context — a child with its own context would advance a fresh trie seeded at the parent's
			// root and hand that same root straight back, the round's work invisible. The overlay likewise
			// carries block metadata written by the insert, identical whether the round is kept or dropped.
			doms.BorrowBlockOverlay(stagingBase)
			doms.AdoptCommitmentContext(stagingBase)
		} else {
			doms = flashUpdate.SD
		}
		// Resuming mid-block re-derives any needed prior state via in-memory history reads (GetAsOf).
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
	// A round that does not go on to register its SD in the frontier owns it, and has to close it. Only the
	// fresh-SD path can reach here holding one nobody else knows about: the carry-forward path is executing
	// into the frontier's own generation, which the frontier closes.
	registered := false
	defer func() {
		if !registered {
			doms.Close()
		}
	}()
	if criticalError != nil {
		return ValidationResult{}, criticalError
	}
	lvh := header.Hash()

	// Record the block in the pre-exec frontier — its own space, keyed by the hash it currently carries
	// (the header re-hashes every round as the body grows, and the seal re-keys it again).
	if status == engine_types.ValidStatus {
		if stagingBase != nil {
			if merr := stagingBase.Merge(ctx, stagingBase.TxNum(), doms, doms.TxNum(), true); merr != nil {
				return ValidationResult{}, fmt.Errorf("commit pre-exec round num=%d: %w", blockNumber, merr)
			}
			registered = true
			doms = stagingBase
			e.preExec.SetActiveHead(lvh, blockNumber)
			e.preExec.SetActiveNotifications(notifications)
		} else {
			e.preExec.Open(doms, lvh, blockNumber)
			e.preExec.SetActiveNotifications(notifications)
			registered = true
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
	// Surface the output side ONLY on a successful pre-exec. A failed round (e.g. a nonce gap in the
	// accumulated body) may have no commitment context to read, so reading the trie unconditionally would
	// nil-deref and CRASH the whole node — a bad round must fail gracefully as BadBlock, not panic.
	//
	// (This used to say the fork validator closes the SD on a failed round. It does not: ExecuteInto touches
	// no validation state and closes nothing. The SD is the caller's, which is what the registered/defer
	// above is for.)
	if validationStatus == ExecutionStatusSuccess {
		if cc := doms.GetCommitmentContext(); cc != nil {
			if root, rerr := cc.Trie().RootHash(); rerr == nil && len(root) > 0 {
				res.ComputedRoot = common.BytesToHash(root)
			}
		}
		receipts := doms.FlashblockReceipts()
		res.FlashblockReceiptCount = len(receipts)
		// The body's gas so far, measured. The driver needs it to decide what else the block can still
		// hold: a per-tx gas ESTIMATE cannot make that decision, because it is as free to be under as
		// over, and under means a body that exceeds the gas limit — which is only discovered at the fork
		// choice, by which point the transaction is already in the body exec maintains.
		// Summed, not taken from the last receipt's CumulativeGasUsed: that counts from the start of the
		// ROUND that produced it, not the start of the block, so it reports only the final round's gas.
		for _, r := range receipts {
			res.GasUsed += r.GasUsed
		}
	}
	return res, nil
}
