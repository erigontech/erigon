package execmodule

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
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
	// The transaction id sequence too. This block's body is allocated its ids after its parent's, and the parent
	// allocated those in its own overlay and has not committed them: seeded from the DB alone, this overlay would hand
	// the successor the parent's ids and its body would overwrite the parent's transactions. Only ever moved forward —
	// a parent that has since committed leaves the DB's sequence ahead of it.
	parentSeq, err := src.ReadSequence(kv.EthTx)
	if err != nil {
		return err
	}
	ownSeq, err := dst.ReadSequence(kv.EthTx)
	if err != nil {
		return err
	}
	if parentSeq > ownSeq {
		return dst.ResetSequence(kv.EthTx, parentSeq)
	}
	return nil
}

// closePreExecutedLocked is the pre-exec CLOSE: block-end over the block in progress, and its output side read off the
// generation with ZERO body re-execution. It reads and executes only in pre-exec space — the block from its
// generation's overlay, state from the generation — so a fork choice between the block's last round and its close
// cannot take anything out from under it. Caller holds e.semaphore.
// traceCanonicalStatus records the bookkeeping the pre-exec close used to JUDGE the block by: the generation
// overlay's canonical row for this height against the header about to be sealed, and the stage progress the
// block's own rounds advanced. The close no longer acts on any of it — it does not unwind — but a
// disagreement here is the signature of that bookkeeping going stale underneath a block, which is exactly
// what let a re-stamped block seal a body its state transition never applied.
//
// Warn when it disagrees, so the next occurrence names itself in one line. Measured on live48 this was worth
// a day of archive forensics and still left two things unexplained: 221 re-stamps produced no unwind, and 4
// unwinds had no re-stamp — neither answerable because these values were computed and thrown away.
func (e *ExecModule) traceCanonicalStatus(ctx context.Context, tx kv.TemporalRwTx, header *types.Header, blockNumber uint64, when string) {
	hash := header.Hash()
	canonical, cerr := e.canonicalHash(ctx, tx, blockNumber)
	headersAt, herr := stages.GetStageProgress(tx, stages.Headers)
	sendersAt, serr := stages.GetStageProgress(tx, stages.Senders)
	execAt, eerr := stages.GetStageProgress(tx, stages.Execution)

	var readErr error
	for _, err := range []error{cerr, herr, serr, eerr} {
		if err != nil {
			readErr = err
			break
		}
	}
	agrees := readErr == nil && canonical == hash &&
		headersAt == blockNumber && sendersAt == blockNumber && execAt == blockNumber

	record := e.logger.Debug
	if !agrees {
		record = e.logger.Warn
	}
	record("[CANON-TRACE] pre-exec bookkeeping", "when", when, "block", blockNumber,
		"header", hash, "canonical", canonical, "canonicalMatches", canonical == hash,
		"headers", headersAt, "senders", sendersAt, "execution", execAt,
		"agrees", agrees, "err", readErr)
}

func (e *ExecModule) closePreExecutedLocked(ctx context.Context, blockHash common.Hash, blockNumber uint64) (ValidationResult, error) {
	ov := e.generationOverlay(blockHash, blockNumber)
	if ov == nil {
		return ValidationResult{ValidationStatus: ExecutionStatusMissingSegment}, nil
	}
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return ValidationResult{}, err
	}
	defer roTx.Rollback()
	ov.UpdateTxn(roTx)
	header, err := e.blockReader.Header(ctx, ov, blockHash, blockNumber)
	if err != nil {
		return ValidationResult{}, err
	}
	body, err := e.blockReader.BodyWithTransactions(ctx, ov, blockHash, blockNumber)
	if err != nil {
		return ValidationResult{}, err
	}
	if header == nil || body == nil {
		return ValidationResult{ValidationStatus: ExecutionStatusMissingSegment}, nil
	}
	flashUpdate := e.preExec.CheckUpdate(blockNumber, body.Transactions)
	if !flashUpdate.IsUpdate || flashUpdate.SD == nil {
		return ValidationResult{}, fmt.Errorf("pre-exec close: block %d %x is not the block in progress", blockNumber, blockHash)
	}

	// The close reuses the maintained accumulating SD: the block's whole body already executed into it across the
	// rounds, and its commitment trie has folded each round's diff. It unsets FlashblockAccumulating (so the block-end
	// task runs engine.Finalize) and resumes execution AT the block-end — past block-start and the executed prefix —
	// so no body tx re-executes and the final ComputeCommitment yields the SAME root a one-shot execution would.
	doms := flashUpdate.SD
	tx := doms.BlockOverlay()
	tx.UpdateTxn(roTx)
	doms.SetInMemHistoryReads(true)
	doms.SetFlashblockAccumulating(false)
	// The block's Min txNum resolves through the block OVERLAY: on a frontier block the predecessor's MaxTxNum entry
	// lives only there. minTxNum is the block-START system txNum, and the resume point is the LAST already-executed
	// txNum (exec3's skip loop drops every task <= it): minTxNum+PrefixLen, which resumes AT the block-end. One past
	// that skipped the block-end itself, so Finalize never ran and withdrawals were never credited.
	if minTxNum, merr := e.blockReader.TxnumReader().Min(ctx, tx, blockNumber); merr == nil {
		doms.SetPreExecStart(minTxNum + uint64(flashUpdate.PrefixLen))
		defer doms.ClearPreExecStart()
	}
	doms.SetStateCache(e.stateCache)
	// NO unwind here. "Is this block where the stages are?" is a CANONICAL question, and the close is not a
	// canonical step: it runs only on the block's own maintained SD (CheckUpdate above refuses anything else),
	// which this node's own rounds put where it is — every round re-establishes canonical[N], the head header,
	// the txNum index and the stage progress at the header's current hash, inside the generation's overlay.
	// The round path already draws this line: it unwinds only for a FRESH SD that is not extending a live
	// frontier (`!reuse && !frontierExtension`), and the close is `reuse` by construction, so the same rule
	// says never. The call was inherited when the pre-exec close was split out of validateChainLocked
	// (db733f4565), not chosen.
	// It also cannot be harmless: with anything stale in that bookkeeping the walk fails to recognise the
	// block, settles on its parent, and unwinds the block's own accumulated state away — the block-end then
	// runs on the parent's state and the block seals a body its state transition never applied.

	e.traceCanonicalStatus(ctx, tx, header, blockNumber, "close")

	status, _, validationError, criticalError := e.forkValidator.ExecuteInto(ctx, doms, tx, header, body.RawBody())
	if criticalError != nil {
		return ValidationResult{}, criticalError
	}
	result := ValidationResult{ValidationStatus: ExecutionStatusSuccess, LatestValidHash: header.Hash()}
	if status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil {
		result.ValidationStatus = ExecutionStatusBadBlock
		if validationError != nil {
			result.ValidationError = validationError.Error()
		}
		if e.stateCache != nil {
			e.stateCache.ClearWithHash(header.ParentHash)
		}
		return result, nil
	}
	if status == engine_types.AcceptedStatus {
		result.ValidationStatus = ExecutionStatusMissingSegment
		return result, nil
	}
	e.preExec.RecordTxHashes(body.Transactions)
	if dispatcher := e.pipelineExecutor.Dispatcher(); dispatcher != nil && len(body.Transactions) > 0 {
		txHashes := make([]common.Hash, len(body.Transactions))
		for i, t := range body.Transactions {
			txHashes[i] = t.Hash()
		}
		dispatcher.OnTransactionValidated(txHashes)
	}

	if cc := doms.GetCommitmentContext(); cc != nil {
		if root, rerr := cc.Trie().RootHash(); rerr == nil && len(root) > 0 {
			result.ComputedRoot = common.BytesToHash(root)
		}
	}
	// FRONTIER POSITION SAVE: the close reads the root off the folded trie but never persists the commitment "state"
	// marker into THIS SD. It is parked as the successor's read-through parent, and both the successor's
	// SeekCommitment and the FCU merge read the block's position from it — without the marker they fall through to the
	// predecessor's position. The trie is already folded, so this hits the save-only path.
	if blockTxNum, terr := e.blockReader.TxnumReader().Max(ctx, tx, blockNumber); terr == nil {
		if _, cerr := doms.ComputeCommitment(ctx, tx, true, blockNumber, blockTxNum, "frontier-close", nil); cerr != nil {
			return ValidationResult{}, fmt.Errorf("frontier close: save commitment state: %w", cerr)
		}
	}
	// The output side off the accumulated receipts, including the EMPTY block: DeriveSha(nil) = EmptyRootHash and an
	// empty bloom are exactly what a full re-execution computes. Each round used a per-round gas pool, so
	// CumulativeGasUsed is restamped as the running sum across the whole body.
	fbReceipts := doms.FlashblockReceipts()
	var cum uint64
	for _, r := range fbReceipts {
		cum += r.GasUsed
		r.CumulativeGasUsed = cum
	}
	result.FlashblockReceiptCount = len(fbReceipts)
	result.GasUsed = cum
	result.ReceiptHash = types.DeriveSha(fbReceipts)
	result.Bloom = types.CreateBloom(fbReceipts)
	return result, nil
}

// generationOverlay returns the overlay of the pre-exec generation holding (hash, number), or nil when the frontier
// holds no such block. Pre-exec reads a block's data only from here — never from the module context, which is the
// canonical flow's.
func (e *ExecModule) generationOverlay(hash common.Hash, number uint64) *membatchwithdb.MemoryMutation {
	if sd := e.preExec.Find(hash, number); sd != nil {
		return sd.BlockOverlay()
	}
	return nil
}

// preExecuteLocked runs a pre-exec round: it stores the round's block in pre-exec state and executes the block's NEW
// transactions into the ONE MAINTAINED SharedDomains of the block in progress (or a fresh one for a block's first
// round), carrying accumulated state forward across rounds and firing execobserver.OnTx once per new tx. It runs NO
// finished-block checks (root/gas belong to the close).
//
// The single-execution guarantee: because the SD is MAINTAINED, its committed txNum already reflects the prefix already
// executed, so exec3 resumes at the new txs and does not re-run the prefix. The close later sees a 100%-prefix match
// (CheckUpdate) and seals over this SD with ZERO re-execution — one execution total.
//
// Everything here is pre-exec space: the block goes into the round's own overlay, and nothing is read from or written to
// the module context, which is the canonical flow's. Caller holds e.semaphore.
func (e *ExecModule) preExecuteLocked(ctx context.Context, block *types.RawBlock) (ValidationResult, error) {
	header := block.Header
	blockNumber := header.Number.Uint64()
	txs := make([]types.Transaction, 0, len(block.Body.Transactions))
	for i, rlp := range block.Body.Transactions {
		txn, derr := types.DecodeTransaction(rlp)
		if derr != nil {
			return ValidationResult{}, fmt.Errorf("pre-exec block %d: decode tx %d: %w", blockNumber, i, derr)
		}
		txs = append(txs, txn)
	}
	body := &types.Body{Transactions: txs, Withdrawals: block.Body.Withdrawals}
	var err error

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
	// round is set when this round's SD is disposable. If its execution detached on cancel, goroutines are still
	// reading through roTx and the SD, and the round's owner releases both once they have finished.
	var round *execctx.SharedDomains
	defer func() {
		if round == nil || !round.Detached() {
			roTx.Rollback()
		}
	}()

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
		stagingBase = flashUpdate.SD
		if stagingBase != nil {
			// The round's commitment is parented like the rest of its state: its own context, restored from the
			// block's saved commitment state through its parent, folding only its own trie — so a round that is
			// dropped, or still calculating when it is cut, cannot touch the block's (live46: a round computing on
			// the block's borrowed context left the block sealing a root it could not back, and closes failing
			// "empty branch data read during unfold"). Only the merge carries the round's commitment into the block.
			//
			// The block's last deferred branch update is written into the block's own domain first, so the round
			// reads those branches through its parent like any other block data.
			if err = stagingBase.FlushPendingUpdates(ctx, roTx); err != nil {
				return ValidationResult{}, fmt.Errorf("pre-exec round num=%d: flush the block's pending commitment: %w", blockNumber, err)
			}
			if doms, err = execctx.NewSharedDomains(ctx, roTx, e.logger, execctx.WithParent(stagingBase)); err != nil {
				return ValidationResult{}, err
			}
			// The overlay is the block's: it carries block metadata written by the insert, identical whether the
			// round is kept or dropped.
			doms.BorrowBlockOverlay(stagingBase)
		} else {
			doms = flashUpdate.SD
		}
		// Resuming mid-block re-derives any needed prior state via in-memory history reads (GetAsOf). Enable
		// them on the BLOCK's SD as well as the round's: a staged round's reads fall THROUGH to the block's mem
		// batch, and a history read that lands there fails outright if the flag is only set on the child
		// ("GetAsOf called on TemporalMemBatch with inMemHistoryReads disabled").
		flashUpdate.SD.SetInMemHistoryReads(true)
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
		// lagging DB (the predecessor has opened but not canonicalised). With no live ancestor — the first block
		// after a restart — the parent is canonical, and its state is read from the DB beneath the SD: never from
		// the module context, which is the canonical flow's.
		frontierParent := e.preExec.ParentFor(blockNumber)
		parent := frontierParent
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

	// The round's block goes into pre-exec state itself — the generation's overlay for a staged round, the fresh
	// block's own otherwise — where execution, the close and the seal read it.
	if err = e.writePreExecBlock(tx, roTx, block); err != nil {
		if !reuse {
			doms.Close()
		}
		return ValidationResult{}, fmt.Errorf("pre-exec block %d: %w", blockNumber, err)
	}
	doms.SetStateCache(e.stateCache)

	// A round's results are its caller's to keep or drop, and a dropped round is never read. So when its deadline
	// cancels it, execution gives the executor back at once rather than waiting for its workers to shut down — the
	// seal is waiting behind it (live40). The shutdown finishes in the background: the frontier generations it reads
	// through stay pinned, and this round's SD and tx are released only once it is done.
	//
	// Only an SD nobody else holds qualifies — a staged child, or a fresh block — and only one that reads purely
	// through the frontier: the canonical context a first block chains to is torn down by the fork choice, which a
	// pin does not hold.
	if stagingBase != nil || (!reuse && frontierExtension) {
		round = doms
		round.SetDetachOnCancel(func(finish func()) {
			release := e.preExec.PinAll()
			go func() {
				defer release()
				finish()
				round.Close()
				roTx.Rollback()
			}()
		})
	}

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
	detached := round != nil && round.Detached()
	if round != nil && !detached {
		// It ran to the end, so nothing will detach from it now; an SD that goes on to become a generation must
		// not carry the hook.
		round.SetDetachOnCancel(nil)
	}
	registered := false
	defer func() {
		if !registered && !detached {
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
		// The caller may already have stopped waiting and requeued this round's transactions. Commit only
		// if this round wins the claim; otherwise the deferred close drops the staged state untouched.
		if claim := roundCommitClaim(ctx); claim != nil && !claim() {
			return ValidationResult{}, fmt.Errorf("%w: num=%d lost the commit claim", ErrRoundAbandoned, blockNumber)
		}
		if stagingBase != nil {
			if merr := stagingBase.Merge(ctx, stagingBase.TxNum(), doms, doms.TxNum(), true); merr != nil {
				return ValidationResult{}, fmt.Errorf("commit pre-exec round num=%d: %w", blockNumber, merr)
			}
			// The merge carries the round's commitment into the block: its branch writes and saved trie state are
			// now the block's, and the block's trie is restored from that state — otherwise it still stands where
			// it was before the round, and the root the block reports does not advance with its body.
			if _, _, serr := stagingBase.GetCommitmentContext().SeekCommitment(ctx, roTx); serr != nil {
				return ValidationResult{}, fmt.Errorf("commit pre-exec round num=%d: restore the block's commitment: %w", blockNumber, serr)
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
