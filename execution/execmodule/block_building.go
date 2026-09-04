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
	"fmt"
	"reflect"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// BlockAssembler is the DAG-driven L2 block-production hook. On such an L2, AssembleBlock defers to
// this instead of building from-scratch from the txpool: the implementation (the cocoon rollup driver)
// inserts a block-end MARKER carrying the CL's proposer attributes into the ordering layer and WAITS
// (bounded, inside the CL assembly delay) for it to appear in the committed stream — the consensus point
// at which every node agrees the block ends — then seals the already-pre-executed body (zero re-execution)
// and returns it. The interface lives in erigon and is implemented in cocoon (dependency inversion:
// erigon cannot import cocoon). See [[dag_start_end_system_tx]].
type BlockAssembler interface {
	// NewPayloadAttrs delivers the NEXT block's payload attributes (the CL builds them per slot from the head
	// beacon state, on EVERY client, not just the proposer) BEFORE AssembleBlock. It is what TRIGGERS the next
	// block's start of execution: the assembler opens that block on params.ParentHash and begins executing it
	// (header + block-start system tx + coinbase) under these attrs, instead of waiting for the build trigger to
	// hand them over. Non-blocking. params carries the parent (head) and the attrs.
	NewPayloadAttrs(ctx context.Context, params *builder.Parameters) error
	// AssembleBlock inserts the block-end marker carrying params' attrs into the ordering layer and returns
	// IMMEDIATELY (non-blocking). Called from AssembleBlock, which must return a PayloadID promptly — the CL's
	// ForkChoiceUpdate blocks on it, so it cannot wait here for the DAG round trip.
	AssembleBlock(ctx context.Context, params *builder.Parameters) error
	// GetAssembledBlock blocks (bounded, the CL assembly delay) until the marker has committed and the block's
	// body is fully pre-executed + recorded as the extending-fork flashblock (so a follow-up assemblePreconfirmed
	// seals it with zero re-execution). Called from GetAssembledBlock, the GetPayload path — and CRUCIALLY the
	// caller must NOT hold the exec semaphore while awaiting, or the commits-handler PreExecute (which needs the
	// semaphore to record the body) deadlocks against it. Returns an error (non-fatal) if the marker does not
	// commit in time; the CL skips the slot and retries, and the retry adopts the block if it sealed meanwhile.
	GetAssembledBlock(ctx context.Context, params *builder.Parameters) error
}

// SetBlockAssembler installs the DAG-L2 boundary assembler (see BlockAssembler). Called at wiring
// time by the cocoon node for a chain whose builder is the incremental/DAG model.
func (e *ExecModule) SetBlockAssembler(ba BlockAssembler) {
	e.blockAssembler = ba
}

// NewPayloadAttrs forwards the CL-delivered next-block payload attributes to the DAG boundary assembler so it
// can open the block and start executing it ahead of AssembleBlock. Thin passthrough — the assembler (cocoon
// driver) records the attrs and drives its own pre-execution; the exec semaphore is NOT held here (the record
// is non-blocking and the drain that follows acquires the semaphore itself). No-op when no assembler is set.
func (e *ExecModule) NewPayloadAttrs(ctx context.Context, params *builder.Parameters) error {
	if e.blockAssembler == nil {
		return nil
	}
	return e.blockAssembler.NewPayloadAttrs(ctx, params)
}

func (e *ExecModule) checkWithdrawalsPresence(time uint64, withdrawals []*types.Withdrawal) error {
	if !e.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before shanghai"}
	}
	if e.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
}

func (e *ExecModule) evictOldBuilders() {
	ids := common.SortedKeys(e.builders)

	// remove old builders so that at most MaxBuilders - 1 remain
	for i := 0; i <= len(e.builders)-engine_helpers.MaxBuilders; i++ {
		delete(e.builders, ids[i])
	}
}

func (e *ExecModule) AssembleBlock(ctx context.Context, params *builder.Parameters) (AssembleBlockResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return AssembleBlockResult{Busy: true}, nil
	}
	defer e.semaphore.Release(1)

	if err := e.checkWithdrawalsPresence(params.Timestamp, params.Withdrawals); err != nil {
		return AssembleBlockResult{}, err
	}

	// First check if we're already building a block with the requested parameters
	if e.lastParameters != nil {
		params.PayloadId = e.lastParameters.PayloadId
		if reflect.DeepEqual(e.lastParameters, params) {
			e.logger.Info("[ForkChoiceUpdated] duplicate build request")
			return AssembleBlockResult{PayloadID: e.lastParameters.PayloadId}, nil
		}
	}

	// Initiate payload building
	e.evictOldBuilders()

	e.nextPayloadId++
	params.PayloadId = e.nextPayloadId
	e.lastParameters = params

	// DAG-L2 BOUNDARY ASSEMBLE: on a DAG-driven L2 the CL's FCU-with-attributes does NOT build from-scratch
	// from the txpool. It inserts a block-end MARKER carrying these attrs into the ordering layer and WAITS
	// (bounded, inside this assembly delay — the "one DAG round trip" the boundary costs) for the marker to
	// appear in the committed stream, the point every node agrees the block ends; then seals the already-
	// pre-executed body (zero re-execution). The marker carrying params gives attribute agreement for free
	// (pre-exec ran under these same attrs, sourced from this FCU). See [[dag_start_end_system_tx]].
	if e.blockAssembler != nil {
		// RE-ANCHOR (exec-internal, under this semaphore hold): if the CL is now building on a DIFFERENT parent
		// than the in-progress flashblock was opened on, Caplin accepted a new head — drop the stale fork and
		// re-anchor the frontier to that head so the boundary opens FRESH on it. This replaced the driver's
		// AnchorFrontier callback, which was semaphore-free only because it rode inside this hold (an anti-pattern
		// on the public interface); the trigger + action now live where the semaphore is genuinely held.
		e.reanchorFrontierForBlockLocked(ctx, params)
		// Insert the block-end marker NOW (non-blocking) so it starts committing during the CL assembly
		// delay, and record the params so GetAssembledBlock can GetAssembledBlock + seal. MUST NOT wait here —
		// the CL's ForkChoiceUpdate blocks on AssembleBlock returning a PayloadID.
		if err := e.blockAssembler.AssembleBlock(ctx, params); err != nil {
			return AssembleBlockResult{}, err
		}
		e.pendingBlockMu.Lock()
		e.pendingBlock[e.nextPayloadId] = params
		e.pendingBlockMu.Unlock()
		e.logger.Info("[ForkChoiceUpdated] DAG boundary begun", "payload", e.nextPayloadId, "parent", params.ParentHash)
		return AssembleBlockResult{PayloadID: e.nextPayloadId}, nil
	}

	// PRECONFIRM VARIANT: if a preconfirm producer (op-stack sequencer) has accumulated a matching in-progress
	// flashblock in the fork validator, SEAL it synchronously and hand it back directly — no from-scratch build,
	// no body re-execution. Falls through to the builder when none.
	if br, ok, err := e.assemblePreconfirmed(ctx, params); err != nil {
		return AssembleBlockResult{}, err
	} else if ok {
		e.preconfirmedBlocks[e.nextPayloadId] = br
		e.logger.Info("[ForkChoiceUpdated] preconfirm assemble", "payload", e.nextPayloadId, "num", br.Block.NumberU64(), "hash", br.Block.Hash(), "txs", len(br.Block.Transactions()))
		return AssembleBlockResult{PayloadID: e.nextPayloadId}, nil
	}

	e.builders[e.nextPayloadId] = builder.NewBlockBuilder(e.builderFunc, params, e.config.SecondsPerSlot()/4)
	e.logger.Info("[ForkChoiceUpdated] BlockBuilder added", "payload", e.nextPayloadId)

	return AssembleBlockResult{PayloadID: e.nextPayloadId}, nil
}

// assemblePreconfirmed is the base-erigon PRECONFIRM assemble variant. When the fork validator holds a
// preconfirmed in-progress flashblock (the extending fork a preconfirm producer — op-stack sequencer or
// the DAG driver — accumulated via PreExecute) whose parent + attributes match the requested build, it
// SEALS that flashblock (the close runs block-end over the maintained SharedDomains → real
// Root/GasUsed/ReceiptHash/Bloom, ZERO body re-execution) and returns the finished block, then re-keys
// the extending fork to the sealed header so the follow-up FCU canonicalises it. Returns (block, true)
// on the preconfirm path; (nil, false) to fall back to the normal from-scratch builder (no matching
// preconfirmed block, or its attributes disagree with the FCU params). Caller MUST hold e.semaphore.
func (e *ExecModule) assemblePreconfirmed(ctx context.Context, params *builder.Parameters) (*types.BlockWithReceipts, bool, error) {
	oldHash, number, sd := e.preExec.Active()
	if sd == nil || oldHash == (common.Hash{}) {
		e.logger.Debug("assemblePreconfirmed: no block open — from-scratch builder", "reqParent", params.ParentHash)
		return nil, false, nil // no preconfirmed flashblock → from-scratch builder
	}
	if e.currentContext == nil || e.currentContext.BlockOverlay() == nil {
		e.logger.Debug("assemblePreconfirmed: no current context — from-scratch builder", "reqParent", params.ParentHash)
		return nil, false, nil
	}

	// Read the in-progress header+body the preconfirm rounds accumulated (from the overlay).
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, false, err
	}
	ov := e.currentContext.BlockOverlay()
	ov.UpdateTxn(roTx)
	inHdr, herr := e.blockReader.Header(ctx, ov, oldHash, number)
	var body *types.Body
	if herr == nil && inHdr != nil {
		body, herr = e.blockReader.BodyWithTransactions(ctx, ov, oldHash, number)
	}
	roTx.Rollback()
	if herr != nil {
		return nil, false, herr
	}
	if inHdr == nil || body == nil {
		e.logger.Debug("assemblePreconfirmed: in-progress header/body missing", "num", number, "oldHash", oldHash,
			"hdrNil", inHdr == nil, "bodyNil", body == nil)
		return nil, false, nil
	}
	// The preconfirmed block must be for THIS build (same parent) and must have accumulated under the same
	// proposer attributes the FCU supplies — otherwise the sealed header (in-progress header + output)
	// would be inconsistent with the requested block. Disagreement ⇒ fall back to the from-scratch builder.
	if inHdr.ParentHash != params.ParentHash || !preconfirmAttrsMatch(inHdr, params) {
		// Reaching here means the reconcile did not re-open the block under these attributes, so seal it
		// is not: the header would claim attributes the body was not executed under.
		e.logger.Warn("[execmodule] in-progress block does not match the requested attributes — from-scratch builder",
			"num", number, "parentOK", inHdr.ParentHash == params.ParentHash,
			"tsOK", inHdr.Time == params.Timestamp, "hdrTs", inHdr.Time, "reqTs", params.Timestamp,
			"randaoOK", inHdr.MixDigest == params.PrevRandao,
			"coinbaseOK", inHdr.Coinbase == params.SuggestedFeeRecipient)
		return nil, false, nil
	}

	// CLOSE (seal): block-end over the maintained SD → the output side, zero body re-execution.
	sealStart := time.Now()
	res, err := e.validateChainLocked(ctx, oldHash, number)
	if err != nil {
		return nil, false, err
	}
	if res.ValidationStatus != ExecutionStatusSuccess {
		return nil, false, fmt.Errorf("assemblePreconfirmed: close status=%v err=%q", res.ValidationStatus, res.ValidationError)
	}
	if nTx := len(body.Transactions); nTx > 0 {
		el := time.Since(sealStart)
		e.execCost.record(el, res.GasUsed, nTx)
		uqTime, uqGas := e.execCost.upperQuartile()
		e.logger.Info("[TPS-seal] close", "block", number, "txs", nTx, "ms", el.Milliseconds(),
			"txPerSec", int(float64(nTx)/el.Seconds()),
			"usPerTx", el.Microseconds()/int64(nTx), "gasPerTx", res.GasUsed/uint64(nTx),
			"uqUsPerTx", uqTime.Microseconds(), "uqGasPerTx", uqGas, "gasUsed", res.GasUsed)
	}

	// Sealed header = the accumulated in-progress header + the computed output side.
	sealed := types.CopyHeader(inHdr)
	sealed.Root = res.ComputedRoot
	sealed.GasUsed = res.GasUsed
	sealed.ReceiptHash = res.ReceiptHash
	sealed.Bloom = res.Bloom

	// Receipts the close accumulated + restamped on the sealed SD (zero re-exec).
	var receipts types.Receipts
	if _, _, sealedSD := e.preExec.Active(); sealedSD != nil {
		receipts = sealedSD.FlashblockReceipts()
	}

	e.auditSealedBody(ctx, number, body.Transactions)

	block := types.NewBlockForAsembling(sealed, body.Transactions, nil, receipts, params.Withdrawals)

	// newPayload ingest: re-key the extending fork to the sealed header so a subsequent FCU canonicalises it.
	if err := e.ingestSealedFlashblockLocked(ctx, sealed); err != nil {
		return nil, false, err
	}
	e.logger.Info("[execmodule] preconfirm assemble: sealed preconfirmed flashblock",
		"number", number, "hash", sealed.Hash(), "root", sealed.Root, "gasUsed", sealed.GasUsed, "txs", len(body.Transactions))
	return &types.BlockWithReceipts{Block: block, Receipts: receipts}, true, nil
}

// SealBlock is the marker-driven CLOSE (the UNIVERSAL block-production step). The boundary assembler
// (the cocoon driver's commits handler) calls it when the block-end MARKER commits in consensus — on EVERY
// node, decoupled from the CL role. It seals the pre-executed in-progress flashblock via assemblePreconfirmed
// (zero re-execution) and stores it keyed by the block's PARENT hash, so the CL's GetAssembledBlock (proposer)
// — riding behind — just RETRIEVES the already-sealed block instead of re-sealing it. Returns the sealed
// block (or nil if there is no matching preconfirmed flashblock for these params). Acquires the semaphore.
func (e *ExecModule) SealBlock(ctx context.Context, params *builder.Parameters, forceEmpty bool) (*types.BlockWithReceipts, error) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer e.semaphore.Release(1)

	// RECONCILE (was sprawled driver-side, reading FlashblockState): ensure a pre-executed in-progress block for
	// params.ParentHash exists whose stamped proposer attrs match params, so assemblePreconfirmed can seal it.
	// forceEmpty (getPayload cut-off) drops the in-flight body and seals an EMPTY block instead.
	if err := e.reconcileForAssembleLocked(ctx, params, forceEmpty); err != nil {
		return nil, err
	}

	// ASSEMBLE (seal N): block-end over the maintained SD → the output side, ZERO body re-execution.
	br, ok, err := e.assemblePreconfirmed(ctx, params)
	if err != nil || !ok || br == nil {
		return nil, err
	}
	e.pendingBlockMu.Lock()
	e.preconfirmedByParent[params.ParentHash] = br
	e.pendingBlockMu.Unlock()
	sealedHdr := br.Block.Header()
	// Exec OWNS the frontier: the block just sealed is the head the successor flashblock chains onto.
	e.frontierHeader.Store(sealedHdr)

	// CLOSE the in-progress block: its body is now IN the sealed block, so there is nothing open any more.
	// The successor opens when its own attributes arrive. Leaving this set would make the next reconcile
	// believe the sealed block is still in progress and carry ITS body into the successor.
	e.flash.mu.Lock()
	e.flash.resetLocked(0)
	e.flash.mu.Unlock()

	// N+1 is NOT opened here. Every attribute a block opens under — timestamp, prevRandao, coinbase,
	// parentBeaconBlockRoot, withdrawals — is EXECUTION-affecting: the timestamp gates fork activation and
	// keys the EIP-4788 ring buffer, block-start writes the beacon root into state, PREVRANDAO and COINBASE
	// are opcodes, and withdrawals credit accounts. Opening under guesses (N's attrs, N's time + 1) therefore
	// produces a block whose STATE is wrong, not merely mislabelled — and it cannot be patched afterwards,
	// only re-executed. So the successor opens when its real attributes arrive, in NewPayloadAttrs.
	//
	// This used to be done here, under the same semaphore hold, so that no FCU could tear down N's
	// SharedDomains in the gap between the close and the open. That race is gone: pre-exec state lives in
	// its own space now and the FCU cannot reach it ([[preexec_validation_space_separation]]).
	e.logger.Info("[execmodule] block sealed", "number", sealedHdr.Number.Uint64(), "hash", sealedHdr.Hash(),
		"parent", params.ParentHash)
	return br, nil
}

// reconcileForAssembleLocked ensures there is a pre-executed in-progress block for params whose stamped proposer
// attrs match params, so assemblePreconfirmed can seal it. It subsumes the driver's former FlashblockState-driven
// empty/stale-attrs handling: nothing pre-executed yet ⇒ open empty under params; an empty block eager-opened
// under provisional attrs at the previous seal whose attrs diverge from THESE CL attrs ⇒ abandon + reopen fresh
// (so block-start re-runs under the CL attrs and the sealed root matches a follower's re-execution). Caller MUST
// hold e.semaphore.
func (e *ExecModule) reconcileForAssembleLocked(ctx context.Context, params *builder.Parameters, forceEmpty bool) error {
	// The fork validator — not e.flash — is the authority on whether a block is actually open: it holds the
	// in-progress SharedDomains that assemblePreconfirmed seals from. The two can disagree, because a clear on
	// the fork validator (FCU cleanup, InsertBlocks) does NOT touch e.flash. Trusting e.flash alone then leaves
	// this reconcile believing a block is still open while its SD is gone, so nothing re-opens, the seal bails
	// "no extending fork", and — since that bail returns before SealBlock's open-N+1 step — every later assemble
	// bails the same way. Treat "no SD" as "not open" and re-open fresh (resetting the stale body, which
	// preExecuteFlashblockLocked would otherwise keep: it only auto-resets on a NEW block number).
	_, _, extSD := e.preExec.Active()

	e.flash.mu.Lock()
	if extSD == nil {
		e.flash.resetLocked(0)
	}
	recorded := e.flash.valid
	built := e.flash.built
	e.flash.mu.Unlock()

	// openUnderParams opens the block under params, re-executing body if given. Passing the accumulated body
	// back in is what makes a re-open under corrected attributes non-destructive: the txs are re-executed
	// under the right values instead of being dropped.
	// restore=true when a body is passed back in: those transactions were already accepted into this block,
	// so the re-open replays them verbatim rather than re-adjudicating them (see reopenFlashblockLocked).
	openUnderParams := func(body [][]byte) error {
		parent := e.frontierOrHead(ctx)
		if parent == nil {
			return fmt.Errorf("reconcileForAssembleLocked: no parent header to open on")
		}
		in := e.flashInputsForChild(parent, params, params.Timestamp)
		_, _, vr, err := e.accumulateFlashblockLocked(ctx, in, body, len(body) > 0)
		if err != nil {
			return err
		}
		if vr.ValidationStatus != ExecutionStatusSuccess {
			return fmt.Errorf("reconcileForAssembleLocked: open status=%v txs=%d", vr.ValidationStatus, len(body))
		}
		return nil
	}

	// forceEmpty (getPayload cut-off path): the in-progress block could not be produced in time, so DROP whatever
	// is accumulated and seal an EMPTY block for params instead — abandon the in-flight fork (if any) and re-open
	// it empty. The dropped txs stay in the pool and re-feed later.
	if forceEmpty {
		if recorded {
			e.abandonExtendingForkLocked()
		}
		return openUnderParams(nil)
	}

	switch {
	case !recorded:
		return openUnderParams(nil)
	case !builtAttrsMatchParams(built, params):
		// The block is open under attributes that are no longer the ones being asked for — the CL missed a
		// slot, so the next slot's attributes describe this same block. They cannot be patched onto the
		// header: every one of them is execution-affecting, so the accumulated execution itself is wrong.
		// Re-open under the CL's attributes and re-execute the body, which costs one block-start plus the
		// txs so far. (This used to run only for an EMPTY block; a non-empty one was left alone, and then
		// the seal rejected it every slot thereafter — the block could never be produced.)
		body := e.flashBodyCopy()
		var reqPbbr common.Hash
		if params.ParentBeaconBlockRoot != nil {
			reqPbbr = *params.ParentBeaconBlockRoot
		}
		e.logger.Info("[execmodule] attrs changed for the open block — re-opening", "txs", len(body),
			"builtTs", built.Timestamp, "reqTs", params.Timestamp,
			"builtRandao", built.PrevRandao, "reqRandao", params.PrevRandao,
			"builtCoinbase", built.FeeRecipient, "reqCoinbase", params.SuggestedFeeRecipient,
			"builtPbbr", built.ParentBeaconBlockRoot, "reqPbbr", reqPbbr)
		e.abandonExtendingForkLocked()
		return openUnderParams(body)
	default:
		return nil
	}
}

// flashBodyCopy returns a copy of the in-progress block's accumulated tx RLPs.
func (e *ExecModule) flashBodyCopy() [][]byte {
	e.flash.mu.Lock()
	defer e.flash.mu.Unlock()
	return append([][]byte(nil), e.flash.body...)
}

// flashInputsForChild builds the FlashblockInputs for the child of parent, taking proposer attrs from params and
// the supplied timestamp; Number/GasLimit/BaseFee derive from the parent header (base fee via the consensus
// EIP-1559 rule so a follower's re-execution computes the identical header).
func (e *ExecModule) flashInputsForChild(parent *types.Header, params *builder.Parameters, timestamp uint64) FlashblockInputs {
	var pbbr common.Hash
	if params.ParentBeaconBlockRoot != nil {
		pbbr = *params.ParentBeaconBlockRoot
	}
	return FlashblockInputs{
		Parent:                parent.Hash(),
		Number:                parent.Number.Uint64() + 1,
		GasLimit:              parent.GasLimit,
		BaseFee:               *misc.CalcBaseFee(e.config, parent),
		Timestamp:             timestamp,
		PrevRandao:            params.PrevRandao,
		FeeRecipient:          params.SuggestedFeeRecipient,
		ParentBeaconBlockRoot: pbbr,
		Withdrawals:           params.Withdrawals,
	}
}

// frontierOrHead returns the exec-owned run-ahead frontier, or the canonical current header when nothing has been
// sealed yet (bootstrap). Caller MUST hold e.semaphore (CurrentHeader itself does not take it).
func (e *ExecModule) frontierOrHead(ctx context.Context) *types.Header {
	if fr := e.frontierHeader.Load(); fr != nil {
		return fr
	}
	head, err := e.CurrentHeader(ctx)
	if err != nil {
		return nil
	}
	return head
}

// ExecCostUpperQuartile returns the running window's 75th-percentile per-tx execution TIME and per-tx GAS. The
// driver reads it to size the next batch it feeds into exec — time bound ≈ assembleTimeout/uqTime, with per-tx gas
// as the execution-measured cross-check for the txpool gas estimate. Returns (0,0) until the window has samples.
func (e *ExecModule) ExecCostUpperQuartile() (time.Duration, uint64) {
	if e.execCost == nil {
		return 0, 0
	}
	return e.execCost.upperQuartile()
}

// builtAttrsMatchParams reports whether an in-progress block's stamped inputs carry the same proposer attrs as
// params (Withdrawals are checked authoritatively by assemblePreconfirmed's WithdrawalsHash comparison).
func builtAttrsMatchParams(built FlashblockInputs, params *builder.Parameters) bool {
	if built.Timestamp != params.Timestamp || built.PrevRandao != params.PrevRandao || built.FeeRecipient != params.SuggestedFeeRecipient {
		return false
	}
	var pbbr common.Hash
	if params.ParentBeaconBlockRoot != nil {
		pbbr = *params.ParentBeaconBlockRoot
	}
	return built.ParentBeaconBlockRoot == pbbr
}

// FrontierHeader returns the exec-owned run-ahead frontier head — the last sealed block a newly-opening
// flashblock chains onto — or nil before the first seal. LOCK-FREE (atomic): safe to call while holding the
// exec semaphore, which AssembleBlock's anchor path does. This is the ownership move that removes the driver's
// ibMu-guarded d.frontier cache and the ibMu↔exec-semaphore deadlock.
func (e *ExecModule) FrontierHeader() *types.Header { return e.frontierHeader.Load() }

// reanchorFrontierForBlockLocked re-anchors the run-ahead frontier when the CL's FCU (AssembleBlock's params)
// builds on a DIFFERENT parent than the in-progress flashblock was opened on — Caplin accepted a new head
// (catch-up/reorg). It drops the stale in-progress fork and re-anchors the frontier to that accepted head, so the
// boundary opens FRESH on it. Abandoning is what makes a SAME-HEIGHT reorg safe (PreExecuteFlashblock only auto-
// resets its body on a NUMBER change). No in-progress block, or the CL still on the same parent ⇒ no-op — this
// must NOT fire during normal run-ahead (frontier legitimately ahead of the accepted head). Caller (AssembleBlock)
// holds e.semaphore; this is the exec-internal replacement for the driver's former AnchorFrontier callback.
func (e *ExecModule) reanchorFrontierForBlockLocked(ctx context.Context, params *builder.Parameters) {
	e.flash.mu.Lock()
	valid := e.flash.valid
	inProgressParent := e.flash.built.Parent
	e.flash.mu.Unlock()
	if !valid || inProgressParent == params.ParentHash {
		return
	}
	if fr := e.frontierHeader.Load(); fr == nil || fr.Hash() != params.ParentHash {
		if hdr := e.headerByHashLocked(ctx, params.ParentHash); hdr != nil {
			e.frontierHeader.Store(hdr)
		}
	}
	e.abandonExtendingForkLocked()
}

// headerByHashLocked resolves a header by hash from the DB (canonical/sealed blocks). Returns nil when it can't
// be resolved (not yet persisted). Caller holds e.semaphore.
func (e *ExecModule) headerByHashLocked(ctx context.Context, hash common.Hash) *types.Header {
	tx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil
	}
	defer tx.Rollback()
	h, err := e.blockReader.HeaderByHash(ctx, tx, hash)
	if err != nil {
		return nil
	}
	return h
}

// abandonExtendingForkLocked discards the active pre-executed in-progress block so the next PreExecute re-opens
// it from a fresh SD (re-running block-start under corrected attrs). Caller MUST hold e.semaphore. It is exec-
// INTERNAL — the reconcile (SealBlock) and the boundary re-anchor (AssembleBlock) are its only callers, both
// already under the semaphore.
func (e *ExecModule) abandonExtendingForkLocked() {
	e.preExec.Abandon()
	// The driver re-opens the SAME block number FRESH after an abandon, so drop the maintained flashblock body
	// too — otherwise PreExecuteFlashblock (which only auto-resets on a NEW number) would keep the stale body.
	e.flash.mu.Lock()
	e.flash.resetLocked(0)
	e.flash.mu.Unlock()
}

// preconfirmAttrsMatch reports whether the accumulated in-progress header's proposer attributes equal the
// FCU params, so the sealed header (in-progress header + computed output) is consistent with the block
// the CL asked to assemble.
func preconfirmAttrsMatch(h *types.Header, params *builder.Parameters) bool {
	if h.Time != params.Timestamp || h.MixDigest != params.PrevRandao || h.Coinbase != params.SuggestedFeeRecipient {
		return false
	}
	if (h.ParentBeaconBlockRoot == nil) != (params.ParentBeaconBlockRoot == nil) {
		return false
	}
	if h.ParentBeaconBlockRoot != nil && params.ParentBeaconBlockRoot != nil && *h.ParentBeaconBlockRoot != *params.ParentBeaconBlockRoot {
		return false
	}
	wh := types.DeriveSha(types.Withdrawals(params.Withdrawals))
	if h.WithdrawalsHash == nil || *h.WithdrawalsHash != wh {
		return false
	}
	return true
}

// blockValue computes the expected value received by the fee recipient in wei.
func blockValue(br *types.BlockWithReceipts, baseFee *uint256.Int) *uint256.Int {
	blockValue := uint256.NewInt(0)
	txs := br.Block.Transactions()
	var gas, txValue uint256.Int
	for i := range txs {
		// Receipts can lag the tx list on some assembled-block paths (e.g. an L2 block surfaced
		// before its receipts are attached); indexing past them panicked and crashed block
		// production. blockValue is a reported fee-recipient value, not consensus state, so a
		// partial/zero value is safe — stop rather than fault the chain.
		if i >= len(br.Receipts) {
			break
		}
		gas.SetUint64(br.Receipts[i].GasUsed)

		effectiveTip := txs[i].GetEffectiveGasTip(baseFee)

		txValue.Mul(&gas, &effectiveTip)
		blockValue.Add(blockValue, &txValue)
	}
	return blockValue
}

func (e *ExecModule) GetAssembledBlock(ctx context.Context, payloadID uint64) (AssembledBlockResult, error) {
	// DAG BOUNDARY: complete a boundary assemble whose marker was inserted (non-blocking) by AssembleBlock.
	// GetAssembledBlock FIRST, WITHOUT the semaphore — the commits-handler PreExecute needs the semaphore to
	// record the body, so holding it here would deadlock the wait. Then acquire the semaphore and seal via
	// assemblePreconfirmed (zero re-execution; the marker carried these attrs so its checks match).
	e.pendingBlockMu.Lock()
	params, isBlock := e.pendingBlock[payloadID]
	e.pendingBlockMu.Unlock()
	if isBlock {
		// GetPayload just RETURNS the block the marker handler already sealed (SealBlock) — it does NOT
		// seal. GetAssembledBlock drives the DAG so the marker commits (and the commits handler seals+stores);
		// then the block is retrieved by its parent hash. The CL is riding behind the marker-driven close.
		if err := e.blockAssembler.GetAssembledBlock(ctx, params); err != nil {
			// Marker not committed in time (e.g. a quiet DAG at boot). Drop it and return no block — the CL
			// skips this slot and retries on the NEXT slot with a fresh boundary; if the marker committed
			// meanwhile, that retry's GetAssembledBlock adopts the already-sealed block. Not fatal.
			e.pendingBlockMu.Lock()
			delete(e.pendingBlock, payloadID)
			e.pendingBlockMu.Unlock()
			e.logger.Info("[GetPayload] DAG boundary not ready, skipping slot", "payload", payloadID, "err", err)
			return AssembledBlockResult{}, nil
		}
		e.pendingBlockMu.Lock()
		br, ready := e.preconfirmedByParent[params.ParentHash]
		if ready {
			delete(e.preconfirmedByParent, params.ParentHash)
			delete(e.pendingBlock, payloadID)
		}
		e.pendingBlockMu.Unlock()
		if !ready {
			// GetAssembledBlock returned but the commits handler has not stored the sealed block yet (a race at
			// the marker) — report not-ready so the CL retries GetPayload rather than skipping the slot.
			e.logger.Warn("[GetPayload] DAG boundary awaited but sealed block not yet stored", "payload", payloadID)
			return AssembledBlockResult{}, nil
		}
		e.logger.Info("[GetPayload] DAG boundary retrieved (marker-sealed)", "payload", payloadID, "num", br.Block.NumberU64(), "hash", br.Block.Hash(), "txs", len(br.Block.Transactions()))
		return AssembledBlockResult{Block: br, BlockValue: blockValue(br, br.Block.Header().BaseFee)}, nil
	}

	if !e.semaphore.TryAcquire(1) {
		return AssembledBlockResult{Busy: true}, nil
	}
	defer e.semaphore.Release(1)

	// PRECONFIRM VARIANT: a synchronously-sealed preconfirmed block is returned directly (no async builder).
	if br, ok := e.preconfirmedBlocks[payloadID]; ok {
		delete(e.preconfirmedBlocks, payloadID)
		return AssembledBlockResult{Block: br, BlockValue: blockValue(br, br.Block.Header().BaseFee)}, nil
	}

	bldr, ok := e.builders[payloadID]
	if !ok {
		return AssembledBlockResult{}, nil
	}
	blockWithReceipts, err := bldr.Stop()
	if err != nil {
		e.logger.Error("Failed to build PoS block", "err", err)
		return AssembledBlockResult{}, err
	}
	if blockWithReceipts == nil {
		return AssembledBlockResult{}, nil
	}

	header := blockWithReceipts.Block.Header()
	baseFee := header.BaseFee
	value := blockValue(blockWithReceipts, baseFee)

	return AssembledBlockResult{
		Block:      blockWithReceipts,
		BlockValue: value,
	}, nil
}
