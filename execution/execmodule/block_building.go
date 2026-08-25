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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// BoundaryAssembler is the DAG-driven L2 block-production hook. On such an L2, AssembleBlock defers to
// this instead of building from-scratch from the txpool: the implementation (the cocoon rollup driver)
// inserts a block-end MARKER carrying the CL's proposer attributes into the ordering layer and WAITS
// (bounded, inside the CL assembly delay) for it to appear in the committed stream — the consensus point
// at which every node agrees the block ends — then seals the already-pre-executed body (zero re-execution)
// and returns it. The interface lives in erigon and is implemented in cocoon (dependency inversion:
// erigon cannot import cocoon). See [[dag_start_end_system_tx]].
type BoundaryAssembler interface {
	// BeginBoundary inserts the block-end marker carrying params' attrs into the ordering layer and returns
	// IMMEDIATELY (non-blocking). Called from AssembleBlock, which must return a PayloadID promptly — the CL's
	// ForkChoiceUpdate blocks on it, so it cannot wait here for the DAG round trip.
	BeginBoundary(ctx context.Context, params *builder.Parameters) error
	// AwaitBoundary blocks (bounded, the CL assembly delay) until the marker has committed and the block's
	// body is fully pre-executed + recorded as the extending-fork flashblock (so a follow-up assemblePreconfirmed
	// seals it with zero re-execution). Called from GetAssembledBlock, the GetPayload path — and CRUCIALLY the
	// caller must NOT hold the exec semaphore while awaiting, or the commits-handler PreExecute (which needs the
	// semaphore to record the body) deadlocks against it. Returns an error if the marker does not commit in time.
	AwaitBoundary(ctx context.Context, params *builder.Parameters) error
}

// SetBoundaryAssembler installs the DAG-L2 boundary assembler (see BoundaryAssembler). Called at wiring
// time by the cocoon node for a chain whose builder is the incremental/DAG model.
func (e *ExecModule) SetBoundaryAssembler(ba BoundaryAssembler) {
	e.boundaryAssembler = ba
	// A DAG-boundary producer runs the decoupled frontier: block N+1 pre-execs on N's still-live SD
	// while N's FCU lags. Arm the fork validator to KEEP the canonicalised block's SD alive (park it)
	// so the successor reads N's live commitment. Off for normal sync/reorg (drop-on-merge as before).
	e.forkValidator.SetFrontierMode(true)
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
	if e.boundaryAssembler != nil {
		// Insert the block-end marker NOW (non-blocking) so it starts committing during the CL assembly
		// delay, and record the params so GetAssembledBlock can AwaitBoundary + seal. MUST NOT wait here —
		// the CL's ForkChoiceUpdate blocks on AssembleBlock returning a PayloadID.
		if err := e.boundaryAssembler.BeginBoundary(ctx, params); err != nil {
			return AssembleBlockResult{}, err
		}
		e.pendingBoundaryMu.Lock()
		e.pendingBoundary[e.nextPayloadId] = params
		e.pendingBoundaryMu.Unlock()
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
	oldHash, number, sd := e.forkValidator.ExtendingFork()
	if sd == nil || oldHash == (common.Hash{}) {
		return nil, false, nil // no preconfirmed flashblock → from-scratch builder
	}
	if e.currentContext == nil || e.currentContext.BlockOverlay() == nil {
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
		return nil, false, nil
	}
	// The preconfirmed block must be for THIS build (same parent) and must have accumulated under the same
	// proposer attributes the FCU supplies — otherwise the sealed header (in-progress header + output)
	// would be inconsistent with the requested block. Disagreement ⇒ fall back to the from-scratch builder.
	if inHdr.ParentHash != params.ParentHash || !preconfirmAttrsMatch(inHdr, params) {
		// The accumulated flashblock's attributes diverge from the FCU params — fall back to the
		// from-scratch builder rather than seal a header inconsistent with the requested block.
		return nil, false, nil
	}

	// CLOSE (seal): block-end over the maintained SD → the output side, zero body re-execution.
	res, err := e.validateChainLocked(ctx, oldHash, number)
	if err != nil {
		return nil, false, err
	}
	if res.ValidationStatus != ExecutionStatusSuccess {
		return nil, false, fmt.Errorf("assemblePreconfirmed: close status=%v err=%q", res.ValidationStatus, res.ValidationError)
	}

	// Sealed header = the accumulated in-progress header + the computed output side.
	sealed := types.CopyHeader(inHdr)
	sealed.Root = res.ComputedRoot
	sealed.GasUsed = res.GasUsed
	sealed.ReceiptHash = res.ReceiptHash
	sealed.Bloom = res.Bloom

	// Receipts the close accumulated + restamped on the sealed SD (zero re-exec).
	var receipts types.Receipts
	if _, _, sealedSD := e.forkValidator.ExtendingFork(); sealedSD != nil {
		receipts = sealedSD.FlashblockReceipts()
	}

	block := types.NewBlockForAsembling(sealed, body.Transactions, nil, receipts, params.Withdrawals)

	// newPayload ingest: re-key the extending fork to the sealed header so a subsequent FCU canonicalises it.
	if err := e.ingestSealedFlashblockLocked(ctx, sealed); err != nil {
		return nil, false, err
	}
	e.logger.Info("[execmodule] preconfirm assemble: sealed preconfirmed flashblock",
		"number", number, "hash", sealed.Hash(), "root", sealed.Root, "gasUsed", sealed.GasUsed, "txs", len(body.Transactions))
	return &types.BlockWithReceipts{Block: block, Receipts: receipts}, true, nil
}

// SealBoundary is the marker-driven CLOSE (the UNIVERSAL block-production step). The boundary assembler
// (the cocoon driver's commits handler) calls it when the block-end MARKER commits in consensus — on EVERY
// node, decoupled from the CL role. It seals the pre-executed in-progress flashblock via assemblePreconfirmed
// (zero re-execution) and stores it keyed by the block's PARENT hash, so the CL's GetAssembledBlock (proposer)
// — riding behind — just RETRIEVES the already-sealed block instead of re-sealing it. Returns the sealed
// block (or nil if there is no matching preconfirmed flashblock for these params). Acquires the semaphore.
func (e *ExecModule) SealBoundary(ctx context.Context, params *builder.Parameters) (*types.BlockWithReceipts, error) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer e.semaphore.Release(1)
	br, ok, err := e.assemblePreconfirmed(ctx, params)
	if err != nil || !ok || br == nil {
		return nil, err
	}
	e.pendingBoundaryMu.Lock()
	e.preconfirmedByParent[params.ParentHash] = br
	e.pendingBoundaryMu.Unlock()
	e.logger.Info("[execmodule] boundary sealed at marker", "number", br.Block.NumberU64(), "hash", br.Block.Hash(), "parent", params.ParentHash)
	return br, nil
}

// AbandonExtendingFork discards the active pre-executed in-progress block so the next PreExecute re-opens it
// from a fresh SD (re-running block-start under corrected attrs). See ForkValidator.AbandonExtendingFork and
// the ExecutionModule interface. Takes the exec semaphore to serialise against PreExecute/assemble.
func (e *ExecModule) AbandonExtendingFork() {
	if err := e.semaphore.Acquire(context.Background(), 1); err != nil {
		return
	}
	defer e.semaphore.Release(1)
	e.forkValidator.AbandonExtendingFork()
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
		gas.SetUint64(br.Receipts[i].GasUsed)

		effectiveTip := txs[i].GetEffectiveGasTip(baseFee)

		txValue.Mul(&gas, &effectiveTip)
		blockValue.Add(blockValue, &txValue)
	}
	return blockValue
}

func (e *ExecModule) GetAssembledBlock(ctx context.Context, payloadID uint64) (AssembledBlockResult, error) {
	// DAG BOUNDARY: complete a boundary assemble whose marker was inserted (non-blocking) by AssembleBlock.
	// AwaitBoundary FIRST, WITHOUT the semaphore — the commits-handler PreExecute needs the semaphore to
	// record the body, so holding it here would deadlock the wait. Then acquire the semaphore and seal via
	// assemblePreconfirmed (zero re-execution; the marker carried these attrs so its checks match).
	e.pendingBoundaryMu.Lock()
	params, isBoundary := e.pendingBoundary[payloadID]
	e.pendingBoundaryMu.Unlock()
	if isBoundary {
		// GetPayload just RETURNS the block the marker handler already sealed (SealBoundary) — it does NOT
		// seal. AwaitBoundary drives the DAG so the marker commits (and the commits handler seals+stores);
		// then the block is retrieved by its parent hash. The CL is riding behind the marker-driven close.
		if err := e.boundaryAssembler.AwaitBoundary(ctx, params); err != nil {
			// Marker not committed in time (e.g. a quiet DAG at boot). Drop it and return no block — the CL
			// skips this slot and retries; nothing to canonicalise yet. Not fatal.
			e.pendingBoundaryMu.Lock()
			delete(e.pendingBoundary, payloadID)
			e.pendingBoundaryMu.Unlock()
			e.logger.Info("[GetPayload] DAG boundary not ready, skipping slot", "payload", payloadID, "err", err)
			return AssembledBlockResult{}, nil
		}
		e.pendingBoundaryMu.Lock()
		br, ready := e.preconfirmedByParent[params.ParentHash]
		if ready {
			delete(e.preconfirmedByParent, params.ParentHash)
			delete(e.pendingBoundary, payloadID)
		}
		e.pendingBoundaryMu.Unlock()
		if !ready {
			// AwaitBoundary returned but the commits handler has not stored the sealed block yet (a race at
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
