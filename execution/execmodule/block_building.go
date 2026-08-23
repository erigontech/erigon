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

	// PRECONFIRM VARIANT: if a preconfirm producer (op-stack sequencer or the DAG driver) has already
	// accumulated a matching in-progress flashblock in the fork validator, SEAL it synchronously and hand
	// it back directly — no from-scratch build, no body re-execution. General base-erigon path; falls
	// through to the normal builder when there is no matching preconfirmed block.
	if br, ok, err := e.assemblePreconfirmed(ctx, params); err != nil {
		return AssembleBlockResult{}, err
	} else if ok {
		e.preconfirmedBlocks[e.nextPayloadId] = br
		e.logger.Info("[ForkChoiceUpdated] preconfirm assemble", "payload", e.nextPayloadId, "hash", br.Block.Hash())
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

func (e *ExecModule) GetAssembledBlock(_ context.Context, payloadID uint64) (AssembledBlockResult, error) {
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
