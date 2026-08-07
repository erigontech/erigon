// Copyright 2026 The Erigon Authors
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

package handler

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

var (
	errNodeSyncing    = errors.New("node is syncing")
	errNotOurProposal = errors.New("next slot is not proposed by a registered validator")
	errNoPayloadID    = errors.New("execution layer returned no payload id")
)

// preparedPayload records the payload id the execution layer returned for a slot this node primed
// ahead of time. Block production compares the id its own forkchoice update returns against this
// record: an equal id means the execution layer recognised the request as a repeat and has been
// packing transactions since the prime, so the payload is worth taking early. Anything else — a
// reorg, a late block, a changed fee recipient, an execution layer that was busy — yields a
// different id and leaves production on its usual later schedule.
type preparedPayload struct {
	mu        sync.Mutex
	slot      uint64
	payloadID []byte
}

func (p *preparedPayload) set(slot uint64, payloadID []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slot, p.payloadID = slot, bytes.Clone(payloadID)
}

func (p *preparedPayload) matches(slot uint64, payloadID []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(payloadID) > 0 && p.slot == slot && bytes.Equal(p.payloadID, payloadID)
}

// StartPayloadPreparation primes the execution layer for slots this node is due to propose, so the
// payload is already packed when the validator client asks for a block instead of being built from
// scratch inside the proposal slot.
func (a *ApiHandler) StartPayloadPreparation(ctx context.Context) {
	go a.preparePayloadLoop(ctx)
}

func (a *ApiHandler) preparePayloadLoop(ctx context.Context) {
	// A quarter-slot tick lands well inside every slot without assuming when in the slot the head
	// arrives; preparation is skipped unless the next slot is ours, so the cost is a proposer
	// lookup on a state we already hold.
	ticker := time.NewTicker(time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second / 4)
	defer ticker.Stop()

	var lastPrepared uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		targetSlot := a.ethClock.GetCurrentSlot() + 1
		if targetSlot <= lastPrepared {
			continue
		}
		if a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch).AfterOrEqual(clparams.GloasVersion) {
			// [Gloas:EIP7732] builders gossip bids instead; the engine is not primed this way.
			continue
		}
		if err := a.preparePayloadFor(ctx, targetSlot); err != nil {
			log.Debug("PayloadPreparation: skipped", "slot", targetSlot, "err", err)
			continue
		}
		lastPrepared = targetSlot
	}
}

// preparePayloadFor sends the forkchoice update for targetSlot ahead of the slot itself. It returns
// an error, rather than logging loudly, whenever there is simply nothing to do — the node is
// syncing, the slot belongs to someone else, or the validator client has not registered a fee
// recipient yet — because block production falls back to building inside the slot in every one of
// those cases.
func (a *ApiHandler) preparePayloadFor(ctx context.Context, targetSlot uint64) error {
	baseBlockRoot := a.syncedData.HeadRoot()
	if baseBlockRoot == (common.Hash{}) {
		return errNodeSyncing
	}

	var proposerIndex uint64
	if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		var err error
		proposerIndex, err = headState.GetBeaconProposerIndexForSlot(targetSlot)
		return err
	}); err != nil {
		return err
	}
	// Only our own proposals are worth priming, and a fee recipient we do not yet know would build
	// a payload that block production could not reuse anyway.
	feeRecipient, ok := a.validatorParams.GetFeeRecipient(proposerIndex)
	if !ok {
		return errNotOurProposal
	}

	var baseState *state.CachingBeaconState
	if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		var err error
		baseState, err = headState.Copy()
		return err
	}); err != nil {
		return err
	}
	if err := transition.DefaultMachine.ProcessSlots(baseState, targetSlot); err != nil {
		return err
	}

	head, safeHash, finalizedHash, attrs, err := a.preparedForkChoiceInputs(baseState, baseBlockRoot, targetSlot, feeRecipient)
	if err != nil {
		return err
	}
	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
	payloadID, err := a.engine.ForkChoiceUpdate(ctx, finalizedHash, safeHash, head, attrs, stateVersion)
	if err != nil {
		return err
	}
	if len(payloadID) == 0 {
		return errNoPayloadID
	}

	a.preparedPayload.set(targetSlot, payloadID)
	log.Info("PayloadPreparation: primed execution layer", "slot", targetSlot, "proposer", proposerIndex)
	return nil
}

// preparedForkChoiceInputs assembles the forkchoice-update arguments for building targetSlot on top
// of baseState, mirroring the pre-Gloas path in produceBeaconBody.
//
// The two must derive byte-identical arguments: the execution layer keeps the builder it already
// warmed only when it recognises the request as a repeat. Divergence costs that warm builder and
// nothing else — production simply builds inside the slot as before — but it is silent, so
// PayloadPreparation logs whether the primed id was still valid at production time.
func (a *ApiHandler) preparedForkChoiceInputs(
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	feeRecipient common.Address,
) (head, safeHash, finalizedHash common.Hash, attrs *engine_types.PayloadAttributes, err error) {
	head = baseState.LatestExecutionPayloadHeader().BlockHash

	finalizedHash = a.forkchoiceStore.GetFinalizedExecutionHash(baseState.FinalizedCheckpoint().Root)
	if finalizedHash == (common.Hash{}) {
		finalizedHash = head
	}
	safeHash = a.forkchoiceStore.GetFinalizedExecutionHash(baseState.CurrentJustifiedCheckpoint().Root)
	if safeHash == (common.Hash{}) {
		safeHash = head
	}

	epoch := targetSlot / a.beaconChainCfg.SlotsPerEpoch
	clWithdrawals, err := state.GetExpectedWithdrawals(baseState, epoch)
	if err != nil {
		return head, safeHash, finalizedHash, nil, err
	}
	withdrawals := make([]*types.Withdrawal, 0, len(clWithdrawals.Withdrawals))
	for _, w := range clWithdrawals.Withdrawals {
		withdrawals = append(withdrawals, &types.Withdrawal{
			Index:     w.Index,
			Amount:    w.Amount,
			Validator: w.Validator,
			Address:   w.Address,
		})
	}

	attrs = &engine_types.PayloadAttributes{
		Timestamp:             hexutil.Uint64(state.ComputeTimestampAtSlot(baseState, targetSlot)),
		PrevRandao:            baseState.GetRandaoMixes(epoch),
		SuggestedFeeRecipient: feeRecipient,
		Withdrawals:           withdrawals,
		ParentBeaconBlockRoot: &baseBlockRoot,
	}
	return head, safeHash, finalizedHash, attrs, nil
}
