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

	"github.com/erigontech/erigon/cl/beacon/synced_data"
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
	errHeadTooFarBack = errors.New("head state is too far behind the slot to prepare")
)

// preparedPayloadRetainSlots keeps a primed record alive past the slot it was primed for, so
// priming the next slot cannot evict the record for a proposal that is still being produced.
const preparedPayloadRetainSlots = 2

// preparedPayload records the payload id the execution layer returned for a slot this node primed
// ahead of time. Block production compares the id its own forkchoice update returns against this
// record: an equal id means the execution layer recognised the request as a repeat and has been
// packing transactions since the prime, so the payload is worth taking early. Anything else — a
// reorg, a late block, a changed fee recipient, an execution layer that was busy — yields a
// different id and leaves production on its usual later schedule.
// Records are kept per slot: consecutive proposals would otherwise let the prime for the later slot
// evict the one production is about to look up.
type preparedPayload struct {
	mu       sync.Mutex
	payloads map[uint64][]byte
}

func (p *preparedPayload) set(slot uint64, payloadID []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.payloads == nil {
		p.payloads = map[uint64][]byte{}
	}
	// Slots this far back can no longer be produced, so dropping them bounds the map.
	for recorded := range p.payloads {
		if recorded+preparedPayloadRetainSlots < slot {
			delete(p.payloads, recorded)
		}
	}
	p.payloads[slot] = bytes.Clone(payloadID)
}

func (p *preparedPayload) matches(slot uint64, payloadID []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(payloadID) > 0 && bytes.Equal(p.payloads[slot], payloadID)
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
			// Most ticks land on a slot somebody else proposes; logging those would drown out the
			// failures worth seeing.
			if !isExpectedPreparationSkip(err) {
				log.Debug("PayloadPreparation: skipped", "slot", targetSlot, "err", err)
			}
			continue
		}
		lastPrepared = targetSlot
	}
}

// isExpectedPreparationSkip reports whether there was simply nothing to prepare, as opposed to a
// failure worth reporting.
func isExpectedPreparationSkip(err error) bool {
	return errors.Is(err, errNotOurProposal) ||
		errors.Is(err, errNodeSyncing) ||
		errors.Is(err, errHeadTooFarBack) ||
		errors.Is(err, synced_data.ErrNotSynced)
}

// preparePayloadFor sends the forkchoice update for targetSlot ahead of the slot itself. It returns
// an error, rather than logging loudly, whenever there is simply nothing to do — the node is
// syncing, the slot belongs to someone else, or the validator client has not registered a fee
// recipient yet — because block production falls back to building inside the slot in every one of
// those cases.
func (a *ApiHandler) preparePayloadFor(ctx context.Context, targetSlot uint64) error {
	var (
		baseBlockRoot common.Hash
		proposerIndex uint64
		feeRecipient  common.Address
		baseState     *state.CachingBeaconState
	)
	// Root, proposer and state all come from one view of the head. Reading them separately would
	// let a head update in between pair a parent beacon block root with a different state, priming
	// a builder that production can never match.
	if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		baseBlockRoot = a.syncedData.HeadRoot()
		if baseBlockRoot == (common.Hash{}) {
			return errNodeSyncing
		}
		// Beyond the proposer lookahead the index has to be reshuffled from the seed, which is far
		// too costly to repeat every tick on a large validator set.
		slotsPerEpoch := a.beaconChainCfg.SlotsPerEpoch
		if targetSlot/slotsPerEpoch > headState.Slot()/slotsPerEpoch+a.beaconChainCfg.MinSeedLookahead {
			return errHeadTooFarBack
		}

		var err error
		if proposerIndex, err = headState.GetBeaconProposerIndexForSlot(targetSlot); err != nil {
			return err
		}
		// Only our own proposals are worth priming, and a fee recipient we do not yet know would
		// build a payload that block production could not reuse anyway. Checked before the state
		// copy, which is the expensive part.
		var ok bool
		if feeRecipient, ok = a.validatorParams.GetFeeRecipient(proposerIndex); !ok {
			return errNotOurProposal
		}
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
