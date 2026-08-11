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
	errNotOurProposal         = errors.New("next slot is not proposed by a registered validator")
	errNoPayloadID            = errors.New("execution layer returned no payload id")
	errHeadTooFarBack         = errors.New("head state is too far behind the slot to prepare")
	errPreparationHeadChanged = errors.New("head changed while preparing payload")
)

// preparedPayloadRetainSlots keeps a primed record alive past the slot it was primed for, so
// priming the next slot cannot evict the record for a proposal that is still being produced.
const preparedPayloadRetainSlots = 2

type preparedPayloadRecord struct {
	id       []byte
	primedAt time.Time
}

type preparedPayload struct {
	mu       sync.Mutex
	payloads map[uint64]preparedPayloadRecord
}

func (p *preparedPayload) set(slot uint64, payloadID []byte, primedAt time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.payloads == nil {
		p.payloads = map[uint64]preparedPayloadRecord{}
	}
	// Slots this far back can no longer be produced, so dropping them bounds the map.
	for recorded := range p.payloads {
		if recorded+preparedPayloadRetainSlots < slot {
			delete(p.payloads, recorded)
		}
	}
	p.payloads[slot] = preparedPayloadRecord{id: bytes.Clone(payloadID), primedAt: primedAt}
}

func (p *preparedPayload) matches(slot uint64, payloadID []byte, now time.Time, minAge time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	record, ok := p.payloads[slot]
	return ok && len(payloadID) > 0 && bytes.Equal(record.id, payloadID) && now.Sub(record.primedAt) >= minAge
}

func canUsePreparedPayload(p *preparedPayload, builderContinuity bool, slot uint64, payloadID []byte, now time.Time, minAge time.Duration) bool {
	return builderContinuity && p.matches(slot, payloadID, now, minAge)
}

// StartPayloadPreparation primes the execution layer for slots this node is due to propose, so the
// payload is already packed when the validator client asks for a block instead of being built from
// scratch inside the proposal slot.
func (a *ApiHandler) StartPayloadPreparation(ctx context.Context) {
	if a.routerCfg == nil || !a.routerCfg.Validator || a.engine == nil || !a.engine.SupportInsertion() {
		return
	}
	go a.preparePayloadLoop(ctx)
}

func (a *ApiHandler) preparePayloadLoop(ctx context.Context) {
	// A quarter-slot tick lands well inside every slot without assuming when in the slot the head
	// arrives; preparation is skipped unless the next slot is ours, so the cost is a proposer
	// lookup on a state we already hold.
	tick := time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second / 4
	// Preparation is silent on a node that rarely proposes, so say once that it is running:
	// otherwise a loop that never started looks exactly like one with nothing to do.
	log.Info("PayloadPreparation: watching for proposals", "every", tick)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var (
		primedSlot     uint64
		primedHead     common.Hash
		lastFailureLog time.Time
	)
	for immediate := true; ; immediate = false {
		if immediate {
			select {
			case <-ctx.Done():
				return
			default:
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}

		targetSlot := a.ethClock.GetCurrentSlot() + 1
		stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
		if stateVersion.AfterOrEqual(clparams.GloasVersion) {
			return
		}
		if !shouldPrepare(targetSlot, primedSlot, a.syncedData.HeadRoot(), primedHead) {
			continue
		}
		if !shouldPreparePayloadVersion(stateVersion) {
			continue
		}
		prepareCtx, cancel := context.WithDeadline(ctx, a.ethClock.GetSlotTime(targetSlot))
		head, err := a.preparePayloadFor(prepareCtx, targetSlot)
		cancel()
		if err != nil {
			if !isExpectedPreparationSkip(err) && time.Since(lastFailureLog) >= time.Minute {
				log.Warn("PayloadPreparation: failed", "slot", targetSlot, "err", err)
				lastFailureLog = time.Now()
			}
			continue
		}
		primedSlot, primedHead = targetSlot, head
	}
}

// shouldPrepare requires a fresh builder after the target slot or its parent head changes.
func shouldPrepare(targetSlot, primedSlot uint64, head, primedHead common.Hash) bool {
	return targetSlot != primedSlot || head != primedHead
}

func shouldPreparePayloadVersion(version clparams.StateVersion) bool {
	return version.AfterOrEqual(clparams.BellatrixVersion) && version.Before(clparams.GloasVersion)
}

// isExpectedPreparationSkip reports whether there was simply nothing to prepare, as opposed to a
// failure worth reporting.
func isExpectedPreparationSkip(err error) bool {
	return errors.Is(err, errNotOurProposal) ||
		errors.Is(err, errNoPayloadID) ||
		errors.Is(err, errHeadTooFarBack) ||
		errors.Is(err, errPreparationHeadChanged) ||
		errors.Is(err, synced_data.ErrNotSynced)
}

// preparePayloadFor sends the forkchoice update for targetSlot ahead of the slot itself.
func (a *ApiHandler) preparePayloadFor(ctx context.Context, targetSlot uint64) (common.Hash, error) {
	var (
		baseBlockRoot      common.Hash
		proposerIndex      uint64
		feeRecipient       common.Address
		baseState          *state.CachingBeaconState
		lookupAfterAdvance bool
	)
	// Root, proposer and state all come from one view of the head. Reading them separately would
	// let a head update in between pair a parent beacon block root with a different state, priming
	// a builder that production can never match.
	if err := a.syncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, _ uint64) error {
		baseBlockRoot = root
		// Beyond the proposer lookahead the index has to be reshuffled from the seed, which is far
		// too costly to repeat every tick on a large validator set.
		slotsPerEpoch := a.beaconChainCfg.SlotsPerEpoch
		if targetSlot/slotsPerEpoch > headState.Slot()/slotsPerEpoch+a.beaconChainCfg.MinSeedLookahead {
			return errHeadTooFarBack
		}

		lookupAfterAdvance = targetSlot/slotsPerEpoch > headState.Slot()/slotsPerEpoch
		var err error
		if !lookupAfterAdvance {
			if proposerIndex, err = headState.GetBeaconProposerIndexForSlot(targetSlot); err != nil {
				return err
			}
			var ok bool
			if feeRecipient, ok = a.validatorParams.GetFeeRecipient(proposerIndex); !ok {
				return errNotOurProposal
			}
		}
		baseState, err = headState.Copy()
		return err
	}); err != nil {
		return common.Hash{}, err
	}

	if err := transition.DefaultMachine.ProcessSlots(baseState, targetSlot); err != nil {
		return common.Hash{}, err
	}
	if lookupAfterAdvance {
		var err error
		if proposerIndex, err = baseState.GetBeaconProposerIndexForSlot(targetSlot); err != nil {
			return common.Hash{}, err
		}
		var ok bool
		if feeRecipient, ok = a.validatorParams.GetFeeRecipient(proposerIndex); !ok {
			return common.Hash{}, errNotOurProposal
		}
	}

	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
	head, safeHash, finalizedHash, attrs, err := a.preGloasForkChoiceInputs(baseState, baseBlockRoot, targetSlot, feeRecipient, stateVersion)
	if err != nil {
		return common.Hash{}, err
	}
	if a.syncedData.HeadRoot() != baseBlockRoot {
		return common.Hash{}, errPreparationHeadChanged
	}
	payloadID, err := a.engine.ForkChoiceUpdate(ctx, finalizedHash, safeHash, head, attrs, stateVersion)
	if err != nil {
		return common.Hash{}, err
	}
	if len(payloadID) == 0 {
		return common.Hash{}, errNoPayloadID
	}

	a.preparedPayload.set(targetSlot, payloadID, time.Now())
	log.Info("PayloadPreparation: primed execution layer", "slot", targetSlot, "proposer", proposerIndex, "head", baseBlockRoot)
	return baseBlockRoot, nil
}

// preGloasForkChoiceInputs builds the shared preparation and production inputs.
func (a *ApiHandler) preGloasForkChoiceInputs(
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	feeRecipient common.Address,
	stateVersion clparams.StateVersion,
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
	var withdrawals []*types.Withdrawal
	if stateVersion.AfterOrEqual(clparams.CapellaVersion) {
		clWithdrawals, err := state.GetExpectedWithdrawals(baseState, epoch)
		if err != nil {
			return head, safeHash, finalizedHash, nil, err
		}
		withdrawals = make([]*types.Withdrawal, 0, len(clWithdrawals.Withdrawals))
		for _, w := range clWithdrawals.Withdrawals {
			withdrawals = append(withdrawals, &types.Withdrawal{
				Index:     w.Index,
				Amount:    w.Amount,
				Validator: w.Validator,
				Address:   w.Address,
			})
		}
	}

	attrs = payloadAttributesForVersion(
		stateVersion,
		hexutil.Uint64(state.ComputeTimestampAtSlot(baseState, targetSlot)),
		baseState.GetRandaoMixes(epoch),
		feeRecipient,
		withdrawals,
		&baseBlockRoot,
		nil,
		nil,
	)
	return head, safeHash, finalizedHash, attrs, nil
}
