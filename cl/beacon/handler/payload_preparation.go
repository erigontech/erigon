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
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
)

var (
	errNotOurProposal         = errors.New("next slot is not proposed by a registered validator")
	errNoPayloadID            = errors.New("execution layer returned no payload id")
	errHeadTooFarBack         = errors.New("head state is too far behind the slot to prepare")
	errPreparationHeadChanged = synced_data.ErrSelectedHeadChanged
	errProposalInFlight       = errors.New("block production is in progress")
	errPreparationTooLate     = errors.New("slot is too close to prime a payload production would use")
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

// payloadPreparationGate lets production cancel speculative EL work without waiting for it.
type payloadPreparationGate struct {
	mu          sync.Mutex
	proposals   int
	preparation *payloadPreparationAdmission
}

type payloadPreparationAdmission struct {
	cancel context.CancelCauseFunc
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

func (g *payloadPreparationGate) beginProduction() func() {
	g.mu.Lock()
	g.proposals++
	preparation := g.preparation
	if preparation != nil {
		preparation.cancel(errProposalInFlight)
	}
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		g.proposals--
		g.mu.Unlock()
	}
}

func (g *payloadPreparationGate) productionInFlight() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.proposals > 0
}

func (g *payloadPreparationGate) tryBeginPreparation(ctx context.Context) (context.Context, func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.proposals > 0 || g.preparation != nil {
		return nil, nil, false
	}
	prepareCtx, cancel := context.WithCancelCause(ctx)
	admission := &payloadPreparationAdmission{cancel: cancel}
	g.preparation = admission
	return prepareCtx, func() {
		cancel(context.Canceled)
		g.mu.Lock()
		if g.preparation == admission {
			g.preparation = nil
		}
		g.mu.Unlock()
	}, true
}

// StartPayloadPreparation primes the execution layer for slots this node is due to propose.
// The returned channel closes when the preparation loop stops.
func (a *ApiHandler) StartPayloadPreparation(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if a.routerCfg == nil || !a.routerCfg.Validator || a.engine == nil || !a.engine.SupportInsertion() {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		a.preparePayloadLoop(ctx)
	}()
	return done
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
		primed         slotHead
		notOurs        slotHead
		failed         slotHead
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
		if preparationRetired(stateVersion) {
			return
		}
		if !shouldPreparePayloadVersion(stateVersion) {
			continue
		}
		// On consecutive proposals the prime for the next slot lands inside this one.
		if a.payloadPreparationGate.productionInFlight() {
			continue
		}
		// Nothing is registered, so nothing here can be ours. Checking first keeps a non-validating
		// node off the state copy entirely.
		generation := a.validatorParams.Generation()
		if generation == 0 {
			continue
		}
		// Before genesis the current slot clamps to zero, so the next slot can be arbitrarily far
		// off; a builder primed that early hits its own cap before the slot even starts. Too late
		// and the prime can never reach the age production demands of it.
		slotStart := a.ethClock.GetSlotTime(targetSlot)
		lead := time.Until(slotStart)
		if lead > maxPreparationLead(a.beaconChainCfg) || lead < preparedPayloadMinimumAge(a.beaconChainCfg, stateVersion) {
			continue
		}
		selectedRoot, _, selected := a.syncedData.SelectedHead()
		if !selected {
			continue
		}
		// Preparation requires the selected and materialized head identities to match. Otherwise
		// its payload attributes can target stale state.
		if selectedRoot != a.syncedData.HeadRoot() {
			continue
		}
		// Memoized outcomes remain valid only for the same slot, head and validator registrations.
		settled := slotHead{slot: targetSlot, head: selectedRoot, generation: generation}
		if primed == settled || notOurs == settled || failed == settled {
			continue
		}
		// An ordinary FCU deadline leaves execution work running asynchronously. A preparation-specific
		// cause stops speculative work when the slot begins.
		prepareCtx, cancel := context.WithDeadlineCause(ctx, slotStart, errPreparationTooLate)
		head, err := a.preparePayloadFor(prepareCtx, targetSlot)
		cancel()
		if err != nil {
			outcome := slotHead{slot: targetSlot, head: head, generation: generation}
			if errors.Is(err, errNotOurProposal) {
				notOurs = outcome
			} else if isStablePreparationFailure(err) {
				failed = outcome
			}
			if !isExpectedPreparationSkip(err) && time.Since(lastFailureLog) >= time.Minute {
				log.Warn("PayloadPreparation: failed", "slot", targetSlot, "err", err)
				lastFailureLog = time.Now()
			}
			continue
		}
		primed = slotHead{slot: targetSlot, head: head, generation: generation}
	}
}

// slotHead records work already settled for a target slot, on a given head, under a given set of
// validator registrations. A zero value matches nothing reachable: preparation never runs before
// the first registration arrives.
type slotHead struct {
	slot       uint64
	head       common.Hash
	generation uint64
}

// maxPreparationLead bounds how far ahead of a slot priming is worthwhile. One slot is all a live
// chain ever offers, since preparation only ever targets the slot after the current one.
func maxPreparationLead(cfg *clparams.BeaconChainConfig) time.Duration {
	return time.Duration(cfg.SecondsPerSlot) * time.Second
}

// preparationRetired is the single authority for the fork after which builders gossip bids
// instead of being primed, so the loop and the per-slot check cannot drift apart.
func preparationRetired(version clparams.StateVersion) bool {
	return version.AfterOrEqual(clparams.GloasVersion)
}

// shouldPreparePayloadVersion confines preparation to the Capella-through-Fulu builder protocol.
func shouldPreparePayloadVersion(version clparams.StateVersion) bool {
	return version.AfterOrEqual(clparams.CapellaVersion) && !preparationRetired(version)
}

// isExpectedPreparationSkip reports whether there was simply nothing to prepare, as opposed to a
// failure worth reporting.
func isExpectedPreparationSkip(err error) bool {
	return errors.Is(err, errNotOurProposal) ||
		errors.Is(err, errNoPayloadID) ||
		errors.Is(err, errHeadTooFarBack) ||
		errors.Is(err, errPreparationHeadChanged) ||
		errors.Is(err, errProposalInFlight) ||
		errors.Is(err, errPreparationTooLate) ||
		errors.Is(err, execution_client.ErrPayloadPreparationPreempted) ||
		errors.Is(err, execution_client.ErrForkChoiceNotAdopted) ||
		errors.Is(err, execution_client.ErrForkChoiceSyncing) ||
		errors.Is(err, execution_client.ErrForkChoiceUpdateNoPayloadID) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, synced_data.ErrNotSynced)
}

// isStablePreparationFailure identifies answers that cannot change while the target slot, selected
// head and validator registrations remain unchanged.
func isStablePreparationFailure(err error) bool {
	return errors.Is(err, errNoPayloadID) ||
		errors.Is(err, execution_client.ErrForkChoiceUpdateNoPayloadID) ||
		errors.Is(err, execution_client.ErrForkChoiceNotAdopted)
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

		// Fulu's proposer lookahead is valid across the next epoch, so reject an unregistered
		// proposer before copying and advancing the full state.
		lookupAfterAdvance = targetSlot/slotsPerEpoch > headState.Slot()/slotsPerEpoch && headState.Version().Before(clparams.FuluVersion)
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
		return baseBlockRoot, err
	}

	if err := transition.DefaultMachine.ProcessSlots(baseState, targetSlot); err != nil {
		return baseBlockRoot, err
	}
	if lookupAfterAdvance {
		var err error
		if proposerIndex, err = baseState.GetBeaconProposerIndexForSlot(targetSlot); err != nil {
			return baseBlockRoot, err
		}
		var ok bool
		if feeRecipient, ok = a.validatorParams.GetFeeRecipient(proposerIndex); !ok {
			return baseBlockRoot, errNotOurProposal
		}
	}

	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
	// The lead was measured before the copy and the epoch transition, which are slow enough to have
	// spent it. Stop rather than record a prime production would reject on age.
	if time.Until(a.ethClock.GetSlotTime(targetSlot)) < preparedPayloadMinimumAge(a.beaconChainCfg, stateVersion) {
		return baseBlockRoot, errPreparationTooLate
	}
	head, safeHash, finalizedHash, attrs, err := a.preGloasForkChoiceInputs(baseState, baseBlockRoot, targetSlot, feeRecipient, stateVersion)
	if err != nil {
		return baseBlockRoot, err
	}
	payloadID, err := a.forkChoiceUpdateForPreparation(ctx, baseBlockRoot, finalizedHash, safeHash, head, attrs, stateVersion)
	if err != nil {
		return baseBlockRoot, err
	}
	if len(payloadID) == 0 {
		return baseBlockRoot, errNoPayloadID
	}

	a.preparedPayload.set(targetSlot, payloadID, time.Now())
	log.Info("PayloadPreparation: primed execution layer", "slot", targetSlot, "proposer", proposerIndex, "head", baseBlockRoot)
	return baseBlockRoot, nil
}

// forkChoiceUpdateForPreparation retries contention while baseBlockRoot remains selected. One
// cancellation scope spans every attempt and retry delay, so higher-priority execution work can
// preempt preparation without waiting for the retry sequence to finish.
func (a *ApiHandler) forkChoiceUpdateForPreparation(
	ctx context.Context,
	baseBlockRoot common.Hash,
	finalized, safe, head common.Hash,
	attrs *engine_types.PayloadAttributes,
	stateVersion clparams.StateVersion,
) ([]byte, error) {
	headCtx, releaseHead, ok := a.syncedData.BindToSelectedHead(ctx, baseBlockRoot)
	if !ok {
		return nil, errPreparationHeadChanged
	}
	defer releaseHead()
	headCtx, releaseExecution := execution_client.BindPayloadPreparation(headCtx, a.engine)
	defer releaseExecution()
	prepareCtx, finishPreparation, ok := a.payloadPreparationGate.tryBeginPreparation(headCtx)
	if !ok {
		return nil, errProposalInFlight
	}
	defer finishPreparation()

	for {
		if cause := context.Cause(prepareCtx); cause != nil {
			return nil, cause
		}
		payloadID, err := a.engine.ForkChoiceUpdate(prepareCtx, finalized, safe, head, attrs, stateVersion)
		cancelCause := context.Cause(prepareCtx)

		// Success wins a concurrent cancellation because the execution layer has already completed
		// the update for these exact inputs.
		if err == nil {
			return payloadID, nil
		}
		if cancelCause != nil {
			return nil, cancelCause
		}
		if !isPreparationContention(err) {
			return payloadID, err
		}
		if err := common.Sleep(prepareCtx, execution_client.ForkChoiceRetryDelay); err != nil {
			return nil, context.Cause(prepareCtx)
		}
	}
}

func isPreparationContention(err error) bool {
	return execution_client.IsForkChoiceContention(err) ||
		errors.Is(err, chainreader.ErrExecutionBusy)
}

// executionCheckpointHashes resolves the safe and finalized execution hashes known to fork choice.
// An unknown checkpoint stays zero because substituting the head would falsely finalize it.
func (a *ApiHandler) executionCheckpointHashes(baseState *state.CachingBeaconState) (safeHash, finalizedHash common.Hash) {
	finalizedHash = a.forkchoiceStore.GetFinalizedExecutionHash(baseState.FinalizedCheckpoint().Root)
	safeHash = a.forkchoiceStore.GetFinalizedExecutionHash(baseState.CurrentJustifiedCheckpoint().Root)
	return safeHash, finalizedHash
}

// preGloasForkChoiceInputs keeps preparation and production on identical FCU inputs.
func (a *ApiHandler) preGloasForkChoiceInputs(
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	feeRecipient common.Address,
	stateVersion clparams.StateVersion,
) (head, safeHash, finalizedHash common.Hash, attrs *engine_types.PayloadAttributes, err error) {
	head = baseState.LatestExecutionPayloadHeader().BlockHash
	safeHash, finalizedHash = a.executionCheckpointHashes(baseState)

	withdrawals, err := a.expectedWithdrawals(baseState, nil, stateVersion, targetSlot)
	if err != nil {
		return head, safeHash, finalizedHash, nil, err
	}
	targetEpoch := targetSlot / a.beaconChainCfg.SlotsPerEpoch
	attrs = payloadAttributes(
		stateVersion,
		hexutil.Uint64(state.ComputeTimestampAtSlot(baseState, targetSlot)),
		baseState.GetRandaoMixes(targetEpoch),
		feeRecipient,
		withdrawals,
		&baseBlockRoot,
		nil,
		nil,
	)
	return head, safeHash, finalizedHash, attrs, nil
}
