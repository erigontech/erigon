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
	"github.com/erigontech/erigon/execution/types"
)

var (
	errNotOurProposal         = errors.New("next slot is not proposed by a registered validator")
	errNoPayloadID            = errors.New("execution layer returned no payload id")
	errHeadTooFarBack         = errors.New("head state is too far behind the slot to prepare")
	errPreparationHeadChanged = errors.New("selected head changed while preparing payload")
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

// payloadPreparationGate keeps the short builder-start call separate from block production.
// Preparation never holds the gate while copying state or waiting to retry.
type payloadPreparationGate struct {
	attempt sync.RWMutex
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
	g.attempt.RLock()
	return sync.OnceFunc(g.attempt.RUnlock)
}

func (g *payloadPreparationGate) idle() bool {
	if !g.attempt.TryLock() {
		return false
	}
	g.attempt.Unlock()
	return true
}

func (g *payloadPreparationGate) tryBeginPreparation() (func(), bool) {
	if !g.attempt.TryLock() {
		return nil, false
	}
	return g.attempt.Unlock, true
}

// StartPayloadPreparation primes the execution layer for slots this node is due to propose.
// The returned channel closes when the preparation loop stops.
func (a *ApiHandler) StartPayloadPreparation(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if a.routerCfg == nil || !a.routerCfg.Validator || a.engine == nil {
		close(done)
		return done
	}
	// Direct builder startup requires the same local chain access reported by SupportInsertion.
	if _, ok := a.engine.(execution_client.PayloadBuilder); !ok || !a.engine.SupportInsertion() {
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

	var lastSettled slotHead
	var lastFailureLog time.Time
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
		// Gloas uses builder bids instead of local payload priming. Before Capella the payload
		// attributes needed by this path are not available.
		if stateVersion.AfterOrEqual(clparams.GloasVersion) {
			return
		}
		if stateVersion.Before(clparams.CapellaVersion) {
			continue
		}
		// This early check avoids state work during production. The gate is checked again before
		// builder startup to cover production that begins after this point.
		if !a.payloadPreparationGate.idle() {
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
		current := slotHead{slot: targetSlot, head: selectedRoot, generation: generation}
		if current == lastSettled {
			continue
		}
		prepareCtx, cancel := context.WithDeadlineCause(ctx, slotStart, errPreparationTooLate)
		head, err := a.preparePayloadFor(prepareCtx, targetSlot)
		cancel()
		outcome := slotHead{slot: targetSlot, head: head, generation: generation}
		if err != nil {
			if errors.Is(err, errNotOurProposal) || errors.Is(err, errNoPayloadID) {
				lastSettled = outcome
			}
			if !isExpectedPreparationSkip(err) && time.Since(lastFailureLog) >= time.Minute {
				log.Warn("PayloadPreparation: failed", "slot", targetSlot, "err", err)
				lastFailureLog = time.Now()
			}
			continue
		}
		lastSettled = outcome
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

// isExpectedPreparationSkip reports whether there was simply nothing to prepare, as opposed to a
// failure worth reporting.
func isExpectedPreparationSkip(err error) bool {
	return errors.Is(err, errNotOurProposal) ||
		errors.Is(err, errNoPayloadID) ||
		errors.Is(err, errHeadTooFarBack) ||
		errors.Is(err, errPreparationHeadChanged) ||
		errors.Is(err, errProposalInFlight) ||
		errors.Is(err, errPreparationTooLate) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, synced_data.ErrNotSynced)
}

// preparePayloadFor starts the builder for targetSlot without changing execution fork choice.
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
	withdrawals, err := a.expectedWithdrawals(baseState, nil, stateVersion, targetSlot)
	if err != nil {
		return baseBlockRoot, err
	}
	head := baseState.LatestExecutionPayloadHeader().BlockHash
	attrs := a.payloadBuildAttributes(
		baseState, baseBlockRoot, targetSlot, feeRecipient, withdrawals, nil, nil, stateVersion,
	)
	payloadID, err := a.startPayloadBuildForPreparation(ctx, baseBlockRoot, head, attrs)
	if err != nil {
		return baseBlockRoot, err
	}
	if len(payloadID) == 0 {
		return baseBlockRoot, errNoPayloadID
	}
	selectedRoot, _, selected := a.syncedData.SelectedHead()
	if !selected || selectedRoot != baseBlockRoot {
		return baseBlockRoot, errPreparationHeadChanged
	}

	a.preparedPayload.set(targetSlot, payloadID, time.Now())
	log.Info("PayloadPreparation: primed execution layer", "slot", targetSlot, "proposer", proposerIndex, "head", baseBlockRoot)
	return baseBlockRoot, nil
}

// startPayloadBuildForPreparation retries only when the execution head is still catching up or the
// in-process execution module is busy. Each attempt is non-blocking and separately gated.
func (a *ApiHandler) startPayloadBuildForPreparation(
	ctx context.Context,
	baseBlockRoot common.Hash,
	head common.Hash,
	attrs *engine_types.PayloadAttributes,
) ([]byte, error) {
	payloadBuilder, ok := a.engine.(execution_client.PayloadBuilder)
	if !ok {
		return nil, execution_client.ErrNotSupported
	}
	for {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		selectedRoot, _, selected := a.syncedData.SelectedHead()
		if !selected || selectedRoot != baseBlockRoot {
			return nil, errPreparationHeadChanged
		}
		finishAttempt, ok := a.payloadPreparationGate.tryBeginPreparation()
		if !ok {
			return nil, errProposalInFlight
		}
		payloadID, err := payloadBuilder.StartPayloadBuild(ctx, head, attrs)
		finishAttempt()
		if err == nil {
			return payloadID, nil
		}
		if !errors.Is(err, execution_client.ErrPayloadBuildHeadMismatch) &&
			!errors.Is(err, chainreader.ErrExecutionBusy) {
			return payloadID, err
		}
		if err := common.Sleep(ctx, 100*time.Millisecond); err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return nil, cause
			}
			return nil, err
		}
	}
}

// payloadBuildAttributes is shared because production reuses a prepared builder only when every
// attribute is identical. Fields introduced with Gloas remain nil during pre-Gloas preparation.
func (a *ApiHandler) payloadBuildAttributes(
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	feeRecipient common.Address,
	withdrawals []*types.Withdrawal,
	slotNumber, targetGasLimit *hexutil.Uint64,
	stateVersion clparams.StateVersion,
) *engine_types.PayloadAttributes {
	targetEpoch := targetSlot / a.beaconChainCfg.SlotsPerEpoch
	return payloadAttributes(
		stateVersion,
		hexutil.Uint64(state.ComputeTimestampAtSlot(baseState, targetSlot)),
		baseState.GetRandaoMixes(targetEpoch),
		feeRecipient,
		withdrawals,
		&baseBlockRoot,
		slotNumber,
		targetGasLimit,
	)
}
