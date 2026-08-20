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
	"sync/atomic"
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
	errExecutionWorkInFlight  = errors.New("block production or local block adoption is using the execution layer")
	errPreparationTooLate     = errors.New("slot is too close to prime a payload production would use")
)

// preparedPayloadRetainSlots keeps a primed record alive past the slot it was primed for, so
// priming the next slot cannot evict the record for a proposal that is still being produced.
const preparedPayloadRetainSlots = 2

// Leave enough time for the state copy and one builder-start attempt. Starting later is likely to
// overlap production without giving the builder useful warmup.
const minimumPreparationLead = 500 * time.Millisecond

type preparedPayloadRecord struct {
	id       []byte
	primedAt time.Time
}

type preparedPayload struct {
	mu       sync.Mutex
	payloads map[uint64]preparedPayloadRecord
}

type payloadPreparationScratch struct {
	state      *state.CachingBeaconState
	targetSlot uint64
}

// copyFrom reuses the scratch state's large buffers across attempts for one target slot.
func (s *payloadPreparationScratch) copyFrom(source *state.CachingBeaconState, cfg *clparams.BeaconChainConfig) (*state.CachingBeaconState, error) {
	if s.state == nil {
		s.state = state.New(cfg)
	}
	return s.state, source.CopyInto(s.state)
}

func (s *payloadPreparationScratch) resetForTargetSlot(targetSlot uint64) {
	if s.targetSlot == targetSlot {
		return
	}
	s.state = nil
	s.targetSlot = targetSlot
}

// payloadPreparationGate keeps builder startup from overlapping execution work for production or
// local block adoption. It also records local block work so stale-head fallback stays off its path.
// Preparation does not hold the execution gate while copying state or waiting to retry.
type payloadPreparationGate struct {
	attempt        sync.RWMutex
	localBlockWork atomic.Int64
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

// warmupAndMismatch returns inherited build time for an exact payload-ID match. It also reports
// when the slot has a prepared record but production chose another payload ID.
func (p *preparedPayload) warmupAndMismatch(slot uint64, payloadID []byte, now time.Time) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	record, ok := p.payloads[slot]
	if !ok || len(payloadID) == 0 {
		return 0, false
	}
	if !bytes.Equal(record.id, payloadID) {
		return 0, true
	}
	return max(now.Sub(record.primedAt), 0), false
}

func (g *payloadPreparationGate) beginExecutionWork() func() {
	g.attempt.RLock()
	return g.attempt.RUnlock
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

func (g *payloadPreparationGate) beginLocalBlockWork() func() {
	g.localBlockWork.Add(1)
	return sync.OnceFunc(func() {
		g.localBlockWork.Add(-1)
	})
}

func (g *payloadPreparationGate) localBlockWorkInFlight() bool {
	return g.localBlockWork.Load() > 0
}

// StartPayloadPreparation primes the execution layer for slots this node is due to propose.
// The returned channel closes when the preparation loop stops.
func (a *ApiHandler) StartPayloadPreparation(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if a.routerCfg == nil || !a.routerCfg.Validator || a.engine == nil {
		close(done)
		return done
	}
	// Only the direct execution client exposes builder startup without a fork-choice update.
	if _, ok := a.engine.(execution_client.PayloadBuilder); !ok {
		a.payloadPreparationLogger().Info(
			"PayloadPreparation: disabled",
			"reason", "execution client does not support direct payload building",
		)
		close(done)
		return done
	}
	go func() {
		defer close(done)
		a.preparePayloadLoop(ctx)
	}()
	return done
}

func (a *ApiHandler) payloadPreparationLogger() log.Logger {
	if a.logger != nil {
		return a.logger
	}
	return log.Root()
}

func (a *ApiHandler) preparePayloadLoop(ctx context.Context) {
	logger := a.payloadPreparationLogger()
	// Polling once per quarter slot gives a newly selected head several chances to trigger
	// preparation. Most non-proposal ticks stop before copying state; a pre-Fulu epoch boundary
	// needs state advancement before the proposer is known.
	tick := time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second / 4
	if tick <= 0 {
		logger.Warn("PayloadPreparation: disabled because the slot duration is zero")
		return
	}
	// Preparation is silent on a node that rarely proposes, so say once that it is running:
	// otherwise a loop that never started looks exactly like one with nothing to do.
	logger.Info("PayloadPreparation: watching for proposals", "every", tick)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var lastSettled slotHead
	var lastFailureLog time.Time
	var scratch payloadPreparationScratch
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

		currentSlot := a.ethClock.GetCurrentSlot()
		targetSlot := currentSlot + 1
		scratch.resetForTargetSlot(targetSlot)
		stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
		// Payload preparation is scoped from Capella until Gloas, where builders gossip bids instead.
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
		// off; a builder primed that early hits its own cap before the slot even starts.
		slotStart := a.ethClock.GetSlotTime(targetSlot)
		lead := time.Until(slotStart)
		if lead > maxPreparationLead(a.beaconChainCfg) || lead <= minimumPreparationLead {
			continue
		}
		selectedRoot, selectedSlot, selected := a.syncedData.SelectedHead()
		if !selected {
			continue
		}
		if selectedSlot != currentSlot && shouldWaitForCurrentSlotHead(
			currentSlot,
			selectedSlot,
			time.Now(),
			a.ethClock.GetSlotTime(currentSlot),
			attestationDue(a.beaconChainCfg, stateVersion),
			a.payloadPreparationGate.localBlockWorkInFlight(),
		) {
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
		head, err := a.preparePayloadForWithScratch(prepareCtx, targetSlot, &scratch)
		cancel()
		outcome := slotHead{slot: targetSlot, head: head, generation: generation}
		if err != nil {
			if errors.Is(err, errNotOurProposal) || errors.Is(err, errNoPayloadID) {
				lastSettled = outcome
			}
			if !isExpectedPreparationSkip(err) && time.Since(lastFailureLog) >= time.Minute {
				logger.Warn("PayloadPreparation: failed", "slot", targetSlot, "err", err)
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

// An older selected head becomes usable only after the current slot's attestation deadline and
// after local block work has finished. A future selected head is never valid here.
func shouldWaitForCurrentSlotHead(
	currentSlot, selectedSlot uint64,
	now, currentSlotStart time.Time,
	attestationDeadline time.Duration,
	localBlockWorkInFlight bool,
) bool {
	if selectedSlot == currentSlot {
		return false
	}
	if selectedSlot > currentSlot || localBlockWorkInFlight {
		return true
	}
	return now.Before(currentSlotStart.Add(attestationDeadline))
}

// isExpectedPreparationSkip reports whether there was simply nothing to prepare, as opposed to a
// failure worth reporting.
func isExpectedPreparationSkip(err error) bool {
	return errors.Is(err, errNotOurProposal) ||
		errors.Is(err, errNoPayloadID) ||
		errors.Is(err, errHeadTooFarBack) ||
		errors.Is(err, errPreparationHeadChanged) ||
		errors.Is(err, errExecutionWorkInFlight) ||
		errors.Is(err, errPreparationTooLate) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, synced_data.ErrNotSynced)
}

func (a *ApiHandler) preparePayloadForWithScratch(
	ctx context.Context,
	targetSlot uint64,
	scratch *payloadPreparationScratch,
) (common.Hash, error) {
	if time.Until(a.ethClock.GetSlotTime(targetSlot)) <= minimumPreparationLead {
		return common.Hash{}, errPreparationTooLate
	}
	scratch.resetForTargetSlot(targetSlot)
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
			proposerIndex, feeRecipient, err = a.registeredProposer(headState, targetSlot)
			if err != nil {
				return err
			}
		}
		if !a.payloadPreparationGate.idle() {
			return errExecutionWorkInFlight
		}
		baseState, err = scratch.copyFrom(headState, a.beaconChainCfg)
		return err
	}); err != nil {
		return baseBlockRoot, err
	}

	if err := transition.DefaultMachine.ProcessSlots(baseState, targetSlot); err != nil {
		return baseBlockRoot, err
	}
	if lookupAfterAdvance {
		var err error
		proposerIndex, feeRecipient, err = a.registeredProposer(baseState, targetSlot)
		if err != nil {
			return baseBlockRoot, err
		}
	}

	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
	// The state copy and epoch transition can cross the slot boundary. Do not start preparation once
	// production may already be running for the target slot.
	if time.Until(a.ethClock.GetSlotTime(targetSlot)) <= 0 {
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
	a.payloadPreparationLogger().Info("PayloadPreparation: primed execution layer", "slot", targetSlot, "proposer", proposerIndex, "head", baseBlockRoot)
	return baseBlockRoot, nil
}

func (a *ApiHandler) registeredProposer(beaconState *state.CachingBeaconState, targetSlot uint64) (uint64, common.Address, error) {
	proposerIndex, err := beaconState.GetBeaconProposerIndexForSlot(targetSlot)
	if err != nil {
		return 0, common.Address{}, err
	}
	feeRecipient, ok := a.validatorParams.GetFeeRecipient(proposerIndex)
	if !ok {
		return proposerIndex, common.Address{}, errNotOurProposal
	}
	return proposerIndex, feeRecipient, nil
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
			return nil, errExecutionWorkInFlight
		}
		payloadID, err := payloadBuilder.StartPayloadBuild(ctx, head, attrs)
		finishAttempt()
		if err == nil {
			return payloadID, nil
		}
		if !errors.Is(err, execution_client.ErrPayloadBuildHeadMismatch) &&
			!errors.Is(err, chainreader.ErrExecutionBusy) {
			return nil, err
		}
		if err := common.Sleep(ctx, 100*time.Millisecond); err != nil {
			return nil, context.Cause(ctx)
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
