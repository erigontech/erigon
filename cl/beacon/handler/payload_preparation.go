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
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/types"
)

var (
	errNotOurProposal           = errors.New("next slot is not proposed by a registered validator")
	errNoPayloadID              = errors.New("execution layer returned no payload id")
	errHeadTooFarBack           = errors.New("head state is too far behind the slot to prepare")
	errPreparationHeadChanged   = errors.New("selected head changed while preparing payload")
	errBlockWorkInFlight        = errors.New("block production, publication, or adoption is in progress")
	errPreparationTooLate       = errors.New("slot is too close to prime a payload production would use")
	errGloasPayloadPending      = errors.New("gloas parent payload decision is not ready")
	errForkChoiceHeadChanged    = errors.New("fork choice head changed")
	errGloasPathNeedsForkChoice = errors.New("gloas payload path requires a fork-choice update before preparation")
)

// preparedPayloadRetainSlots keeps a primed record alive past the slot it was primed for, so
// priming the next slot cannot evict the record for a proposal that is still being produced.
const preparedPayloadRetainSlots = 2

// minimumPreparationLead leaves a best-effort margin for useful warmup before the proposal slot.
// State copying and slot processing cannot be cancelled mid-call and may still overlap production.
const minimumPreparationLead = 500 * time.Millisecond

const (
	payloadBuildBusyRetryDelay         = 100 * time.Millisecond
	payloadBuildHeadMismatchRetryDelay = 500 * time.Millisecond
)

type preparedPayloadRecord struct {
	id       []byte
	head     common.Hash
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
	s.release()
	s.targetSlot = targetSlot
}

func (s *payloadPreparationScratch) release() {
	s.state = nil
}

// payloadPreparationGate gives block work priority over speculative builder startup. Its exclusive
// side is always acquired with TryLock, so nested shared holds cannot deadlock.
type payloadPreparationGate struct {
	blockWork         sync.RWMutex
	producedBlockMu   sync.Mutex
	producedBlockEnds map[uint64]time.Time
}

func producedBlockSigningExpiry(completedAt, targetSlotStart time.Time, signingWindow time.Duration) time.Time {
	if completedAt.Before(targetSlotStart) {
		completedAt = targetSlotStart
	}
	return completedAt.Add(signingWindow)
}

func (p *preparedPayload) set(slot uint64, payloadID []byte, head common.Hash, primedAt time.Time) {
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
	// Re-recording the same EL builder must preserve its original warmup and head identity.
	if previous, ok := p.payloads[slot]; ok && bytes.Equal(previous.id, payloadID) && previous.primedAt.Before(primedAt) {
		primedAt = previous.primedAt
		head = previous.head
	}
	p.payloads[slot] = preparedPayloadRecord{id: bytes.Clone(payloadID), head: head, primedAt: primedAt}
}

// warmupAndMismatch returns inherited build time for an exact payload-ID match. For a mismatch it
// also returns the head from which the prepared builder started.
func (p *preparedPayload) warmupAndMismatch(slot uint64, payloadID []byte, now time.Time) (time.Duration, common.Hash, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	record, ok := p.payloads[slot]
	if !ok || len(payloadID) == 0 {
		return 0, common.Hash{}, false
	}
	if !bytes.Equal(record.id, payloadID) {
		return 0, record.head, true
	}
	return max(now.Sub(record.primedAt), 0), record.head, false
}

func (g *payloadPreparationGate) beginBlockWork() func() {
	g.blockWork.RLock()
	return sync.OnceFunc(g.blockWork.RUnlock)
}

func (g *payloadPreparationGate) idle() bool {
	if !g.blockWork.TryLock() {
		return false
	}
	g.blockWork.Unlock()
	return true
}

func (g *payloadPreparationGate) tryBeginPreparation(referenceSlot, selectedSlot uint64) (func(), bool) {
	if !g.blockWork.TryLock() {
		return nil, false
	}
	// Production records its marker while holding the shared side of this gate. Checking after
	// taking the exclusive side closes the handoff race between production and signing.
	if g.producedBlockPending(referenceSlot, selectedSlot, time.Now()) {
		g.blockWork.Unlock()
		return nil, false
	}
	return sync.OnceFunc(g.blockWork.Unlock), true
}

func (g *payloadPreparationGate) noteProducedBlock(
	completionSlot, producedSlot uint64,
	completedAt, expiresAt time.Time,
) {
	// Only production completed in its target slot or a neighboring slot can overlap the next
	// preparation window. Older or farther-future requests must not suppress unrelated work.
	if !slotsWithinOne(completionSlot, producedSlot) {
		return
	}
	g.producedBlockMu.Lock()
	defer g.producedBlockMu.Unlock()
	if g.producedBlockEnds == nil {
		g.producedBlockEnds = make(map[uint64]time.Time)
	}
	for slot, expiry := range g.producedBlockEnds {
		if !completedAt.Before(expiry) {
			delete(g.producedBlockEnds, slot)
		}
	}
	if previous, ok := g.producedBlockEnds[producedSlot]; ok && previous.After(expiresAt) {
		return
	}
	g.producedBlockEnds[producedSlot] = expiresAt
}

func (g *payloadPreparationGate) clearProducedBlock(slot uint64) {
	g.producedBlockMu.Lock()
	defer g.producedBlockMu.Unlock()
	delete(g.producedBlockEnds, slot)
}

func (g *payloadPreparationGate) producedBlockPending(referenceSlot, selectedSlot uint64, now time.Time) bool {
	g.producedBlockMu.Lock()
	defer g.producedBlockMu.Unlock()
	for slot, expiry := range g.producedBlockEnds {
		if !now.Before(expiry) {
			delete(g.producedBlockEnds, slot)
			continue
		}
		if selectedSlot < slot && slotsWithinOne(referenceSlot, slot) {
			return true
		}
	}
	return false
}

func slotsWithinOne(first, second uint64) bool {
	return math.AbsoluteDifference(first, second) <= 1
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
		a.logger.Info(
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

func (a *ApiHandler) preparePayloadLoop(ctx context.Context) {
	a.preparePayloadLoopWith(ctx, a.preparePayloadForWithScratch)
}

func (a *ApiHandler) preparePayloadLoopWith(
	ctx context.Context,
	prepare func(context.Context, preparationKey, *payloadPreparationScratch) (preparationKey, error),
) {
	logger := a.logger
	// Polling once per quarter slot gives a newly selected head several chances to trigger
	// preparation. Most non-proposal ticks stop before copying state; a pre-Fulu epoch boundary
	// needs state advancement before the proposer is known.
	slotDuration := time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second
	tick := slotDuration / 4
	if tick <= 0 {
		logger.Warn("PayloadPreparation: disabled because the slot duration is zero")
		return
	}
	payloadAttestationDeadline := time.Duration(a.beaconChainCfg.PayloadAttestationDueMs()) * time.Millisecond
	gloasWindow := slotDuration - payloadAttestationDeadline
	if a.beaconChainCfg.GloasForkEpoch != a.beaconChainCfg.FarFutureEpoch &&
		gloasWindow <= minimumPreparationLead {
		logger.Warn(
			"PayloadPreparation: Gloas preparation window is too short",
			"available", gloasWindow,
			"minimum", minimumPreparationLead,
		)
	}
	// Preparation is silent on a node that rarely proposes, so say once that it is running:
	// otherwise a loop that never started looks exactly like one with nothing to do.
	logger.Info("PayloadPreparation: watching for proposals", "every", tick)
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var lastSettled preparationKey
	var lastFailureLog time.Time
	var lastPendingLog time.Time
	var scratch payloadPreparationScratch
	immediate := true
	for {
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
		immediate = false

		currentSlot := a.ethClock.GetCurrentSlot()
		targetSlot := currentSlot + 1
		scratch.resetForTargetSlot(targetSlot)
		// Head fallback follows the current slot's timing, while payload attributes follow the
		// target slot's fork. The versions can differ at an upgrade boundary.
		currentVersion := a.beaconChainCfg.GetCurrentStateVersion(currentSlot / a.beaconChainCfg.SlotsPerEpoch)
		stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
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
		currentSlotStart := slotStart.Add(-slotDuration)
		lead := time.Until(slotStart)
		if lead > slotDuration || lead <= minimumPreparationLead {
			continue
		}
		selectedRoot, selectedSlot, selected := a.syncedData.SelectedHead()
		if !selected {
			continue
		}
		if a.payloadPreparationGate.producedBlockPending(currentSlot, selectedSlot, time.Now()) {
			continue
		}
		if shouldWaitForCurrentSlotHead(
			currentSlot,
			selectedSlot,
			a.forkchoiceStore.BlockProcessing(),
			time.Now(),
			currentSlotStart,
			attestationDue(a.beaconChainCfg, currentVersion),
		) {
			continue
		}
		// Preparation requires the selected and materialized head identities to match. Otherwise
		// its payload attributes can target stale state.
		if selectedRoot != a.syncedData.HeadRoot() {
			continue
		}
		current := preparationKey{
			targetSlot:          targetSlot,
			headRoot:            selectedRoot,
			validatorGeneration: generation,
		}
		// A pre-Fulu proposer lookup may need an epoch transition. Reuse only the index
		// derived from this same head and target; preferences are still read on every tick.
		if current.targetSlot == lastSettled.targetSlot && current.headRoot == lastSettled.headRoot {
			current.proposerIndex = lastSettled.proposerIndex
		}
		if stateVersion.AfterOrEqual(clparams.GloasVersion) {
			var err error
			current, err = a.precheckPreparationInputs(current)
			if err != nil {
				if !isExpectedPreparationSkip(err) && time.Since(lastFailureLog) >= time.Minute {
					logger.Warn("PayloadPreparation: proposer check failed", "slot", targetSlot, "err", err)
					lastFailureLog = time.Now()
				}
				continue
			}
			selectedVersion := a.beaconChainCfg.GetCurrentStateVersion(selectedSlot / a.beaconChainCfg.SlotsPerEpoch)
			// Only a current-slot Gloas head depends on the current slot's PTC decision. An
			// older selected head has already crossed its payload-decision boundary.
			if selectedSlot == currentSlot &&
				selectedVersion.AfterOrEqual(clparams.GloasVersion) {
				if delay := gloasPayloadDecisionDelay(
					time.Now(),
					currentSlotStart,
					payloadAttestationDeadline,
				); delay > 0 {
					if err := common.Sleep(ctx, delay); err != nil {
						return
					}
					// Re-read the head and payload decision at the deadline. Waiting for the next
					// periodic tick can consume the rest of the preparation window.
					immediate = true
					continue
				}
			}
			if selectedVersion.AfterOrEqual(clparams.GloasVersion) {
				var pathErr error
				current.gloasPath, pathErr = a.resolveGloasPayloadPath(selectedRoot, targetSlot)
				if current.gloasPath == gloasPayloadPathPending {
					if time.Since(lastPendingLog) >= time.Minute {
						logger.Warn("PayloadPreparation: Gloas payload path is still pending", "slot", targetSlot, "head", selectedRoot, "err", pathErr)
						lastPendingLog = time.Now()
					}
					continue
				}
			}
		}
		if current == lastSettled {
			continue
		}
		if gloasPathRequiresForkChoiceUpdate(current.gloasPath) {
			lastSettled = current
			scratch.release()
			continue
		}
		prepareCtx, cancel := context.WithDeadlineCause(
			ctx, slotStart.Add(-minimumPreparationLead), errPreparationTooLate,
		)
		outcome, err := prepare(prepareCtx, current, &scratch)
		cancel()
		if isSettledPreparationOutcome(err) {
			lastSettled = outcome
			scratch.release()
		}
		if err != nil && !isExpectedPreparationSkip(err) && time.Since(lastFailureLog) >= time.Minute {
			logger.Warn("PayloadPreparation: failed", "slot", targetSlot, "err", err)
			lastFailureLog = time.Now()
		}
	}
}

// preparationKey identifies the inputs of a settled attempt. Comparing the effective gas limit
// avoids repeating state work for unrelated preference updates. Attribute presence is part of
// the key: an omitted gas limit and an explicit zero are different build requests.
type preparationKey struct {
	targetSlot          uint64
	headRoot            common.Hash
	validatorGeneration uint64
	gloasPath           gloasPayloadPath
	proposerIndex       uint64
	targetGasLimit      hexutil.Uint64
	targetGasLimitSet   bool
}

func (k *preparationKey) setTargetGasLimit(limit *hexutil.Uint64) {
	k.targetGasLimitSet = limit != nil
	k.targetGasLimit = 0
	if limit != nil {
		k.targetGasLimit = *limit
	}
}

type gloasPayloadPath uint8

const (
	gloasPayloadPathPreFork gloasPayloadPath = iota
	gloasPayloadPathPending
	gloasPayloadPathEmpty
	gloasPayloadPathFull
	gloasPayloadPathReorgToEmpty
)

func (p gloasPayloadPath) String() string {
	switch p {
	case gloasPayloadPathPreFork:
		return "pre-fork"
	case gloasPayloadPathPending:
		return "pending"
	case gloasPayloadPathEmpty:
		return "empty"
	case gloasPayloadPathFull:
		return "full"
	case gloasPayloadPathReorgToEmpty:
		return "full-to-empty"
	default:
		return "unknown"
	}
}

// An older selected head becomes usable after the attestation deadline only when no OnBlock call
// is active or waiting for the store lock, regardless of its slot. A future head is not usable.
func shouldWaitForCurrentSlotHead(
	currentSlot, selectedSlot uint64,
	blockProcessing bool,
	now, currentSlotStart time.Time,
	attestationDeadline time.Duration,
) bool {
	if selectedSlot == currentSlot {
		return false
	}
	if selectedSlot > currentSlot || blockProcessing {
		return true
	}
	return now.Before(currentSlotStart.Add(attestationDeadline))
}

// A Gloas head needs the current slot's PTC decision before its FULL or EMPTY parent is known.
func gloasPayloadDecisionDelay(
	now, currentSlotStart time.Time,
	payloadAttestationDeadline time.Duration,
) time.Duration {
	return max(currentSlotStart.Add(payloadAttestationDeadline).Sub(now), 0)
}

// isExpectedPreparationSkip reports whether there was simply nothing to prepare, as opposed to a
// failure worth reporting.
func isExpectedPreparationSkip(err error) bool {
	return isSettledPreparationOutcome(err) ||
		errors.Is(err, errHeadTooFarBack) ||
		errors.Is(err, errPreparationHeadChanged) ||
		errors.Is(err, errForkChoiceHeadChanged) ||
		errors.Is(err, errBlockWorkInFlight) ||
		errors.Is(err, errGloasPayloadPending) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, synced_data.ErrNotSynced)
}

func isSettledPreparationOutcome(err error) bool {
	return err == nil ||
		errors.Is(err, errNotOurProposal) ||
		errors.Is(err, errNoPayloadID) ||
		errors.Is(err, errPreparationTooLate) ||
		errors.Is(err, errGloasPathNeedsForkChoice)
}

// A direct builder start cannot change the EL head, so these paths must wait for production's FCU.
func gloasPathRequiresForkChoiceUpdate(path gloasPayloadPath) bool {
	return path == gloasPayloadPathFull || path == gloasPayloadPathReorgToEmpty
}

// preparePayloadForWithScratch returns the inputs actually used, keeping sampled values for
// inputs not reached before a skip. A preference update during the build must not change that
// returned key, or the next comparison may miss a required retry.
func (a *ApiHandler) preparePayloadForWithScratch(
	ctx context.Context,
	key preparationKey,
	scratch *payloadPreparationScratch,
) (preparationKey, error) {
	targetSlot := key.targetSlot
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
		key.headRoot = root
		// Beyond the proposer lookahead the index has to be reshuffled from the seed, which is far
		// too costly to repeat every tick on a large validator set.
		if a.proposerLookupTooFarAhead(headState, targetSlot) {
			return errHeadTooFarBack
		}

		// Fulu's proposer lookahead is valid across the next epoch, so reject an unregistered
		// proposer before copying and advancing the full state.
		slotsPerEpoch := a.beaconChainCfg.SlotsPerEpoch
		lookupAfterAdvance = targetSlot/slotsPerEpoch > headState.Slot()/slotsPerEpoch && headState.Version().Before(clparams.FuluVersion)
		var err error
		if !lookupAfterAdvance {
			proposerIndex, feeRecipient, err = a.registeredProposer(headState, targetSlot)
			key.proposerIndex = proposerIndex
			if err != nil {
				return err
			}
		}
		if !a.payloadPreparationGate.idle() {
			return errBlockWorkInFlight
		}
		baseState, err = scratch.copyFrom(headState, a.beaconChainCfg)
		return err
	}); err != nil {
		return key, err
	}

	if err := transition.DefaultMachine.ProcessSlots(baseState, targetSlot); err != nil {
		return key, err
	}
	if lookupAfterAdvance {
		var err error
		proposerIndex, feeRecipient, err = a.registeredProposer(baseState, targetSlot)
		key.proposerIndex = proposerIndex
		if err != nil {
			return key, err
		}
	}

	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
	// State derivation can consume the entry lead, so enforce the same floor again.
	if time.Until(a.ethClock.GetSlotTime(targetSlot)) <= minimumPreparationLead {
		return key, errPreparationTooLate
	}
	targetGasLimit := a.targetGasLimitForProposal(
		baseState, targetSlot, proposerIndex, stateVersion,
	)
	key.setTargetGasLimit(targetGasLimit)
	// The loop-level path is an early filter. Resolve it again after state work because the Gloas
	// decision can change without changing the beacon head root.
	payloadSource, err := a.resolveExecutionPayloadSource(baseState, baseBlockRoot, targetSlot, stateVersion)
	if err != nil {
		return key, err
	}
	key.gloasPath = payloadSource.gloasPath
	if payloadSource.fallbackCause != nil {
		return key, payloadSource.fallbackCause
	}
	if payloadSource.gloasPath == gloasPayloadPathPending {
		return key, errGloasPayloadPending
	}
	if gloasPathRequiresForkChoiceUpdate(payloadSource.gloasPath) {
		return key, errGloasPathNeedsForkChoice
	}
	withdrawalsState, err := withdrawalsStateForExecutionPayloadSource(baseState, payloadSource)
	if err != nil {
		return key, fmt.Errorf("prepare payload: derive withdrawals state: %w", err)
	}
	withdrawals, err := a.expectedWithdrawals(baseState, withdrawalsState, targetSlot)
	if err != nil {
		return key, err
	}
	slotNumber := hexutil.Uint64(targetSlot)
	attrs := a.payloadBuildAttributes(
		baseState, baseBlockRoot, targetSlot, feeRecipient, withdrawals, &slotNumber, targetGasLimit, stateVersion,
	)
	payloadID, err := a.startPayloadBuildForPreparation(ctx, targetSlot, baseBlockRoot, payloadSource.head, attrs)
	if err != nil {
		return key, err
	}
	if len(payloadID) == 0 {
		return key, errNoPayloadID
	}
	selectedRoot, _, selected := a.syncedData.SelectedHead()
	if !selected || selectedRoot != baseBlockRoot {
		return key, errPreparationHeadChanged
	}

	a.preparedPayload.set(targetSlot, payloadID, baseBlockRoot, time.Now())
	a.logger.Info("PayloadPreparation: primed execution layer", "slot", targetSlot, "proposer", proposerIndex, "head", baseBlockRoot)
	return key, nil
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

func (a *ApiHandler) proposerLookupTooFarAhead(beaconState *state.CachingBeaconState, targetSlot uint64) bool {
	slotsPerEpoch := a.beaconChainCfg.SlotsPerEpoch
	return targetSlot/slotsPerEpoch > beaconState.Slot()/slotsPerEpoch+a.beaconChainCfg.MinSeedLookahead
}

func (a *ApiHandler) precheckPreparationInputs(key preparationKey) (preparationKey, error) {
	err := a.syncedData.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, root common.Hash, _ uint64) error {
		if root != key.headRoot {
			return errPreparationHeadChanged
		}
		if a.proposerLookupTooFarAhead(headState, key.targetSlot) {
			return errHeadTooFarBack
		}
		// Before Fulu, finding the proposer may require an epoch transition, so the sampled
		// index is only a hint until preparation checks the advanced state. Fulu's lookahead
		// makes this check possible without copying or advancing the state.
		if headState.Version().AfterOrEqual(clparams.FuluVersion) {
			var err error
			key.proposerIndex, _, err = a.registeredProposer(headState, key.targetSlot)
			if err != nil {
				return err
			}
		}
		key.setTargetGasLimit(a.targetGasLimitForProposal(headState, key.targetSlot, key.proposerIndex, clparams.GloasVersion))
		return nil
	})
	return key, err
}

// startPayloadBuildForPreparation retries only execution-head mismatches and a busy execution module.
// Each attempt uses non-blocking gate acquisition, but the builder-start call is synchronous.
// The gate is released before retry sleeps.
func (a *ApiHandler) startPayloadBuildForPreparation(
	ctx context.Context,
	targetSlot uint64,
	baseBlockRoot common.Hash,
	head common.Hash,
	attrs *engine_types.PayloadAttributes,
) ([]byte, error) {
	payloadBuilder, ok := a.engine.(execution_client.PayloadBuilder)
	if !ok {
		return nil, execution_client.ErrNotSupported
	}
	preparationSlot := targetSlot - 1
	for {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		selectedRoot, selectedSlot, selected := a.syncedData.SelectedHead()
		if !selected || selectedRoot != baseBlockRoot {
			return nil, errPreparationHeadChanged
		}
		payloadID, err := a.startPayloadBuildAttempt(ctx, payloadBuilder, preparationSlot, selectedSlot, head, attrs)
		if err == nil {
			return payloadID, nil
		}
		if !errors.Is(err, execution_client.ErrPayloadBuildHeadMismatch) &&
			!errors.Is(err, chainreader.ErrExecutionBusy) {
			return nil, err
		}
		retryDelay := payloadBuildBusyRetryDelay
		if errors.Is(err, execution_client.ErrPayloadBuildHeadMismatch) {
			retryDelay = payloadBuildHeadMismatchRetryDelay
		}
		if err := common.Sleep(ctx, retryDelay); err != nil {
			return nil, context.Cause(ctx)
		}
	}
}

func (a *ApiHandler) startPayloadBuildAttempt(
	ctx context.Context,
	payloadBuilder execution_client.PayloadBuilder,
	preparationSlot uint64,
	selectedSlot uint64,
	head common.Hash,
	attrs *engine_types.PayloadAttributes,
) ([]byte, error) {
	finishAttempt, ok := a.payloadPreparationGate.tryBeginPreparation(preparationSlot, selectedSlot)
	if !ok {
		return nil, errBlockWorkInFlight
	}
	defer finishAttempt()
	return payloadBuilder.StartPayloadBuild(ctx, head, attrs)
}

// payloadBuildAttributes is shared because production reuses a prepared builder only when every
// attribute is identical.
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

type executionPayloadSource struct {
	head                    common.Hash
	parentExecutionRequests *cltypes.ExecutionRequests
	gloasPath               gloasPayloadPath
	fallbackCause           error
}

func withdrawalsStateForExecutionPayloadSource(
	baseState *state.CachingBeaconState,
	payloadSource executionPayloadSource,
) (*state.CachingBeaconState, error) {
	// A pre-Gloas parent needs a fresh withdrawal sweep after the upgrade. Non-FULL
	// Gloas paths, including genesis, use the cached payload withdrawals instead.
	if payloadSource.gloasPath == gloasPayloadPathPreFork {
		return baseState, nil
	}
	if payloadSource.parentExecutionRequests == nil {
		return nil, nil
	}
	// Applying a FULL parent mutates Gloas withdrawal state. Other production inputs must keep
	// reading the unmodified base state.
	withdrawalsState, err := baseState.Copy()
	if err != nil {
		return nil, fmt.Errorf("copy state for FULL parent payload: %w", err)
	}
	if err := transition.DefaultMachine.ApplyParentExecutionPayload(withdrawalsState, payloadSource.parentExecutionRequests); err != nil {
		return nil, fmt.Errorf("apply FULL parent payload: %w", err)
	}
	return withdrawalsState, nil
}

// resolveExecutionPayloadSource is shared by preparation and production so both choose the same
// execution parent and FULL-parent requests. A changed beacon head is an error: choosing EMPTY
// cannot repair a stale beacon parent. On a matching head, a pending or unreadable FULL path
// returns its EMPTY-parent fallback; preparation waits instead of priming that fallback.
func (a *ApiHandler) resolveExecutionPayloadSource(
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	stateVersion clparams.StateVersion,
) (executionPayloadSource, error) {
	if stateVersion.Before(clparams.GloasVersion) {
		return executionPayloadSource{head: baseState.LatestExecutionPayloadHeader().BlockHash, gloasPath: gloasPayloadPathPreFork}, nil
	}
	path := gloasPayloadPathPreFork
	var pathErr error
	if baseState.GetLatestExecutionPayloadBid() != nil && !a.isPreGloasParent(baseState) {
		path, pathErr = a.resolveGloasPayloadPath(baseBlockRoot, targetSlot)
		if errors.Is(pathErr, errForkChoiceHeadChanged) {
			return executionPayloadSource{}, pathErr
		}
	}
	source := a.executionPayloadSourceForGloasPath(baseState, baseBlockRoot, path)
	if pathErr != nil {
		source.fallbackCause = pathErr
	}
	return source, nil
}

func (a *ApiHandler) executionPayloadSourceForGloasPath(
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	path gloasPayloadPath,
) executionPayloadSource {
	parentBid := baseState.GetLatestExecutionPayloadBid()
	if parentBid == nil {
		return executionPayloadSource{head: baseState.GetLatestBlockHash(), gloasPath: gloasPayloadPathEmpty}
	}
	// Until a FULL parent is applied, latest_block_hash is the execution parent already
	// committed by the state. This also covers the fork transition and Gloas genesis.
	if path != gloasPayloadPathFull {
		return executionPayloadSource{head: baseState.GetLatestBlockHash(), gloasPath: path}
	}
	envelope, err := a.forkchoiceStore.ReadEnvelopeFromDisk(baseBlockRoot)
	if err != nil {
		return executionPayloadSource{
			head:          baseState.GetLatestBlockHash(),
			gloasPath:     gloasPayloadPathPending,
			fallbackCause: fmt.Errorf("read FULL parent payload envelope: %w", err),
		}
	}
	if envelope == nil || envelope.Message == nil || envelope.Message.ExecutionRequests == nil {
		return executionPayloadSource{
			head:          baseState.GetLatestBlockHash(),
			gloasPath:     gloasPayloadPathPending,
			fallbackCause: fmt.Errorf("FULL parent payload has no execution requests for root %x", baseBlockRoot),
		}
	}
	return executionPayloadSource{
		head:                    parentBid.BlockHash,
		parentExecutionRequests: envelope.Message.ExecutionRequests,
		gloasPath:               path,
	}
}

func (a *ApiHandler) isPreGloasParent(baseState *state.CachingBeaconState) bool {
	parentSlot := baseState.LatestBlockHeader().Slot
	// Use the parent's fork, not the advanced state's version. A Gloas-at-genesis parent
	// follows the EMPTY path; it has no executed bid payload or fresh withdrawal sweep.
	return a.beaconChainCfg.GetCurrentStateVersion(parentSlot / a.beaconChainCfg.SlotsPerEpoch).Before(clparams.GloasVersion)
}

func (a *ApiHandler) resolveGloasPayloadPath(baseBlockRoot common.Hash, targetSlot uint64) (gloasPayloadPath, error) {
	head, _, err := a.forkchoiceStore.GetHeadNode()
	if err != nil {
		return gloasPayloadPathPending, fmt.Errorf("%w: resolve fork choice head: %w", errGloasPayloadPending, err)
	}
	if head.Root != baseBlockRoot {
		return gloasPayloadPathPending, fmt.Errorf("%w: proposal parent %s, current head %s", errForkChoiceHeadChanged, baseBlockRoot, head.Root)
	}
	return a.gloasPayloadPathForHead(head, targetSlot), nil
}

func (a *ApiHandler) gloasPayloadPathForHead(head forkchoice.ForkChoiceNode, targetSlot uint64) gloasPayloadPath {
	switch head.PayloadStatus {
	case cltypes.PayloadStatusPending:
		return gloasPayloadPathPending
	case cltypes.PayloadStatusEmpty:
		return gloasPayloadPathEmpty
	case cltypes.PayloadStatusFull:
		if !a.forkchoiceStore.ShouldBuildOnFull(head, targetSlot) {
			return gloasPayloadPathReorgToEmpty
		}
		if !a.forkchoiceStore.HasEnvelope(head.Root) {
			return gloasPayloadPathPending
		}
		return gloasPayloadPathFull
	default:
		return gloasPayloadPathPending
	}
}

// targetGasLimitForProposal uses the matching proposer's preference or the parent's gas limit.
// stateVersion is the target fork; the head state may still be from before Gloas.
func (a *ApiHandler) targetGasLimitForProposal(
	baseState *state.CachingBeaconState,
	targetSlot, proposerIndex uint64,
	stateVersion clparams.StateVersion,
) *hexutil.Uint64 {
	if stateVersion.Before(clparams.GloasVersion) {
		return nil
	}
	var targetGasLimit *hexutil.Uint64
	if baseState.Version().Before(clparams.GloasVersion) {
		// The Gloas upgrade carries this limit into the parent bid.
		gasLimit := hexutil.Uint64(baseState.LatestExecutionPayloadHeader().GasLimit)
		targetGasLimit = &gasLimit
	} else if parentBid := baseState.GetLatestExecutionPayloadBid(); parentBid != nil {
		gasLimit := hexutil.Uint64(parentBid.GasLimit)
		targetGasLimit = &gasLimit
	}
	if a.epbsPool == nil {
		return targetGasLimit
	}
	proposalEpoch := state.GetEpochAtSlot(a.beaconChainCfg, targetSlot)
	dependentRoot, err := state.GetProposerDependentRoot(baseState, proposalEpoch)
	if err != nil {
		log.Trace("Skipping proposer preferences target gas limit", "slot", targetSlot, "err", err)
		return targetGasLimit
	}
	preference, ok := a.epbsPool.GetPreference(targetSlot, dependentRoot)
	if !ok || preference == nil || preference.Message == nil || preference.Message.ValidatorIndex != proposerIndex {
		return targetGasLimit
	}
	gasLimit := hexutil.Uint64(preference.Message.TargetGasLimit)
	return &gasLimit
}
