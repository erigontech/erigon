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
	"sync/atomic"
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
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/types"
)

var (
	errNotOurProposal         = errors.New("next slot is not proposed by a registered validator")
	errNoPayloadID            = errors.New("execution layer returned no payload id")
	errHeadTooFarBack         = errors.New("head state is too far behind the slot to prepare")
	errPreparationHeadChanged = errors.New("selected head changed while preparing payload")
	errBlockWorkInFlight      = errors.New("block production, publication, or adoption is in progress")
	errPreparationTooLate     = errors.New("slot is too close to prime a payload production would use")
	errGloasPayloadPending    = errors.New("gloas parent payload decision is not ready")
	errGloasReorgToEmpty      = errors.New("gloas EMPTY path cannot be primed from a FULL execution head")
)

// preparedPayloadRetainSlots keeps a primed record alive past the slot it was primed for, so
// priming the next slot cannot evict the record for a proposal that is still being produced.
const preparedPayloadRetainSlots = 2

// Leave enough time for the state copy and one builder-start attempt. Starting later is likely to
// overlap production without giving the builder useful warmup.
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

// payloadPreparationGate gives real block work priority over speculative builder startup.
// Production, publication, and adoption hold the shared side. Preparation checks that the gate is
// idle before state work, then briefly takes the exclusive side only for StartPayloadBuild.
// latestProducedSlot covers the signing interval when neither HTTP request holds the gate.
type payloadPreparationGate struct {
	blockWork          sync.RWMutex
	latestProducedSlot atomic.Uint64
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

func (g *payloadPreparationGate) tryBeginPreparation() (func(), bool) {
	if !g.blockWork.TryLock() {
		return nil, false
	}
	return sync.OnceFunc(g.blockWork.Unlock), true
}

func (g *payloadPreparationGate) noteProducedBlock(currentSlot, producedSlot uint64) {
	// The signing interval can cross one slot boundary, but no more.
	if !slotsWithinOne(currentSlot, producedSlot) {
		return
	}
	for {
		previous := g.latestProducedSlot.Load()
		if previous >= producedSlot || g.latestProducedSlot.CompareAndSwap(previous, producedSlot) {
			return
		}
	}
}

func (g *payloadPreparationGate) producedBlockPending(currentSlot, selectedSlot uint64) bool {
	producedSlot := g.latestProducedSlot.Load()
	return selectedSlot < producedSlot && slotsWithinOne(currentSlot, producedSlot)
}

func slotsWithinOne(first, second uint64) bool {
	if first > second {
		return first-second <= 1
	}
	return second-first <= 1
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
	logger := a.logger
	// Polling once per quarter slot gives a newly selected head several chances to trigger
	// preparation. Most non-proposal ticks stop before copying state; a pre-Fulu epoch boundary
	// needs state advancement before the proposer is known.
	tick := time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second / 4
	if tick <= 0 {
		logger.Warn("PayloadPreparation: disabled because the slot duration is zero")
		return
	}
	gloasWindow := time.Duration(a.beaconChainCfg.SecondsPerSlot)*time.Second -
		time.Duration(a.beaconChainCfg.PayloadAttestationDueMs())*time.Millisecond
	if gloasWindow <= minimumPreparationLead {
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
		currentSlotStart := slotStart.Add(-time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second)
		lead := time.Until(slotStart)
		if lead > maxPreparationLead(a.beaconChainCfg) || lead <= minimumPreparationLead {
			continue
		}
		selectedRoot, selectedSlot, selected := a.syncedData.SelectedHead()
		if !selected {
			continue
		}
		if a.payloadPreparationGate.producedBlockPending(currentSlot, selectedSlot) {
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
		var gloasPath gloasPayloadPath
		var proposerPreferencesGeneration uint64
		if stateVersion.AfterOrEqual(clparams.GloasVersion) {
			selectedVersion := a.beaconChainCfg.GetCurrentStateVersion(selectedSlot / a.beaconChainCfg.SlotsPerEpoch)
			if currentVersion.AfterOrEqual(clparams.GloasVersion) && selectedVersion.AfterOrEqual(clparams.GloasVersion) {
				if delay := gloasPayloadDecisionDelay(
					time.Now(),
					currentSlotStart,
					time.Duration(a.beaconChainCfg.PayloadAttestationDueMs())*time.Millisecond,
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
				gloasPath = a.resolveGloasPayloadPath(selectedRoot, targetSlot)
				if gloasPath == gloasPayloadPathPending {
					continue
				}
			}
			if a.epbsPool != nil {
				proposerPreferencesGeneration = a.epbsPool.ProposerPreferencesGeneration(targetSlot)
			}
		}
		current := preparationKey{
			targetSlot:                    targetSlot,
			headRoot:                      selectedRoot,
			validatorGeneration:           generation,
			gloasPath:                     gloasPath,
			proposerPreferencesGeneration: proposerPreferencesGeneration,
		}
		if current == lastSettled {
			continue
		}
		// Direct preparation avoids fork choice, so it cannot move a FULL execution head back
		// to its EMPTY parent.
		if current.gloasPath == gloasPayloadPathReorgToEmpty {
			lastSettled = current
			scratch.release()
			continue
		}
		prepareCtx, cancel := context.WithDeadlineCause(
			ctx, slotStart.Add(-minimumPreparationLead), errPreparationTooLate,
		)
		head, err := a.preparePayloadForWithScratch(prepareCtx, targetSlot, &scratch)
		cancel()
		outcome := current
		outcome.headRoot = head
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

// preparationKey invalidates a settled result whenever an input that can change the build changes.
type preparationKey struct {
	targetSlot                    uint64
	headRoot                      common.Hash
	validatorGeneration           uint64
	gloasPath                     gloasPayloadPath
	proposerPreferencesGeneration uint64
}

type gloasPayloadPath uint8

const (
	gloasPayloadPathPreFork gloasPayloadPath = iota
	gloasPayloadPathPending
	gloasPayloadPathEmpty
	gloasPayloadPathFull
	gloasPayloadPathReorgToEmpty
)

// maxPreparationLead bounds how far ahead of a slot priming is worthwhile. One slot is all a live
// chain ever offers, since preparation only ever targets the slot after the current one.
func maxPreparationLead(cfg *clparams.BeaconChainConfig) time.Duration {
	return time.Duration(cfg.SecondsPerSlot) * time.Second
}

// An older selected head becomes usable only after the attestation deadline when no current-slot
// block is still being processed. A future head is invalid.
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
	return errors.Is(err, errNotOurProposal) ||
		errors.Is(err, errNoPayloadID) ||
		errors.Is(err, errHeadTooFarBack) ||
		errors.Is(err, errPreparationHeadChanged) ||
		errors.Is(err, errBlockWorkInFlight) ||
		errors.Is(err, errPreparationTooLate) ||
		errors.Is(err, errGloasPayloadPending) ||
		errors.Is(err, errGloasReorgToEmpty) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, synced_data.ErrNotSynced)
}

func isSettledPreparationOutcome(err error) bool {
	return err == nil ||
		errors.Is(err, errNotOurProposal) ||
		errors.Is(err, errNoPayloadID) ||
		errors.Is(err, errPreparationTooLate) ||
		errors.Is(err, errGloasReorgToEmpty)
}

func (a *ApiHandler) preparePayloadForWithScratch(
	ctx context.Context,
	targetSlot uint64,
	scratch *payloadPreparationScratch,
) (common.Hash, error) {
	if time.Until(a.ethClock.GetSlotTime(targetSlot)) <= minimumPreparationLead {
		return common.Hash{}, errPreparationTooLate
	}
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
			return errBlockWorkInFlight
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
	// State derivation can consume most of the useful lead. Apply the same floor again so a late
	// builder does not overlap production without providing meaningful warmup.
	if time.Until(a.ethClock.GetSlotTime(targetSlot)) <= minimumPreparationLead {
		return baseBlockRoot, errPreparationTooLate
	}
	targetGasLimit := a.targetGasLimitForProposal(baseState, targetSlot, proposerIndex, stateVersion)
	payloadSource, err := a.resolveExecutionPayloadSource(baseState, baseBlockRoot, targetSlot, stateVersion)
	if err != nil {
		return baseBlockRoot, err
	}
	if payloadSource.gloasPath == gloasPayloadPathReorgToEmpty {
		return baseBlockRoot, errGloasReorgToEmpty
	}
	var withdrawalsState *state.CachingBeaconState
	if payloadSource.parentExecutionRequests != nil {
		// The scratch state is private to preparation, so the FULL parent can be applied in place.
		if err := applyParentExecutionPayload(baseState, payloadSource.parentExecutionRequests); err != nil {
			return baseBlockRoot, fmt.Errorf("prepare payload: apply parent execution payload: %w", err)
		}
		withdrawalsState = baseState
	}
	withdrawals, err := a.expectedWithdrawals(baseState, withdrawalsState, stateVersion, targetSlot)
	if err != nil {
		return baseBlockRoot, err
	}
	slotNumber := hexutil.Uint64(targetSlot)
	attrs := a.payloadBuildAttributes(
		baseState, baseBlockRoot, targetSlot, feeRecipient, withdrawals, &slotNumber, targetGasLimit, stateVersion,
	)
	payloadID, err := a.startPayloadBuildForPreparation(ctx, baseBlockRoot, payloadSource.head, attrs)
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

	a.preparedPayload.set(targetSlot, payloadID, baseBlockRoot, time.Now())
	a.logger.Info("PayloadPreparation: primed execution layer", "slot", targetSlot, "proposer", proposerIndex, "head", baseBlockRoot)
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
		payloadID, err := a.startPayloadBuildAttempt(ctx, payloadBuilder, head, attrs)
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
	head common.Hash,
	attrs *engine_types.PayloadAttributes,
) ([]byte, error) {
	finishAttempt, ok := a.payloadPreparationGate.tryBeginPreparation()
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
}

// resolveExecutionPayloadSource is shared by preparation and production so both choose the same
// execution parent and FULL-parent requests.
func (a *ApiHandler) resolveExecutionPayloadSource(
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	stateVersion clparams.StateVersion,
) (executionPayloadSource, error) {
	if stateVersion.Before(clparams.GloasVersion) {
		return executionPayloadSource{head: baseState.LatestExecutionPayloadHeader().BlockHash}, nil
	}
	parentBid := baseState.GetLatestExecutionPayloadBid()
	if parentBid == nil {
		return executionPayloadSource{head: baseState.GetLatestBlockHash(), gloasPath: gloasPayloadPathEmpty}, nil
	}
	if parentBid.ParentBlockHash == (common.Hash{}) && parentBid.Slot == 0 {
		return executionPayloadSource{head: parentBid.BlockHash, gloasPath: gloasPayloadPathPreFork}, nil
	}

	path := a.resolveGloasPayloadPath(baseBlockRoot, targetSlot)
	if path == gloasPayloadPathPending {
		return executionPayloadSource{}, errGloasPayloadPending
	}
	if path != gloasPayloadPathFull {
		return executionPayloadSource{head: parentBid.ParentBlockHash, gloasPath: path}, nil
	}
	envelope, err := a.forkchoiceStore.ReadEnvelopeFromDisk(baseBlockRoot)
	if err != nil {
		return executionPayloadSource{}, fmt.Errorf("read FULL parent payload envelope: %w", err)
	}
	if envelope == nil || envelope.Message == nil || envelope.Message.ExecutionRequests == nil {
		return executionPayloadSource{}, fmt.Errorf("FULL parent payload has no execution requests for root %x", baseBlockRoot)
	}
	return executionPayloadSource{
		head:                    parentBid.BlockHash,
		parentExecutionRequests: envelope.Message.ExecutionRequests,
		gloasPath:               path,
	}, nil
}

func (a *ApiHandler) resolveGloasPayloadPath(baseBlockRoot common.Hash, targetSlot uint64) gloasPayloadPath {
	status, matchesHead := a.forkchoiceStore.GetHeadPayloadStatus(baseBlockRoot)
	if !matchesHead {
		return gloasPayloadPathPending
	}
	switch status {
	case cltypes.PayloadStatusPending:
		return gloasPayloadPathPending
	case cltypes.PayloadStatusEmpty:
		return gloasPayloadPathEmpty
	case cltypes.PayloadStatusFull:
		head := forkchoice.ForkChoiceNode{Root: baseBlockRoot, PayloadStatus: status}
		if !a.forkchoiceStore.ShouldBuildOnFull(head, targetSlot) {
			return gloasPayloadPathReorgToEmpty
		}
		if !a.forkchoiceStore.HasEnvelope(baseBlockRoot) {
			return gloasPayloadPathPending
		}
		return gloasPayloadPathFull
	default:
		return gloasPayloadPathPending
	}
}

func applyParentExecutionPayload(beaconState *state.CachingBeaconState, requests *cltypes.ExecutionRequests) error {
	return transition.DefaultMachine.ApplyParentExecutionPayload(beaconState, requests)
}

func (a *ApiHandler) targetGasLimitForProposal(
	baseState *state.CachingBeaconState,
	targetSlot, proposerIndex uint64,
	stateVersion clparams.StateVersion,
) *hexutil.Uint64 {
	if stateVersion.Before(clparams.GloasVersion) {
		return nil
	}
	var targetGasLimit *hexutil.Uint64
	if parentBid := baseState.GetLatestExecutionPayloadBid(); parentBid != nil {
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
	if !ok || preference.Message == nil || preference.Message.ValidatorIndex != proposerIndex {
		return targetGasLimit
	}
	gasLimit := hexutil.Uint64(preference.Message.TargetGasLimit)
	return &gasLimit
}
