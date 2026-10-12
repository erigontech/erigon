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
	"context"
	"errors"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

const (
	// Fractions of the slot: the wait budget, the point in the slot after which a proposal
	// must not wait any longer, and the budget of one retry, which the wait always allows.
	gloasPendingParentMaxWaitDivisor     = 8
	gloasPendingParentSlotCutoffDivisor  = 6
	gloasPendingParentRetryBudgetDivisor = 24
	gloasPendingParentPollInterval       = 100 * time.Millisecond
)

// gloasPendingParentDeadline bounds how long a proposal may wait for the parent payload
// decision: at most the wait budget and not past the in-slot cutoff, but always long enough
// for one retry, so a request that arrives near or after the cutoff waits past it.
func gloasPendingParentDeadline(now, slotStart time.Time, slotDuration time.Duration) time.Time {
	deadline := now.Add(slotDuration / gloasPendingParentMaxWaitDivisor)
	if cutoff := slotStart.Add(slotDuration / gloasPendingParentSlotCutoffDivisor); cutoff.Before(deadline) {
		deadline = cutoff
	}
	if floor := now.Add(slotDuration / gloasPendingParentRetryBudgetDivisor); deadline.Before(floor) {
		return floor
	}
	return deadline
}

// resolveProductionPayloadSource resolves the payload source for a proposal and, when the EMPTY
// head has a parked envelope, waits for its decision.
func (a *ApiHandler) resolveProductionPayloadSource(ctx context.Context, baseState *state.CachingBeaconState, baseBlockRoot common.Hash, targetSlot uint64, stateVersion clparams.StateVersion) (executionPayloadSource, error) {
	source, err := a.resolveExecutionPayloadSource(baseState, baseBlockRoot, targetSlot, stateVersion)
	if err != nil || !source.envelopeParked {
		return source, err
	}
	return a.awaitPendingParentPayload(ctx, baseState, baseBlockRoot, targetSlot, stateVersion, source)
}

// awaitGloasPayloadSource polls parked until it reports false or the deadline passes, then
// resolves the source once, so a resolve racing an envelope commit cannot end the wait early.
// The retry runs on its own goroutine, so a blocked retry cannot hold the wait past the cutoff.
// A head change is returned as an error; any other resolve error keeps the last source.
func awaitGloasPayloadSource(
	ctx context.Context,
	deadline time.Time,
	interval time.Duration,
	last executionPayloadSource,
	parked func() bool,
	retry func(),
	resolve func() (executionPayloadSource, error),
) (executionPayloadSource, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	cutoff := time.NewTimer(time.Until(deadline))
	defer cutoff.Stop()
	retryDone := make(chan struct{})
	go func() {
		defer close(retryDone)
		retry()
	}()
wait:
	for parked() {
		select {
		case <-retryDone:
			retryDone = nil
		case <-ticker.C:
		case <-ctx.Done():
			break wait
		case <-cutoff.C:
			break wait
		}
	}
	source, err := resolve()
	if errors.Is(err, errForkChoiceHeadChanged) {
		return last, err
	}
	if err != nil {
		return last, nil
	}
	return source, nil
}

// awaitPendingParentPayload gives the parent's payload a bounded chance to be applied before
// the proposal falls back to the EMPTY parent. A pending envelope is only re-applied by an
// explicit retry, so the wait starts one; it owns a context that outlives the deadline by one
// retry budget, so a retry in flight at the cutoff is not cut short.
func (a *ApiHandler) awaitPendingParentPayload(
	ctx context.Context,
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	stateVersion clparams.StateVersion,
	current executionPayloadSource,
) (executionPayloadSource, error) {
	slotDuration := time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second
	deadline := gloasPendingParentDeadline(time.Now(), a.ethClock.GetSlotTime(targetSlot), slotDuration)
	a.logger.Info("BlockProduction: waiting for parent payload decision", "slot", targetSlot, "head", baseBlockRoot, "budget", time.Until(deadline).Round(time.Millisecond))
	retry := func() {
		budget := max(time.Until(deadline), 0) + slotDuration/gloasPendingParentRetryBudgetDivisor
		retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
		defer cancel()
		a.forkchoiceStore.RetryPendingExecutionPayloadEnvelope(retryCtx, baseBlockRoot)
	}
	parked := func() bool { return a.envelopeParked(baseBlockRoot) }
	return awaitGloasPayloadSource(ctx, deadline, gloasPendingParentPollInterval, current, parked, retry, func() (executionPayloadSource, error) {
		source, err := a.resolveExecutionPayloadSource(baseState, baseBlockRoot, targetSlot, stateVersion)
		if err != nil {
			a.logger.Warn("BlockProduction: parent payload wait stopped", "slot", targetSlot, "head", baseBlockRoot, "err", err)
		}
		return source, err
	})
}
