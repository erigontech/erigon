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
	return a.awaitPendingParentPayload(ctx, baseState, baseBlockRoot, targetSlot, stateVersion, source), nil
}

// awaitGloasPayloadSource re-resolves the payload source until it is decided or the deadline
// passes. The retry runs on its own goroutine, one at a time, so a retry that blocks cannot
// hold the wait past the cutoff; the source is resolved on the caller's goroutine, which the
// proposal needs after the wait in any case. A resolve error keeps the last resolved source.
func awaitGloasPayloadSource(
	ctx context.Context,
	deadline time.Time,
	interval time.Duration,
	last executionPayloadSource,
	retry func(),
	resolve func() (executionPayloadSource, error),
) executionPayloadSource {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	cutoff := time.NewTimer(time.Until(deadline))
	defer cutoff.Stop()
	var retryDone chan struct{}
	startRetry := func() {
		retryDone = make(chan struct{})
		go func(done chan struct{}) {
			defer close(done)
			retry()
		}(retryDone)
	}
	// A decision that landed during the last poll window is still picked up.
	resolveLast := func() executionPayloadSource {
		if source, err := resolve(); err == nil {
			return source
		}
		return last
	}
	startRetry()
	for {
		select {
		case <-retryDone:
			retryDone = nil
		case <-ticker.C:
			if retryDone == nil {
				startRetry()
			}
		case <-ctx.Done():
			return resolveLast()
		case <-cutoff.C:
			return resolveLast()
		}
		source, err := resolve()
		if err != nil {
			return last
		}
		last = source
		if !last.envelopeParked || !time.Now().Before(deadline) {
			return last
		}
	}
}

// awaitPendingParentPayload gives the parent's payload a bounded chance to be applied before
// the proposal falls back to the EMPTY parent. A pending envelope is only re-applied by an
// explicit retry, so the wait keeps one running; each retry owns a context that outlives the
// deadline by one retry budget, so one in flight at the cutoff is not cut short.
func (a *ApiHandler) awaitPendingParentPayload(
	ctx context.Context,
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	stateVersion clparams.StateVersion,
	current executionPayloadSource,
) executionPayloadSource {
	slotDuration := time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second
	deadline := gloasPendingParentDeadline(time.Now(), a.ethClock.GetSlotTime(targetSlot), slotDuration)
	a.logger.Info("BlockProduction: waiting for parent payload decision", "slot", targetSlot, "head", baseBlockRoot, "budget", time.Until(deadline).Round(time.Millisecond))
	retry := func() {
		budget := max(time.Until(deadline), 0) + slotDuration/gloasPendingParentRetryBudgetDivisor
		retryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
		defer cancel()
		a.forkchoiceStore.RetryPendingExecutionPayloadEnvelope(retryCtx, baseBlockRoot)
	}
	return awaitGloasPayloadSource(ctx, deadline, gloasPendingParentPollInterval, current, retry, func() (executionPayloadSource, error) {
		source, err := a.resolveExecutionPayloadSource(baseState, baseBlockRoot, targetSlot, stateVersion)
		if err != nil {
			// The proposal keeps its parent, as it would without the wait; a head change
			// here is visible but not fatal.
			a.logger.Warn("BlockProduction: parent payload wait stopped", "slot", targetSlot, "head", baseBlockRoot, "err", err)
		}
		return source, err
	})
}
