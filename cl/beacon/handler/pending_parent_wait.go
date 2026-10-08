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
	// must not wait any longer, and the budget of the first retry, which always runs.
	gloasPendingParentMaxWaitDivisor     = 8
	gloasPendingParentSlotCutoffDivisor  = 6
	gloasPendingParentRetryBudgetDivisor = 24
	gloasPendingParentPollInterval       = 100 * time.Millisecond
)

// gloasPendingParentDeadline bounds how long a proposal may wait for the parent payload
// decision: at most the wait budget, and never past the in-slot cutoff.
func gloasPendingParentDeadline(now, slotStart time.Time, slotDuration time.Duration) time.Time {
	deadline := now.Add(slotDuration / gloasPendingParentMaxWaitDivisor)
	if cutoff := slotStart.Add(slotDuration / gloasPendingParentSlotCutoffDivisor); cutoff.Before(deadline) {
		deadline = cutoff
	}
	if deadline.Before(now) {
		return now
	}
	return deadline
}

// awaitGloasPayloadSource re-resolves the payload source until it is no longer pending or the
// deadline passes. A resolve error keeps the last successfully resolved source.
func awaitGloasPayloadSource(
	ctx context.Context,
	deadline time.Time,
	interval time.Duration,
	last executionPayloadSource,
	resolve func() (executionPayloadSource, error),
) executionPayloadSource {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	cutoff := time.NewTimer(time.Until(deadline))
	defer cutoff.Stop()
	for {
		source, err := resolve()
		if err != nil {
			return last
		}
		last = source
		if source.gloasPath != gloasPayloadPathPending || !time.Now().Before(deadline) {
			return source
		}
		select {
		case <-ctx.Done():
			return source
		case <-cutoff.C:
			return source
		case <-ticker.C:
		}
	}
}

// awaitPendingParentPayload gives the parent's payload a bounded chance to be applied before
// the proposal falls back to the EMPTY parent. A pending envelope is only re-applied by an
// explicit retry, so each poll triggers one; the first retry always runs, with its own small
// budget, even when production starts after the cutoff.
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
	retryBudget := max(time.Until(deadline), slotDuration/gloasPendingParentRetryBudgetDivisor)
	retryCtx, cancelRetry := context.WithTimeout(ctx, retryBudget)
	defer cancelRetry()
	a.logger.Info("BlockProduction: waiting for parent payload decision", "slot", targetSlot, "head", baseBlockRoot, "budget", time.Until(deadline).Round(time.Millisecond))
	return awaitGloasPayloadSource(ctx, deadline, gloasPendingParentPollInterval, current, func() (executionPayloadSource, error) {
		if retryCtx.Err() == nil {
			a.forkchoiceStore.RetryPendingExecutionPayloadEnvelope(retryCtx, baseBlockRoot)
		}
		source, err := a.resolveExecutionPayloadSource(baseState, baseBlockRoot, targetSlot, stateVersion)
		if err != nil {
			// The proposal keeps its parent, as it would without the wait; a head change
			// here is visible but not fatal.
			a.logger.Warn("BlockProduction: parent payload wait stopped", "slot", targetSlot, "head", baseBlockRoot, "err", err)
		}
		return source, err
	})
}
