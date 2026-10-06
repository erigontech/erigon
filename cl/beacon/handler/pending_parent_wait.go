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
	// Fractions of the slot: the wait budget and the point in the slot after which a proposal
	// must not wait any longer, so the block still reaches attesters in time.
	gloasPendingParentMaxWaitDivisor    = 8
	gloasPendingParentSlotCutoffDivisor = 6
	gloasPendingParentPollInterval      = 100 * time.Millisecond
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

// awaitGloasPayloadSource re-resolves the payload source until it is no longer pending or
// the deadline passes, returning the last resolved source.
func awaitGloasPayloadSource(
	ctx context.Context,
	deadline time.Time,
	interval time.Duration,
	resolve func() (executionPayloadSource, error),
) (executionPayloadSource, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		source, err := resolve()
		if err != nil {
			return source, err
		}
		if source.gloasPath != gloasPayloadPathPending || !time.Now().Before(deadline) {
			return source, nil
		}
		select {
		case <-ctx.Done():
			return source, ctx.Err()
		case <-ticker.C:
		}
	}
}

// awaitPendingParentPayload gives the parent's payload a bounded chance to be applied before
// the proposal falls back to the EMPTY parent. A pending envelope is only re-applied by an
// explicit retry, so each poll triggers one.
func (a *ApiHandler) awaitPendingParentPayload(
	ctx context.Context,
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	stateVersion clparams.StateVersion,
) (executionPayloadSource, error) {
	slotDuration := time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second
	deadline := gloasPendingParentDeadline(time.Now(), a.ethClock.GetSlotTime(targetSlot), slotDuration)
	a.logger.Info("BlockProduction: waiting for parent payload decision", "slot", targetSlot, "head", baseBlockRoot, "budget", time.Until(deadline).Round(time.Millisecond))
	return awaitGloasPayloadSource(ctx, deadline, gloasPendingParentPollInterval, func() (executionPayloadSource, error) {
		a.forkchoiceStore.RetryPendingExecutionPayloadEnvelope(ctx, baseBlockRoot)
		return a.resolveExecutionPayloadSource(baseState, baseBlockRoot, targetSlot, stateVersion)
	})
}
