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

// proposalWaitPollInterval paces the re-checks of a proposal wait: whether the parked envelope was
// applied, whether the head moved and whether the new head state was published. Each is a lookup,
// so the interval only bounds how late the wait notices a change.
const proposalWaitPollInterval = 100 * time.Millisecond

// proposalWaitCutoff is the point in the slot after which a proposal no longer waits: one
// publication margin before the payload collection deadline, so the payload built after the wait
// still has that margin.
func proposalWaitCutoff(slotStart time.Time, cfg *clparams.BeaconChainConfig, stateVersion clparams.StateVersion) time.Time {
	due := attestationDue(cfg, stateVersion)
	return slotStart.Add(preparedGrabOffset(due) - payloadPublicationMargin(due))
}

// awaitParkedEnvelope polls parked until it reports false or the deadline passes. A head change
// seen on any poll ends the wait at once. retry runs on its own goroutine until the wait returns,
// so a blocked apply cannot hold the wait past the deadline.
func awaitParkedEnvelope(
	ctx context.Context,
	deadline time.Time,
	interval time.Duration,
	parked func() bool,
	headErr func() error,
	retry func(stop <-chan struct{}),
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	cutoff := time.NewTimer(time.Until(deadline))
	defer cutoff.Stop()
	stop := make(chan struct{})
	defer close(stop)
	go retry(stop)
	for parked() {
		if err := headErr(); err != nil {
			return err
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-cutoff.C:
			return nil
		}
	}
	return nil
}

// retryParkedEnvelope retries the envelope parked for root until stop closes, at the pace fork
// choice sets per root. While nothing is parked it keeps polling, because an apply in flight can
// still park the envelope. An apply in flight when stop closes runs on until the payload
// collection deadline, so ending the wait does not waste its EL call.
func (a *ApiHandler) retryParkedEnvelope(ctx context.Context, root common.Hash, collectBy time.Time, stop <-chan struct{}) {
	applyCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), collectBy)
	defer cancel()
	for {
		select {
		case <-stop:
			return
		default:
		}
		next := a.forkchoiceStore.RetryPendingExecutionPayloadEnvelope(applyCtx, root)
		if applyCtx.Err() != nil {
			return
		}
		if next.IsZero() {
			next = time.Now().Add(proposalWaitPollInterval)
		}
		// next may be another caller's booking, which is dropped if its attempt is cut short, so check
		// again at the poll interval at the latest.
		timer := time.NewTimer(min(time.Until(next), proposalWaitPollInterval))
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// awaitPendingParentPayload gives an envelope parked for the EMPTY head a bounded chance to be
// applied before the proposal builds on the EMPTY parent, and retries it meanwhile.
func (a *ApiHandler) awaitPendingParentPayload(
	ctx context.Context,
	baseState *state.CachingBeaconState,
	baseBlockRoot common.Hash,
	targetSlot uint64,
	stateVersion clparams.StateVersion,
) error {
	if stateVersion.Before(clparams.GloasVersion) {
		return nil
	}
	parked, err := a.parentEnvelopeParked(baseState, baseBlockRoot, targetSlot)
	if err != nil || !parked || !a.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(targetSlot) {
		return err
	}
	if baseState.LatestBlockHeader().Slot+1 == targetSlot && a.forkchoiceStore.PayloadVotesRuleOutFull(baseBlockRoot) {
		return nil
	}
	slotStart := a.ethClock.GetSlotTime(targetSlot)
	deadline := proposalWaitCutoff(slotStart, a.beaconChainCfg, stateVersion)
	if !time.Now().Before(deadline) {
		return nil
	}
	a.logger.Info("BlockProduction: waiting for parent payload decision", "slot", targetSlot, "head", baseBlockRoot, "budget", time.Until(deadline).Round(time.Millisecond))
	collectBy := slotStart.Add(preparedGrabOffset(attestationDue(a.beaconChainCfg, stateVersion)))
	return awaitParkedEnvelope(ctx, deadline, proposalWaitPollInterval,
		func() bool { return a.envelopeParked(baseBlockRoot) },
		func() error {
			_, err := a.resolveProposalHead(baseBlockRoot)
			return err
		},
		func(stop <-chan struct{}) { a.retryParkedEnvelope(ctx, baseBlockRoot, collectBy, stop) },
	)
}

// awaitHeadStateAfterChange waits until the published head state is the state of the fork-choice
// head, so production retried after a head change builds on that head instead of the state it
// just failed on. It gives up at the proposal wait cutoff, and does not wait for a slot that is
// not current.
func (a *ApiHandler) awaitHeadStateAfterChange(ctx context.Context, targetSlot uint64) bool {
	if !a.ethClock.IsSlotCurrentSlotWithMaximumClockDisparity(targetSlot) {
		return false
	}
	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
	deadline := proposalWaitCutoff(a.ethClock.GetSlotTime(targetSlot), a.beaconChainCfg, stateVersion)
	cutoff := time.NewTimer(time.Until(deadline))
	defer cutoff.Stop()
	ticker := time.NewTicker(proposalWaitPollInterval)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if root, _, ok := a.syncedData.StateHead(); ok {
			if head, _, err := a.forkchoiceStore.GetHeadNode(); err == nil && head.Root == root {
				return true
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return false
		case <-cutoff.C:
			return false
		}
	}
	return false
}
