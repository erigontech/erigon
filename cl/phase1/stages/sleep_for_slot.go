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

package stages

import (
	"context"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

// A head change only has to reach the synced head before the next slot, seconds after the attestation and payload
// deadlines that cause it. A poll that finds the head cache invalidated recomputes the head under the fork choice lock,
// so the polls add at most five such recomputations a second. A column wake reads the head only when a parked envelope
// can be attempted, which an attempt booking limits to once a second per root; a failed read mutes column wakes until
// the next poll.
const sleepForSlotHeadPollInterval = 200 * time.Millisecond

type sleepForSlotForkChoice interface {
	FastConfirmationChanged() <-chan struct{}
	GetHeadNode() (forkchoice.ForkChoiceNode, uint64, error)
	GetExecutionHead(forkchoice.ForkChoiceNode) (common.Hash, bool)
	RetryDataAvailablePendingExecutionPayloadEnvelopes(ctx context.Context, minSlot uint64)
	PendingEnvelopeRetryWake() <-chan struct{}
	HasAttemptablePendingEnvelope(minSlot uint64) bool
	WakePendingEnvelopeRetry()
	RetryPendingExecutionPayloadEnvelopesOnColumnsStored(ctx context.Context, minSlot uint64)
}

type sleepForSlotSyncedData interface {
	HeadRoot() common.Hash
}

type sleepForSlotClock interface {
	GetCurrentEpoch() uint64
	GetCurrentSlot() uint64
	GetSlotTime(uint64) time.Time
}

type sleepForSlotWake struct {
	root          common.Hash
	executionHead common.Hash
	at            time.Time
	safeChanged   bool
}

// A head that the ForkChoice stage did not materialize (it failed, or the head moved away and back) may wake the
// stage again after this long; a persistent failure then costs one stage pass per interval, not one per poll.
const sleepForSlotRewakeInterval = time.Second

func waitForNextSlotOrHeadChange(
	ctx context.Context,
	nextSlot uint64,
	beaconCfg *clparams.BeaconChainConfig,
	forkChoice sleepForSlotForkChoice,
	syncedData sleepForSlotSyncedData,
	clock sleepForSlotClock,
	lastWake sleepForSlotWake,
	lastExecutionHead common.Hash,
) (sleepForSlotWake, bool, error) {
	lastWake.safeChanged = false
	nextSlotTime := clock.GetSlotTime(nextSlot)
	timer := time.NewTimer(time.Until(nextSlotTime))
	defer timer.Stop()

	var tick <-chan time.Time
	var columnsStored <-chan struct{}
	if beaconCfg.GetCurrentStateVersion(clock.GetCurrentEpoch()) >= clparams.GloasVersion {
		ticker := time.NewTicker(sleepForSlotHeadPollInterval)
		defer ticker.Stop()
		tick = ticker.C
		columnsStored = forkChoice.PendingEnvelopeRetryWake()
	}

	columnWake := columnsStored
	columnWakeUnserved := false
	defer func() {
		if columnWakeUnserved {
			forkChoice.WakePendingEnvelopeRetry()
		}
	}()
	for {
		columnsWoke := false
		select {
		case <-ctx.Done():
			return sleepForSlotWake{}, false, ctx.Err()
		case <-forkChoice.FastConfirmationChanged():
			lastWake.safeChanged = true
			return lastWake, true, nil
		case <-timer.C:
			return sleepForSlotWake{}, false, nil
		case <-tick:
			if columnWakeUnserved {
				forkChoice.WakePendingEnvelopeRetry()
			}
			columnWake = columnsStored
		case <-columnWake:
			columnsWoke = true
			columnWakeUnserved = true
		}
		if !time.Now().Before(nextSlotTime) {
			return sleepForSlotWake{}, false, nil
		}
		currentSlot := clock.GetCurrentSlot()
		columnFloor := max(currentSlot, 1) - 1
		if columnsWoke {
			if err := ctx.Err(); err != nil {
				return sleepForSlotWake{}, false, err
			}
			if !forkChoice.HasAttemptablePendingEnvelope(columnFloor) {
				columnWakeUnserved = false
				continue
			}
		}
		head, headSlot, err := forkChoice.GetHeadNode()
		select {
		case <-ctx.Done():
			return sleepForSlotWake{}, false, ctx.Err()
		default:
		}
		if !time.Now().Before(nextSlotTime) {
			return sleepForSlotWake{}, false, nil
		}
		if err != nil {
			if columnsWoke {
				columnWake = nil
			}
			continue
		}
		executionHead, ready := forkChoice.GetExecutionHead(head)
		// Both fork-choice reads can block past cancellation or the slot boundary.
		select {
		case <-ctx.Done():
			return sleepForSlotWake{}, false, ctx.Err()
		default:
		}
		if !time.Now().Before(nextSlotTime) {
			return sleepForSlotWake{}, false, nil
		}
		changed := head.Root != syncedData.HeadRoot() || (ready && executionHead != lastExecutionHead)
		if changed && (head.Root != lastWake.root || (ready && executionHead != lastWake.executionHead) || time.Since(lastWake.at) >= sleepForSlotRewakeInterval) {
			return sleepForSlotWake{root: head.Root, executionHead: executionHead, at: time.Now()}, true, nil
		}
		retryCtx, cancel := context.WithDeadline(ctx, nextSlotTime)
		if columnsWoke {
			forkChoice.RetryPendingExecutionPayloadEnvelopesOnColumnsStored(retryCtx, columnFloor)
			columnWakeUnserved = false
		} else {
			// The head's envelope decides whether the next proposer builds on a full or empty parent.
			minRetrySlot := headSlot
			if currentSlot > 0 {
				minRetrySlot = min(minRetrySlot, currentSlot-1)
			}
			forkChoice.RetryDataAvailablePendingExecutionPayloadEnvelopes(retryCtx, minRetrySlot)
		}
		cancel()
	}
}
