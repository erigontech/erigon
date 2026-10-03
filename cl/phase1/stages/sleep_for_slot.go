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
	"github.com/erigontech/erigon/common"
)

// Matches chain tip sync's 50 ms polling. GetHead is a cached read unless fork choice changed since the last call (a
// block, payload, attestation or slot tick), in which case it recomputes the head under the fork choice lock.
const sleepForSlotHeadPollInterval = 50 * time.Millisecond

type sleepForSlotForkChoice interface {
	gloasHeadReader
	RetryDataAvailablePendingExecutionPayloadEnvelopes(ctx context.Context, minSlot uint64)
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
	root common.Hash
	slot uint64
}

func waitForNextSlotOrHeadChange(
	ctx context.Context,
	nextSlot uint64,
	beaconCfg *clparams.BeaconChainConfig,
	forkChoice sleepForSlotForkChoice,
	syncedData sleepForSlotSyncedData,
	clock sleepForSlotClock,
	lastWake sleepForSlotWake,
) (sleepForSlotWake, bool, error) {
	nextSlotTime := clock.GetSlotTime(nextSlot)
	timer := time.NewTimer(time.Until(nextSlotTime))
	defer timer.Stop()

	if beaconCfg.GetCurrentStateVersion(clock.GetCurrentEpoch()) < clparams.GloasVersion {
		select {
		case <-ctx.Done():
			return sleepForSlotWake{}, false, ctx.Err()
		case <-timer.C:
			return sleepForSlotWake{}, false, nil
		}
	}

	ticker := time.NewTicker(sleepForSlotHeadPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return sleepForSlotWake{}, false, ctx.Err()
		case <-timer.C:
			return sleepForSlotWake{}, false, nil
		case <-ticker.C:
			if !time.Now().Before(nextSlotTime) {
				return sleepForSlotWake{}, false, nil
			}
			currentSlot := clock.GetCurrentSlot()
			head, headSlot, err := forkChoice.GetHead(nil)
			select {
			case <-ctx.Done():
				return sleepForSlotWake{}, false, ctx.Err()
			default:
			}
			if !time.Now().Before(nextSlotTime) {
				return sleepForSlotWake{}, false, nil
			}
			if err != nil {
				continue
			}
			if head != syncedData.HeadRoot() && (head != lastWake.root || currentSlot != lastWake.slot) {
				return sleepForSlotWake{root: head, slot: currentSlot}, true, nil
			}
			// The head's envelope decides whether the next proposer builds on a full or empty parent.
			minRetrySlot := headSlot
			if currentSlot > 0 {
				minRetrySlot = min(minRetrySlot, currentSlot-1)
			}
			retryCtx, cancel := context.WithDeadline(ctx, nextSlotTime)
			forkChoice.RetryDataAvailablePendingExecutionPayloadEnvelopes(retryCtx, minRetrySlot)
			cancel()
		}
	}
}
