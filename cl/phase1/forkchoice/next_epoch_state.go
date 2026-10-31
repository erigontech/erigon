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

package forkchoice

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func nextEpochToPrecompute(currentSlot, lastEpoch, slotsPerEpoch uint64) uint64 {
	return max(currentSlot/slotsPerEpoch, lastEpoch) + 1
}

// RunNextEpochPrecompute prepares one epoch-start state near the end of each epoch.
func (f *ForkChoiceStore) RunNextEpochPrecompute(ctx context.Context) {
	var lastEpoch uint64
	for {
		currentSlot := f.ethClock.GetCurrentSlot()
		nextEpoch := nextEpochToPrecompute(currentSlot, lastEpoch, f.beaconCfg.SlotsPerEpoch)
		nextEpochStart := nextEpoch * f.beaconCfg.SlotsPerEpoch
		triggerTime := f.ethClock.GetSlotTime(nextEpochStart - 1).Add(time.Duration(f.beaconCfg.SecondsPerSlot) * time.Second * 2 / 3)
		if err := common.Sleep(ctx, time.Until(triggerTime)); err != nil {
			return
		}

		lastEpoch = nextEpoch
		if err := f.precomputeNextEpochState(ctx, nextEpochStart); err != nil {
			log.Warn("Could not precompute next epoch state", "epoch", lastEpoch, "err", err)
		}
	}
}

func (f *ForkChoiceStore) precomputeNextEpochState(ctx context.Context, nextEpochStart uint64) error {
	if !f.Synced() || f.syncedDataManager == nil {
		return nil
	}

	var parentRoot common.Hash
	var nextState *state.CachingBeaconState
	endingEpoch := (nextEpochStart - 1) / f.beaconCfg.SlotsPerEpoch
	err := f.syncedDataManager.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, headRoot common.Hash, headSlot uint64) error {
		if headSlot/f.beaconCfg.SlotsPerEpoch != endingEpoch {
			return nil
		}
		var err error
		nextState, err = headState.Copy()
		parentRoot = headRoot
		return err
	})
	if err != nil && !errors.Is(err, synced_data.ErrNotSynced) {
		return err
	}
	if nextState == nil {
		return nil
	}
	if err := transition.DefaultMachine.ProcessSlots(nextState, nextEpochStart); err != nil {
		return fmt.Errorf("advance state to slot %d: %w", nextEpochStart, err)
	}
	if _, err := nextState.HashSSZ(); err != nil {
		return fmt.Errorf("hash state at slot %d: %w", nextEpochStart, err)
	}
	f.forkGraph.SetNextEpochState(ctx, parentRoot, nextState)
	return nil
}
