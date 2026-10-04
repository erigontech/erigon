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
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type sleepForSlotForkChoiceFake struct {
	head  common.Hash
	calls *int
}

func (f sleepForSlotForkChoiceFake) GetHead(*state.CachingBeaconState) (common.Hash, uint64, error) {
	if f.calls != nil {
		(*f.calls)++
	}
	return f.head, 0, nil
}

type blockingSleepForSlotForkChoiceFake struct {
	head    common.Hash
	started chan<- struct{}
	release <-chan struct{}
}

func (f blockingSleepForSlotForkChoiceFake) GetHead(*state.CachingBeaconState) (common.Hash, uint64, error) {
	close(f.started)
	<-f.release
	return f.head, 0, nil
}

type sleepForSlotSyncedDataFake struct {
	head common.Hash
}

func (f sleepForSlotSyncedDataFake) HeadRoot() common.Hash {
	return f.head
}

type sleepForSlotClockFake struct {
	currentEpoch uint64
	currentSlot  uint64
	nextSlot     time.Time
}

func (f sleepForSlotClockFake) GetCurrentSlot() uint64 {
	return f.currentSlot
}

func (f sleepForSlotClockFake) GetCurrentEpoch() uint64 {
	return f.currentEpoch
}

func (f sleepForSlotClockFake) GetSlotTime(uint64) time.Time {
	return f.nextSlot
}

func TestSleepForSlotGloasHeadChangeWakesEarlyAndTransitionsToForkChoice(t *testing.T) {
	materializedHead := common.Hash{1}
	forkChoiceHead := common.Hash{2}
	calls := 0
	clock := sleepForSlotClockFake{currentEpoch: 11, currentSlot: 10, nextSlot: time.Now().Add(500 * time.Millisecond)}
	started := time.Now()

	wake, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead, calls: &calls},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		sleepForSlotWake{},
	)

	require.NoError(t, err)
	require.True(t, headChanged)
	require.Equal(t, sleepForSlotWake{root: forkChoiceHead, slot: 10}, wake)
	require.Less(t, time.Since(started), 250*time.Millisecond)
	require.Equal(t, 1, calls)
	require.Equal(t, ForkChoice, sleepForSlotNextStage(t.Context(), headChanged, readySleepForSlotArgs()))
}

func TestSleepForSlotGloasEqualHeadsWaitsAndTransitionsToChainTipSync(t *testing.T) {
	head := common.Hash{1}
	wait := 150 * time.Millisecond
	calls := 0
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(wait)}
	started := time.Now()

	_, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: head, calls: &calls},
		sleepForSlotSyncedDataFake{head: head},
		clock,
		sleepForSlotWake{},
	)

	require.NoError(t, err)
	require.False(t, headChanged)
	require.GreaterOrEqual(t, time.Since(started), wait-25*time.Millisecond)
	require.Positive(t, calls)
	require.Equal(t, ChainTipSync, sleepForSlotNextStage(t.Context(), headChanged, readySleepForSlotArgs()))
}

func TestSleepForSlotPreGloasHeadChangeWaitsAndTransitionsToChainTipSync(t *testing.T) {
	wait := 150 * time.Millisecond
	calls := 0
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(wait)}
	started := time.Now()

	_, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(11),
		sleepForSlotForkChoiceFake{head: common.Hash{2}, calls: &calls},
		sleepForSlotSyncedDataFake{head: common.Hash{1}},
		clock,
		sleepForSlotWake{},
	)

	require.NoError(t, err)
	require.False(t, headChanged)
	require.GreaterOrEqual(t, time.Since(started), wait-25*time.Millisecond)
	require.Zero(t, calls)
	require.Equal(t, ChainTipSync, sleepForSlotNextStage(t.Context(), headChanged, readySleepForSlotArgs()))
}

func TestSleepForSlotDoesNotWakeTwiceForSameHeadInSlot(t *testing.T) {
	materializedHead := common.Hash{1}
	forkChoiceHead := common.Hash{2}
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(500 * time.Millisecond)}

	wake, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		sleepForSlotWake{},
	)
	require.NoError(t, err)
	require.True(t, headChanged)

	wait := 150 * time.Millisecond
	clock.nextSlot = time.Now().Add(wait)
	started := time.Now()
	_, headChanged, err = waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		wake,
	)

	require.NoError(t, err)
	require.False(t, headChanged)
	require.GreaterOrEqual(t, time.Since(started), wait-25*time.Millisecond)
}

func TestSleepForSlotPollCompletingAfterNextSlotDoesNotWakeEarly(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	nextSlot := time.Now().Add(250 * time.Millisecond)
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: nextSlot}
	releaseTimer := time.AfterFunc(time.Until(nextSlot)+20*time.Millisecond, func() {
		close(release)
	})
	defer releaseTimer.Stop()

	_, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		blockingSleepForSlotForkChoiceFake{head: common.Hash{2}, started: started, release: release},
		sleepForSlotSyncedDataFake{head: common.Hash{1}},
		clock,
		sleepForSlotWake{},
	)

	require.NoError(t, err)
	require.False(t, headChanged)
	select {
	case <-started:
	default:
		require.Fail(t, "fork choice head was not polled")
	}
}

func TestSleepForSlotContextCancellationEndsWaitPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Second)}
	time.AfterFunc(20*time.Millisecond, cancel)
	started := time.Now()

	_, headChanged, err := waitForNextSlotOrHeadChange(
		ctx, 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: common.Hash{1}},
		sleepForSlotSyncedDataFake{head: common.Hash{1}},
		clock,
		sleepForSlotWake{},
	)

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, headChanged)
	require.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestSleepForSlotCatchingUpPrecedesHeadChange(t *testing.T) {
	args := readySleepForSlotArgs()
	args.seenEpoch = 1
	args.targetEpoch = 2

	require.Equal(t, ForwardSync, sleepForSlotNextStage(t.Context(), true, args))
}

func sleepForSlotNextStage(ctx context.Context, headChanged bool, args Args) StageName {
	cfg := &Cfg{sleepForSlotHeadChanged: headChanged}
	return ConsensusClStages(ctx, cfg).Stages[SleepForSlot].TransitionFunc(cfg, args, nil)
}

func readySleepForSlotArgs() Args {
	return Args{peers: 1, hasDownloaded: true}
}

func sleepForSlotConfig(gloasForkEpoch uint64) *clparams.BeaconChainConfig {
	return &clparams.BeaconChainConfig{
		AltairForkEpoch:    0,
		BellatrixForkEpoch: 0,
		CapellaForkEpoch:   0,
		DenebForkEpoch:     0,
		ElectraForkEpoch:   0,
		FuluForkEpoch:      0,
		GloasForkEpoch:     gloasForkEpoch,
	}
}
