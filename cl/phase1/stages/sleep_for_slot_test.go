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
	"testing/synctest"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type sleepForSlotForkChoiceFake struct {
	head           common.Hash
	headSlot       uint64
	payloadStatus  cltypes.PayloadStatus
	executionHead  common.Hash
	ready          *bool
	calls          *int
	retryMinSlots  *[]uint64
	retryDeadlines *[]time.Time
}

func (f sleepForSlotForkChoiceFake) GetHeadNode() (forkchoice.ForkChoiceNode, uint64, error) {
	if f.calls != nil {
		(*f.calls)++
	}
	return forkchoice.ForkChoiceNode{Root: f.head, PayloadStatus: f.payloadStatus}, f.headSlot, nil
}

func TestSleepForSlotGloasPayloadStatusChangeBypassesRewakeInterval(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   cltypes.PayloadStatus
		previous common.Hash
	}{
		{"EMPTY to FULL", cltypes.PayloadStatusFull, common.Hash{}},
		{"FULL to EMPTY", cltypes.PayloadStatusEmpty, common.Hash{0xee}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				head := common.Hash{1}
				started := time.Now()
				clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: started.Add(2 * sleepForSlotRewakeInterval)}
				wake, changed, err := waitForNextSlotOrHeadChange(
					t.Context(), 11, sleepForSlotConfig(10),
					sleepForSlotForkChoiceFake{head: head, payloadStatus: tc.status},
					sleepForSlotSyncedDataFake{head: head}, clock,
					sleepForSlotWake{root: head, executionHead: tc.previous, at: started}, tc.previous,
				)
				require.NoError(t, err)
				require.True(t, changed, "a payload decision at the same beacon root must wake canonical fork choice")
				require.Equal(t, head, wake.root)
				require.NotEqual(t, tc.previous, wake.executionHead)
				require.Equal(t, sleepForSlotHeadPollInterval, time.Since(started), "execution-head changes must wake on the first poll, even after a recent wake")
			})
		})
	}
}

func (f sleepForSlotForkChoiceFake) RetryDataAvailablePendingExecutionPayloadEnvelopes(ctx context.Context, minSlot uint64) {
	if f.retryMinSlots != nil {
		*f.retryMinSlots = append(*f.retryMinSlots, minSlot)
	}
	if f.retryDeadlines != nil {
		deadline, _ := ctx.Deadline()
		*f.retryDeadlines = append(*f.retryDeadlines, deadline)
	}
}

type blockingSleepForSlotForkChoiceFake struct {
	head               common.Hash
	started            chan<- struct{}
	release            <-chan struct{}
	blockExecutionHead bool
}

func (blockingSleepForSlotForkChoiceFake) RetryDataAvailablePendingExecutionPayloadEnvelopes(context.Context, uint64) {
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
	var retryMinSlots []uint64
	clock := sleepForSlotClockFake{currentEpoch: 11, currentSlot: 10, nextSlot: time.Now().Add(5 * sleepForSlotHeadPollInterval)}
	started := time.Now()

	wake, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead, calls: &calls, retryMinSlots: &retryMinSlots},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		sleepForSlotWake{}, common.Hash{},
	)

	require.NoError(t, err)
	require.True(t, headChanged)
	require.Equal(t, forkChoiceHead, wake.root)
	require.Less(t, time.Since(started), 3*sleepForSlotHeadPollInterval)
	require.Equal(t, 1, calls)
	require.Empty(t, retryMinSlots, "a pending head change is materialized before envelope retries")
	require.Equal(t, ForkChoice, sleepForSlotNextStage(headChanged, readySleepForSlotArgs()))
}

func TestSleepForSlotGloasEqualHeadsWaitsAndTransitionsToChainTipSync(t *testing.T) {
	head := common.Hash{1}
	wait := 5 * sleepForSlotHeadPollInterval
	calls := 0
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(wait)}
	started := time.Now()

	_, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: head, calls: &calls},
		sleepForSlotSyncedDataFake{head: head},
		clock,
		sleepForSlotWake{}, common.Hash{},
	)

	require.NoError(t, err)
	require.False(t, headChanged)
	require.GreaterOrEqual(t, time.Since(started), wait-25*time.Millisecond)
	require.Positive(t, calls)
	require.Equal(t, ChainTipSync, sleepForSlotNextStage(headChanged, readySleepForSlotArgs()))
}

func TestSleepForSlotGloasRetriesDataAvailableEnvelopesFromHeadOrPreviousSlot(t *testing.T) {
	for _, tt := range []struct {
		name        string
		headSlot    uint64
		wantMinSlot uint64
	}{
		{name: "head in current slot", headSlot: 10, wantMinSlot: 9},
		{name: "head before missed slots", headSlot: 6, wantMinSlot: 6},
	} {
		t.Run(tt.name, func(t *testing.T) {
			head := common.Hash{1}
			var retryMinSlots []uint64
			var retryDeadlines []time.Time
			clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(5 * sleepForSlotHeadPollInterval)}

			_, headChanged, err := waitForNextSlotOrHeadChange(
				t.Context(), 11, sleepForSlotConfig(10),
				sleepForSlotForkChoiceFake{head: head, headSlot: tt.headSlot, retryMinSlots: &retryMinSlots, retryDeadlines: &retryDeadlines},
				sleepForSlotSyncedDataFake{head: head},
				clock,
				sleepForSlotWake{}, common.Hash{},
			)

			require.NoError(t, err)
			require.False(t, headChanged)
			require.NotEmpty(t, retryMinSlots)
			for _, minSlot := range retryMinSlots {
				require.Equal(t, tt.wantMinSlot, minSlot)
			}
			for _, deadline := range retryDeadlines {
				require.Equal(t, clock.nextSlot, deadline, "envelope retries must not run past the next slot")
			}
		})
	}
}

func TestSleepForSlotPreGloasHeadChangeWaitsAndTransitionsToChainTipSync(t *testing.T) {
	wait := 2 * sleepForSlotHeadPollInterval
	calls := 0
	var retryMinSlots []uint64
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(wait)}
	started := time.Now()

	_, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(11),
		sleepForSlotForkChoiceFake{head: common.Hash{2}, calls: &calls, retryMinSlots: &retryMinSlots},
		sleepForSlotSyncedDataFake{head: common.Hash{1}},
		clock,
		sleepForSlotWake{}, common.Hash{},
	)

	require.NoError(t, err)
	require.False(t, headChanged)
	require.GreaterOrEqual(t, time.Since(started), wait-25*time.Millisecond)
	require.Zero(t, calls)
	require.Empty(t, retryMinSlots)
	require.Equal(t, ChainTipSync, sleepForSlotNextStage(headChanged, readySleepForSlotArgs()))
}

func TestSleepForSlotRewakesForSameHeadAfterInterval(t *testing.T) {
	materializedHead := common.Hash{1}
	forkChoiceHead := common.Hash{2}
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(5 * sleepForSlotHeadPollInterval)}

	// The earlier wake did not materialize the head (ForkChoice failed, or the head moved away and back).
	_, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		sleepForSlotWake{root: forkChoiceHead, at: time.Now().Add(-sleepForSlotRewakeInterval)}, common.Hash{},
	)

	require.NoError(t, err)
	require.True(t, headChanged)
}

func TestSleepForSlotDoesNotWakeTwiceForSameHeadWithinRewakeInterval(t *testing.T) {
	materializedHead := common.Hash{1}
	forkChoiceHead := common.Hash{2}
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(5 * sleepForSlotHeadPollInterval)}

	wake, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		sleepForSlotWake{}, common.Hash{},
	)
	require.NoError(t, err)
	require.True(t, headChanged)

	wait := 2 * sleepForSlotHeadPollInterval
	clock.nextSlot = time.Now().Add(wait)
	started := time.Now()
	_, headChanged, err = waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		wake, common.Hash{},
	)

	require.NoError(t, err)
	require.False(t, headChanged)
	require.GreaterOrEqual(t, time.Since(started), wait-25*time.Millisecond)
}

func TestSleepForSlotDoesNotBypassRewakeIntervalWhenExecutionHeadBecomesUnavailable(t *testing.T) {
	materializedHead := common.Hash{1}
	forkChoiceHead := common.Hash{2}
	ready := true
	clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(5 * sleepForSlotHeadPollInterval)}

	wake, headChanged, err := waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead, executionHead: common.Hash{0xee}, ready: &ready},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		sleepForSlotWake{}, common.Hash{},
	)
	require.NoError(t, err)
	require.True(t, headChanged)

	ready = false
	wait := 2 * sleepForSlotHeadPollInterval
	clock.nextSlot = time.Now().Add(wait)
	started := time.Now()
	_, headChanged, err = waitForNextSlotOrHeadChange(
		t.Context(), 11, sleepForSlotConfig(10),
		sleepForSlotForkChoiceFake{head: forkChoiceHead, ready: &ready},
		sleepForSlotSyncedDataFake{head: materializedHead},
		clock,
		wake, common.Hash{},
	)

	require.NoError(t, err)
	require.False(t, headChanged)
	require.GreaterOrEqual(t, time.Since(started), wait-25*time.Millisecond)
}

func TestSleepForSlotPollCompletingAfterNextSlotDoesNotWakeEarly(t *testing.T) {
	for _, blockedCall := range []string{"GetHeadNode", "GetExecutionHead"} {
		t.Run(blockedCall, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := make(chan struct{})
				release := make(chan struct{})
				nextSlot := time.Now().Add(5 * sleepForSlotHeadPollInterval)
				clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: nextSlot}
				releaseTimer := time.AfterFunc(time.Until(nextSlot)+20*time.Millisecond, func() {
					close(release)
				})
				defer releaseTimer.Stop()

				wake, headChanged, err := waitForNextSlotOrHeadChange(
					t.Context(), 11, sleepForSlotConfig(10),
					blockingSleepForSlotForkChoiceFake{head: common.Hash{2}, started: started, release: release, blockExecutionHead: blockedCall == "GetExecutionHead"},
					sleepForSlotSyncedDataFake{head: common.Hash{1}},
					clock,
					sleepForSlotWake{}, common.Hash{},
				)

				require.NoError(t, err)
				require.False(t, headChanged)
				require.Zero(t, wake)
				require.Equal(t, ChainTipSync, sleepForSlotNextStage(headChanged, readySleepForSlotArgs()))
				select {
				case <-started:
				default:
					t.Fatalf("%s was not called", blockedCall)
				}
			})
		})
	}
}

func TestSleepForSlotPollCompletingAfterCancellationDoesNotWake(t *testing.T) {
	for _, blockedCall := range []string{"GetHeadNode", "GetExecutionHead"} {
		t.Run(blockedCall, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				started := make(chan struct{})
				release := make(chan struct{})
				go func() {
					select {
					case <-started:
						cancel()
						close(release)
					case <-ctx.Done():
					}
				}()

				wake, headChanged, err := waitForNextSlotOrHeadChange(
					ctx, 11, sleepForSlotConfig(10),
					blockingSleepForSlotForkChoiceFake{head: common.Hash{2}, started: started, release: release, blockExecutionHead: blockedCall == "GetExecutionHead"},
					sleepForSlotSyncedDataFake{head: common.Hash{1}},
					sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Second)},
					sleepForSlotWake{}, common.Hash{},
				)

				require.ErrorIs(t, err, context.Canceled)
				require.False(t, headChanged)
				require.Zero(t, wake)
			})
		})
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
		sleepForSlotWake{}, common.Hash{},
	)

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, headChanged)
	require.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestSleepForSlotCatchingUpPrecedesHeadChange(t *testing.T) {
	args := readySleepForSlotArgs()
	args.seenEpoch = 1
	args.targetEpoch = 2

	require.Equal(t, ForwardSync, sleepForSlotNextStage(true, args))
}

func sleepForSlotNextStage(headChanged bool, args Args) StageName {
	cfg := &Cfg{sleepForSlotHeadChanged: headChanged}
	return ConsensusClStages().Stages[SleepForSlot].TransitionFunc(cfg, args, nil)
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

func (f sleepForSlotForkChoiceFake) GetExecutionHead(head forkchoice.ForkChoiceNode) (common.Hash, bool) {
	if f.ready != nil {
		return f.executionHead, *f.ready
	}
	if head.PayloadStatus == cltypes.PayloadStatusFull {
		return common.Hash{0xee}, true
	}
	return common.Hash{}, true
}

func (f blockingSleepForSlotForkChoiceFake) GetHeadNode() (forkchoice.ForkChoiceNode, uint64, error) {
	if !f.blockExecutionHead {
		close(f.started)
		<-f.release
	}
	return forkchoice.ForkChoiceNode{Root: f.head}, 0, nil
}

func (f blockingSleepForSlotForkChoiceFake) GetExecutionHead(forkchoice.ForkChoiceNode) (common.Hash, bool) {
	if f.blockExecutionHead {
		close(f.started)
		<-f.release
	}
	return common.Hash{}, true
}
