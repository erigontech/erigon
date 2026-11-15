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
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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
	emitter        *beaconevents.EventEmitter
	parkedRoot     func(root common.Hash, minSlot uint64) bool // nil keeps every woken root
	columnRetry    func(ctx context.Context, minSlot uint64)
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

func (f sleepForSlotForkChoiceFake) PendingEnvelopeRetryWake() <-chan struct{} {
	return f.emitter.PendingEnvelopeRetryWake()
}

func (f sleepForSlotForkChoiceFake) PrunePendingEnvelopeRetryRoots(minSlot uint64) bool {
	return f.emitter.PrunePendingEnvelopeRetryRoots(func(root common.Hash) bool {
		return f.parkedRoot == nil || f.parkedRoot(root, minSlot)
	})
}

func (f sleepForSlotForkChoiceFake) RearmPendingEnvelopeRetryWake() {
	f.emitter.RearmPendingEnvelopeRetryWake()
}

func (f sleepForSlotForkChoiceFake) RetryPendingExecutionPayloadEnvelopesOnColumnsStored(ctx context.Context, minSlot uint64) {
	f.emitter.TakePendingEnvelopeRetryRoots()
	if f.columnRetry != nil {
		f.columnRetry(ctx, minSlot)
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

func (blockingSleepForSlotForkChoiceFake) PendingEnvelopeRetryWake() <-chan struct{} {
	return nil
}

func (blockingSleepForSlotForkChoiceFake) PrunePendingEnvelopeRetryRoots(uint64) bool { return false }

func (blockingSleepForSlotForkChoiceFake) RearmPendingEnvelopeRetryWake() {}

func (blockingSleepForSlotForkChoiceFake) RetryPendingExecutionPayloadEnvelopesOnColumnsStored(context.Context, uint64) {
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

type columnRetryLog struct {
	mu       sync.Mutex
	minSlots []uint64
	ctxErrs  []error
}

func (l *columnRetryLog) record(ctx context.Context, minSlot uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.minSlots = append(l.minSlots, minSlot)
	l.ctxErrs = append(l.ctxErrs, ctx.Err())
}

func (l *columnRetryLog) passes() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uint64(nil), l.minSlots...)
}

func (l *columnRetryLog) contextErrors() []error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]error(nil), l.ctxErrs...)
}

func distinctRoots(n int) []common.Hash {
	roots := make([]common.Hash, n)
	for i := range roots {
		roots[i] = common.BytesToHash([]byte{byte(i >> 8), byte(i), 1})
	}
	return roots
}

type sleepForSlotRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func startSleepForSlot(
	t *testing.T,
	beaconCfg *clparams.BeaconChainConfig,
	forkChoice sleepForSlotForkChoice,
	syncedHead common.Hash,
	clock sleepForSlotClockFake,
	lastExecutionHead common.Hash,
) *sleepForSlotRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	run := &sleepForSlotRun{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(run.done)
		_, _, run.err = waitForNextSlotOrHeadChange(
			ctx, 11, beaconCfg, forkChoice, sleepForSlotSyncedDataFake{head: syncedHead}, clock, sleepForSlotWake{}, lastExecutionHead,
		)
	}()
	return run
}

func (r *sleepForSlotRun) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	<-r.done
	require.ErrorIs(t, r.err, context.Canceled)
}

func TestSleepForSlotColumnWakeCoalescesBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		head := common.Hash{1}
		emitter := beaconevents.NewEventEmitter()
		for _, root := range distinctRoots(128) {
			emitter.NotifyDataColumnStored(root)
		}
		var retries columnRetryLog
		clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Minute)}
		run := startSleepForSlot(t, sleepForSlotConfig(10),
			sleepForSlotForkChoiceFake{head: head, emitter: emitter, columnRetry: retries.record},
			head, clock, common.Hash{})

		synctest.Wait()
		require.Len(t, retries.passes(), 1, "a burst of notifications runs one pass")
		emitter.NotifyDataColumnStored(common.Hash{0xff})
		synctest.Wait()
		require.Len(t, retries.passes(), 2)
		run.stop(t)
	})
}

func TestSleepForSlotColumnWakeBoundsPassesUnderSustainedNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		head := common.Hash{1}
		emitter := beaconevents.NewEventEmitter()
		emitter.NotifyDataColumnStored(common.Hash{0xff})
		var retries columnRetryLog
		release := make(chan struct{})
		columnRetry := func(ctx context.Context, minSlot uint64) {
			retries.record(ctx, minSlot)
			if len(retries.passes()) == 1 {
				<-release
			}
		}
		clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Minute)}
		run := startSleepForSlot(t, sleepForSlotConfig(10),
			sleepForSlotForkChoiceFake{head: head, emitter: emitter, columnRetry: columnRetry},
			head, clock, common.Hash{})

		synctest.Wait()
		require.Len(t, retries.passes(), 1)
		for _, root := range distinctRoots(1000) {
			emitter.NotifyDataColumnStored(root)
		}
		close(release)
		synctest.Wait()
		require.Len(t, retries.passes(), 2, "notifications that arrive during a pass cost one more pass")
		run.stop(t)
	})
}

func TestSleepForSlotColumnWakeUsesPreviousSlotFloor(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		currentSlot, headSlot uint64
		want                  uint64
	}{
		{"head behind the previous slot", 10, 5, 9},
		{"genesis slot", 0, 0, 0},
		{"first slot", 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				head := common.Hash{1}
				emitter := beaconevents.NewEventEmitter()
				emitter.NotifyDataColumnStored(common.Hash{0xff})
				var retries columnRetryLog
				clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: tc.currentSlot, nextSlot: time.Now().Add(time.Minute)}
				run := startSleepForSlot(t, sleepForSlotConfig(10),
					sleepForSlotForkChoiceFake{head: head, headSlot: tc.headSlot, emitter: emitter, columnRetry: retries.record},
					head, clock, common.Hash{})

				synctest.Wait()
				require.Equal(t, []uint64{tc.want}, retries.passes())
				run.stop(t)
			})
		})
	}
}

func TestSleepForSlotColumnWakePrunesRootsBelowThePreviousSlot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rootSlot   uint64
		wantPasses []uint64
		wantReads  int
	}{
		{"previous slot", 9, []uint64{9}, 1},
		{"two slots back", 8, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				head := common.Hash{1}
				emitter := beaconevents.NewEventEmitter()
				emitter.NotifyDataColumnStored(common.Hash{0xff})
				var retries columnRetryLog
				reads := 0
				clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Minute)}
				run := startSleepForSlot(t, sleepForSlotConfig(10),
					sleepForSlotForkChoiceFake{
						head: head, calls: &reads, emitter: emitter, columnRetry: retries.record,
						parkedRoot: func(_ common.Hash, minSlot uint64) bool { return tc.rootSlot >= minSlot },
					},
					head, clock, common.Hash{})

				synctest.Wait()
				require.Equal(t, tc.wantPasses, retries.passes())
				require.Equal(t, tc.wantReads, reads)
				run.stop(t)
			})
		})
	}
}

func TestSleepForSlotPreGloasLeavesTheColumnWakeUntouched(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		head := common.Hash{1}
		emitter := beaconevents.NewEventEmitter()
		emitter.NotifyDataColumnStored(common.Hash{0xff})
		var retries columnRetryLog
		reads := 0
		clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Minute)}

		_, headChanged, err := waitForNextSlotOrHeadChange(
			t.Context(), 11, sleepForSlotConfig(11),
			sleepForSlotForkChoiceFake{head: head, calls: &reads, emitter: emitter, columnRetry: retries.record},
			sleepForSlotSyncedDataFake{head: head}, clock, sleepForSlotWake{}, common.Hash{},
		)

		require.NoError(t, err)
		require.False(t, headChanged)
		require.Empty(t, retries.passes())
		require.Zero(t, reads)
		require.Len(t, emitter.PendingEnvelopeRetryWake(), 1, "a wait before Gloas leaves the wake for a Gloas wait")
	})
}

func TestSleepForSlotColumnWakeDefersToHeadChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		emitter := beaconevents.NewEventEmitter()
		emitter.NotifyDataColumnStored(common.Hash{0xff})
		var retries columnRetryLog
		started := time.Now()
		clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: started.Add(time.Minute)}

		wake, headChanged, err := waitForNextSlotOrHeadChange(
			t.Context(), 11, sleepForSlotConfig(10),
			sleepForSlotForkChoiceFake{head: common.Hash{2}, emitter: emitter, columnRetry: retries.record},
			sleepForSlotSyncedDataFake{head: common.Hash{1}}, clock, sleepForSlotWake{}, common.Hash{},
		)

		require.NoError(t, err)
		require.True(t, headChanged)
		require.Equal(t, common.Hash{2}, wake.root)
		require.Empty(t, retries.passes(), "a pending head change is materialized before any envelope retry")
		require.Equal(t, started, time.Now())
	})
}

func TestSleepForSlotColumnWakeWithoutParkedEnvelopeReadsNoHead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		head := common.Hash{1}
		parked := common.Hash{0xff}
		emitter := beaconevents.NewEventEmitter()
		var retries columnRetryLog
		calls := 0
		clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Minute)}
		forkChoice := sleepForSlotForkChoiceFake{
			head: head, calls: &calls, emitter: emitter, columnRetry: retries.record,
			parkedRoot: func(root common.Hash, _ uint64) bool { return root == parked },
		}
		run := startSleepForSlot(t, sleepForSlotConfig(10), forkChoice, head, clock, common.Hash{})
		defer run.cancel()

		for _, root := range distinctRoots(128) {
			emitter.NotifyDataColumnStored(root)
			synctest.Wait()
		}
		require.Empty(t, retries.passes())
		require.Zero(t, calls, "a root without a parked envelope must not cost a head read")

		emitter.NotifyDataColumnStored(parked)
		synctest.Wait()
		require.Len(t, retries.passes(), 1)
		require.Equal(t, 1, calls)
		run.stop(t)
	})
}

type failingHeadForkChoice struct {
	sleepForSlotForkChoiceFake
	failFirst int32 // GetHeadNode fails for this many reads
	reads     atomic.Int32
}

func (f *failingHeadForkChoice) GetHeadNode() (forkchoice.ForkChoiceNode, uint64, error) {
	if f.reads.Add(1) <= f.failFirst {
		return forkchoice.ForkChoiceNode{}, 0, errors.New("checkpoint state unavailable")
	}
	return f.sleepForSlotForkChoiceFake.GetHeadNode()
}

func TestSleepForSlotColumnWakeBoundsHeadReadsWhileGetHeadNodeFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		head := common.Hash{1}
		emitter := beaconevents.NewEventEmitter()
		var retries columnRetryLog
		clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Minute)}
		forkChoice := &failingHeadForkChoice{
			sleepForSlotForkChoiceFake: sleepForSlotForkChoiceFake{head: head, emitter: emitter, columnRetry: retries.record},
			failFirst:                  math.MaxInt32,
		}
		run := startSleepForSlot(t, sleepForSlotConfig(10), forkChoice, head, clock, common.Hash{})
		defer run.cancel()

		for _, root := range distinctRoots(128) {
			emitter.NotifyDataColumnStored(root)
			synctest.Wait()
		}
		require.EqualValues(t, 1, forkChoice.reads.Load(), "a failed head read must not be repeated per column notification")

		for range 10 {
			before := forkChoice.reads.Load()
			time.Sleep(sleepForSlotHeadPollInterval)
			synctest.Wait()
			for _, root := range distinctRoots(128) {
				emitter.NotifyDataColumnStored(root)
				synctest.Wait()
			}
			require.EqualValues(t, 2, forkChoice.reads.Load()-before, "one poll read and one column wake read per poll interval")
		}
		require.Empty(t, retries.passes())

		<-emitter.PendingEnvelopeRetryWake()
		run.stop(t)
		require.Len(t, emitter.PendingEnvelopeRetryWake(), 1, "the unserved wake is rearmed for the next wait")
	})
}

func TestSleepForSlotColumnWakeMutedByAFailedHeadReadIsServedAfterTheNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		head := common.Hash{1}
		emitter := beaconevents.NewEventEmitter()
		var retries columnRetryLog
		clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Minute)}
		forkChoice := &failingHeadForkChoice{
			sleepForSlotForkChoiceFake: sleepForSlotForkChoiceFake{head: head, emitter: emitter, columnRetry: retries.record},
			failFirst:                  1,
		}
		run := startSleepForSlot(t, sleepForSlotConfig(10), forkChoice, head, clock, common.Hash{})
		defer run.cancel()
		synctest.Wait()

		emitter.NotifyDataColumnStored(common.Hash{0xff})
		synctest.Wait()
		require.EqualValues(t, 1, forkChoice.reads.Load())
		require.Empty(t, retries.passes())

		time.Sleep(sleepForSlotHeadPollInterval)
		synctest.Wait()
		require.Len(t, retries.passes(), 1, "the wake that the failed head read muted must not wait for another notification")
		run.stop(t)
	})
}

func TestSleepForSlotColumnWakeSurvivesAHeadChangeReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		emitter := beaconevents.NewEventEmitter()
		emitter.NotifyDataColumnStored(common.Hash{0xff})
		var retries columnRetryLog
		started := time.Now()
		clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: started.Add(time.Minute)}
		forkChoice := sleepForSlotForkChoiceFake{head: common.Hash{2}, emitter: emitter, columnRetry: retries.record}

		_, headChanged, err := waitForNextSlotOrHeadChange(
			t.Context(), 11, sleepForSlotConfig(10), forkChoice,
			sleepForSlotSyncedDataFake{head: common.Hash{1}}, clock, sleepForSlotWake{}, common.Hash{},
		)
		require.NoError(t, err)
		require.True(t, headChanged)
		require.Empty(t, retries.passes())

		run := startSleepForSlot(t, sleepForSlotConfig(10), forkChoice, common.Hash{2}, clock, common.Hash{})
		defer run.cancel()
		synctest.Wait()
		require.Len(t, retries.passes(), 1, "the wake that the head change cut short must not wait for the poll")
		require.Equal(t, started, time.Now())
		run.stop(t)
	})
}

func TestSleepForSlotColumnWakeStopsOnCancel(t *testing.T) {
	t.Run("cancelled while a pass runs", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			head := common.Hash{1}
			emitter := beaconevents.NewEventEmitter()
			emitter.NotifyDataColumnStored(common.Hash{0xff})
			var retries columnRetryLog
			columnRetry := func(ctx context.Context, minSlot uint64) {
				<-ctx.Done()
				retries.record(ctx, minSlot)
			}
			started := time.Now()
			clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: started.Add(time.Minute)}
			run := startSleepForSlot(t, sleepForSlotConfig(10),
				sleepForSlotForkChoiceFake{head: head, emitter: emitter, columnRetry: columnRetry},
				head, clock, common.Hash{})

			synctest.Wait()
			run.stop(t)
			require.Equal(t, started, time.Now(), "cancellation must end the pass, not the slot deadline")
			require.Len(t, retries.contextErrors(), 1)
		})
	})
	t.Run("cancelled before the wake", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			head := common.Hash{1}
			emitter := beaconevents.NewEventEmitter()
			var retries columnRetryLog
			clock := sleepForSlotClockFake{currentEpoch: 10, currentSlot: 10, nextSlot: time.Now().Add(time.Minute)}
			for range 16 {
				emitter.NotifyDataColumnStored(common.Hash{0xff})
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				_, _, err := waitForNextSlotOrHeadChange(
					ctx, 11, sleepForSlotConfig(10),
					sleepForSlotForkChoiceFake{head: head, emitter: emitter, columnRetry: retries.record},
					sleepForSlotSyncedDataFake{head: head}, clock, sleepForSlotWake{}, common.Hash{},
				)
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Empty(t, retries.passes(), "a cancelled wait must not start a pass")
		})
	})
}

func newGloasColumnSidecar(slot uint64, root common.Hash) *cltypes.DataColumnSidecar {
	if clparams.GetBeaconConfig() == nil {
		cfg := clparams.MainnetBeaconConfig
		clparams.InitGlobalStaticConfig(&cfg)
	}
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	sidecar.Slot = slot
	sidecar.BeaconBlockRoot = root
	return sidecar
}

func installColumnStorePeerDas(t *testing.T, f *forwardSyncTipFixture, available func(slot uint64, root common.Hash) (bool, error)) {
	t.Helper()
	peerDas := das_mock_services.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(gomock.Any(), gomock.Any()).DoAndReturn(available).AnyTimes()
	peerDas.EXPECT().SyncColumnDataLater(gomock.Any()).Return(nil).AnyTimes()
	peerDas.EXPECT().BackfillCustodyColumns(gomock.Any(), gomock.Any()).AnyTimes()
	f.cfg.forkChoice.InitPeerDas(peerDas)
}

func startSleepForSlotAtTip(t *testing.T, f *forwardSyncTipFixture, forkChoice sleepForSlotForkChoice) *sleepForSlotRun {
	t.Helper()
	head, _, err := f.cfg.forkChoice.GetHeadNode()
	require.NoError(t, err)
	executionHead, _ := f.cfg.forkChoice.GetExecutionHead(head)
	clock := sleepForSlotClockFake{
		currentEpoch: f.currentSlot / f.cfg.beaconCfg.SlotsPerEpoch,
		currentSlot:  f.currentSlot,
		nextSlot:     time.Now().Add(time.Minute),
	}
	return startSleepForSlot(t, f.cfg.beaconCfg, forkChoice, head.Root, clock, executionHead)
}

type headReadCounter struct {
	*forkchoice.ForkChoiceStore
	headReads    atomic.Int32
	columnPasses atomic.Int32
}

func (c *headReadCounter) GetHeadNode() (forkchoice.ForkChoiceNode, uint64, error) {
	c.headReads.Add(1)
	return c.ForkChoiceStore.GetHeadNode()
}

func (c *headReadCounter) RetryPendingExecutionPayloadEnvelopesOnColumnsStored(ctx context.Context, minSlot uint64) {
	c.columnPasses.Add(1)
	c.ForkChoiceStore.RetryPendingExecutionPayloadEnvelopesOnColumnsStored(ctx, minSlot)
}

func TestSleepForSlotAppliesColumnWaitingEnvelopeOnColumnStore(t *testing.T) {
	for _, notified := range []bool{true, false} {
		t.Run(fmt.Sprintf("notified=%t", notified), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newForwardSyncTipFixtureWithBlob(t, 0, false, true)
				storageEmitter := f.cfg.emitter
				if !notified {
					storageEmitter = beaconevents.NewEventEmitter()
				}
				storage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), f.cfg.beaconCfg, storageEmitter)
				installColumnStorePeerDas(t, f, func(slot uint64, root common.Hash) (bool, error) {
					return storage.ColumnSidecarExists(t.Context(), slot, root, 0)
				})
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
				require.True(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot))
				f.cfg.forkChoice.RetryDataAvailablePendingExecutionPayloadEnvelopes(ctx, f.tip.Block.Slot) // books a probe

				started := time.Now()
				run := startSleepForSlotAtTip(t, f, f.cfg.forkChoice)
				synctest.Wait()
				require.False(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))

				require.NoError(t, storage.WriteColumnSidecars(t.Context(), f.tipRoot, 0, newGloasColumnSidecar(f.tip.Block.Slot, f.tipRoot)))
				synctest.Wait()
				if notified {
					require.True(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot), "the stored column must apply the envelope at once")
					require.False(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot))
					require.Equal(t, started, time.Now())
				} else {
					require.False(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot), "without a notification the poll finds the column")
					time.Sleep(2*sleepForSlotHeadPollInterval - time.Nanosecond)
					synctest.Wait()
					require.False(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot), "the poll waits out the probe interval")
					time.Sleep(time.Nanosecond)
					synctest.Wait()
					require.True(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
					require.Equal(t, started.Add(2*sleepForSlotHeadPollInterval), time.Now())
				}
				run.stop(t)
			})
		})
	}
}

func TestSleepForSlotAppliesEnvelopeParkedAfterItsColumnsWereStored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newForwardSyncTipFixtureWithBlob(t, 0, false, true)
		var available atomic.Bool
		installColumnStorePeerDas(t, f, func(uint64, common.Hash) (bool, error) {
			return available.Load(), nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
		require.True(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot))
		available.Store(true)

		started := time.Now()
		run := startSleepForSlotAtTip(t, f, f.cfg.forkChoice)
		synctest.Wait()

		require.True(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot), "columns stored before the envelope parked leave no other trigger")
		require.Equal(t, started, time.Now())
		run.stop(t)
	})
}

func TestSleepForSlotColumnWakeReadsHeadOnlyOnceTheParkedEnvelopeCanBeAttempted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newForwardSyncTipFixtureWithBlob(t, 0, false, true)
		var available atomic.Bool
		installColumnStorePeerDas(t, f, func(uint64, common.Hash) (bool, error) {
			return available.Load(), nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
		require.True(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot))

		forkChoice := &headReadCounter{ForkChoiceStore: f.cfg.forkChoice}
		run := startSleepForSlotAtTip(t, f, forkChoice)
		synctest.Wait()
		for range 128 {
			f.cfg.emitter.NotifyDataColumnStored(f.tipRoot)
			synctest.Wait()
		}
		require.Zero(t, forkChoice.headReads.Load(), "columns that leave the data unavailable must not cost a head read")
		require.Zero(t, forkChoice.columnPasses.Load())
		require.False(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))

		available.Store(true)
		f.cfg.emitter.NotifyDataColumnStored(f.tipRoot)
		synctest.Wait()
		require.EqualValues(t, 1, forkChoice.headReads.Load())
		require.EqualValues(t, 1, forkChoice.columnPasses.Load())
		require.True(t, f.cfg.forkChoice.HasEnvelope(f.tipRoot))
		run.stop(t)
	})
}

func TestSleepForSlotColumnWakeReadsNoHeadWhileAnAttemptIsBooked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newForwardSyncTipFixtureWithBlob(t, 0, false, true)
		var available atomic.Bool
		installColumnStorePeerDas(t, f, func(uint64, common.Hash) (bool, error) {
			return available.Load(), nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		require.NoError(t, forwardSync(ctx, log.Root(), f.cfg, Args{}))
		f.graph.dumpErr = errors.New("disk full")

		forkChoice := &headReadCounter{ForkChoiceStore: f.cfg.forkChoice}
		run := startSleepForSlotAtTip(t, f, forkChoice)
		synctest.Wait()
		available.Store(true)
		f.cfg.emitter.NotifyDataColumnStored(f.tipRoot)
		synctest.Wait()
		require.EqualValues(t, 1, forkChoice.headReads.Load())
		require.EqualValues(t, 1, forkChoice.columnPasses.Load())
		require.True(t, f.cfg.forkChoice.HasPendingExecutionPayloadEnvelope(f.tipRoot), "the failed attempt keeps the envelope parked")

		for range 128 {
			f.cfg.emitter.NotifyDataColumnStored(f.tipRoot)
			synctest.Wait()
		}
		require.EqualValues(t, 1, forkChoice.headReads.Load(), "columns stored after a failed attempt must not cost a head read")
		require.EqualValues(t, 1, forkChoice.columnPasses.Load())
		run.stop(t)
	})
}
