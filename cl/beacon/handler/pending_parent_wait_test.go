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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func noHeadChange() error { return nil }

// parkedFor reports the envelope parked for the first n polls.
func parkedFor(n int) func() bool {
	polls := 0
	return func() bool {
		polls++
		return polls <= n
	}
}

func TestAwaitGloasPayloadSourceResolvesOnceTheEnvelopeIsNoLongerParked(t *testing.T) {
	resolves := 0
	resolve := func() (executionPayloadSource, error) {
		resolves++
		return executionPayloadSource{gloasPath: gloasPayloadPathFull}, nil
	}
	src, err := awaitGloasPayloadSource(context.Background(), time.Now().Add(time.Second), time.Millisecond, parkedFor(3), noHeadChange, func() {}, resolve)
	require.NoError(t, err)
	require.Equal(t, gloasPayloadPathFull, src.gloasPath)
	require.Equal(t, 1, resolves)
}

// A resolve that races an envelope commit can read the head before the commit and the envelope
// after it, and report EMPTY with nothing parked. The wait must not take that as the decision
// while the cheap parked check still holds.
func TestAwaitGloasPayloadSourceDoesNotEndOnARacedResolve(t *testing.T) {
	resolves := 0
	resolve := func() (executionPayloadSource, error) {
		resolves++
		return executionPayloadSource{gloasPath: gloasPayloadPathEmpty}, nil
	}
	start := time.Now()
	deadline := start.Add(50 * time.Millisecond)
	_, err := awaitGloasPayloadSource(context.Background(), deadline, time.Millisecond, func() bool { return true }, noHeadChange, func() {}, resolve)
	require.NoError(t, err)
	require.False(t, time.Now().Before(deadline), "the wait ended before the cutoff")
	require.Equal(t, 1, resolves, "only the cutoff resolves")
}

func TestAwaitGloasPayloadSourceResolvesOnceMoreAtTheCutoff(t *testing.T) {
	start := time.Now()
	// A poll interval far longer than the deadline: the cutoff itself must end the wait.
	src, err := awaitGloasPayloadSource(context.Background(), start.Add(20*time.Millisecond), time.Second, func() bool { return true }, noHeadChange, func() {}, func() (executionPayloadSource, error) {
		return executionPayloadSource{gloasPath: gloasPayloadPathFull}, nil
	})
	require.NoError(t, err)
	require.Equal(t, gloasPayloadPathFull, src.gloasPath, "a decision that lands at the cutoff is picked up")
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

// Without a resolved source the proposal must not fall back to the one seen before the wait.
func TestAwaitGloasPayloadSourceReturnsAResolveError(t *testing.T) {
	resolveErr := errors.New("state unavailable")
	_, err := awaitGloasPayloadSource(context.Background(), time.Now().Add(time.Second), time.Millisecond, parkedFor(0), noHeadChange, func() {}, func() (executionPayloadSource, error) {
		return executionPayloadSource{}, resolveErr
	})
	require.ErrorIs(t, err, resolveErr)
}

// A block that becomes head during the wait ends it at once instead of at the cutoff.
func TestAwaitGloasPayloadSourceReturnsAHeadChangeMidWait(t *testing.T) {
	checks := 0
	headErr := func() error {
		checks++
		if checks < 3 {
			return nil
		}
		return fmt.Errorf("%w: proposal parent a, current head b", errForkChoiceHeadChanged)
	}
	start := time.Now()
	_, err := awaitGloasPayloadSource(context.Background(), start.Add(3*time.Second), time.Millisecond, func() bool { return true }, headErr, func() {}, func() (executionPayloadSource, error) {
		t.Error("a head change must not wait for the cutoff resolve")
		return executionPayloadSource{}, nil
	})
	require.ErrorIs(t, err, errForkChoiceHeadChanged)
	require.Less(t, time.Since(start), time.Second)
}

func TestAwaitGloasPayloadSourceReturnsAHeadChange(t *testing.T) {
	_, err := awaitGloasPayloadSource(context.Background(), time.Now().Add(time.Second), time.Millisecond, parkedFor(0), noHeadChange, func() {}, func() (executionPayloadSource, error) {
		return executionPayloadSource{}, fmt.Errorf("%w: proposal parent a, current head b", errForkChoiceHeadChanged)
	})
	require.ErrorIs(t, err, errForkChoiceHeadChanged)
}

func TestGloasPendingParentDeadline(t *testing.T) {
	slotStart := time.Unix(1000, 0)
	slot := 12 * time.Second
	require.Equal(t, slotStart.Add(1500*time.Millisecond), gloasPendingParentDeadline(slotStart, slotStart, slot))
	require.Equal(t, slotStart.Add(1000*time.Millisecond), gloasPendingParentDeadline(slotStart.Add(-500*time.Millisecond), slotStart, slot))
	require.Equal(t, slotStart.Add(2*time.Second), gloasPendingParentDeadline(slotStart.Add(time.Second), slotStart, slot))
	// Past the cutoff the wait still allows one retry.
	require.Equal(t, slotStart.Add(3500*time.Millisecond), gloasPendingParentDeadline(slotStart.Add(3*time.Second), slotStart, slot))

	shortSlot := 6 * time.Second
	require.Equal(t, slotStart.Add(750*time.Millisecond), gloasPendingParentDeadline(slotStart, slotStart, shortSlot))
	require.Equal(t, slotStart.Add(time.Second), gloasPendingParentDeadline(slotStart.Add(500*time.Millisecond), slotStart, shortSlot))
}

func TestAwaitGloasPayloadSourceDoesNotWaitForABlockedRetry(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	last := executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}
	start := time.Now()
	src, err := awaitGloasPayloadSource(context.Background(), start.Add(20*time.Millisecond), time.Millisecond, func() bool { return true }, noHeadChange, func() { <-release }, func() (executionPayloadSource, error) { return last, nil })
	require.NoError(t, err)
	require.Equal(t, last, src)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

// Availability changes trigger their own retries, so the wait starts one retry and then only
// polls.
func TestAwaitGloasPayloadSourceStartsOneRetry(t *testing.T) {
	var retries atomic.Int32
	pending := executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}
	_, err := awaitGloasPayloadSource(context.Background(), time.Now().Add(30*time.Millisecond), time.Millisecond, func() bool { return true }, noHeadChange, func() { retries.Add(1) }, func() (executionPayloadSource, error) { return pending, nil })
	require.NoError(t, err)
	require.Equal(t, int32(1), retries.Load())
}

// The retry outlives the wait: a retry in flight at the cutoff keeps its budget instead of
// being cut short by the wait's return.
func TestAwaitPendingParentPayloadLetsTheRetryOutliveTheWait(t *testing.T) {
	ctrl := gomock.NewController(t)
	postState, handler, _, forkchoiceStore, _ := setupGloasPreparationTest(t)
	baseBlockRoot := common.Hash{0x41}
	forkchoiceStore.HeadVal = baseBlockRoot
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
	forkchoiceStore.PendingEnvelopeRoots = map[common.Hash]struct{}{baseBlockRoot: {}}
	postState.SetLatestBlockHash(common.Hash{0xa1})
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{BlockHash: common.Hash{0xb2}, Slot: postState.Slot()})
	// Past the cutoff the wait lasts one retry budget (slot/24, 500 ms), and the retry keeps as much again.
	handler.beaconChainCfg.SecondsPerSlot = 12
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(-time.Hour)).AnyTimes()
	handler.ethClock = clock

	release := make(chan struct{})
	retryCtxErr := make(chan error, 1)
	forkchoiceStore.RetryPendingEnvelopeFunc = func(ctx context.Context, _ common.Hash) {
		<-release
		retryCtxErr <- ctx.Err()
	}
	source, err := handler.resolveExecutionPayloadSource(postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion)
	require.NoError(t, err)
	require.True(t, source.envelopeParked)

	requestCtx, cancelRequest := context.WithCancel(t.Context())
	_, err = handler.awaitPendingParentPayload(requestCtx, postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion)
	require.NoError(t, err)
	cancelRequest()
	close(release)

	select {
	case err := <-retryCtxErr:
		require.NoError(t, err, "the wait's return must not cancel the retry")
	case <-time.After(5 * time.Second):
		t.Fatal("the retry did not finish")
	}
}

func TestResolveProductionPayloadSourceWaitsOnlyForAParkedEnvelope(t *testing.T) {
	for _, parked := range []bool{false, true} {
		postState, handler, _, forkchoiceStore, _ := setupGloasPreparationTest(t)
		baseBlockRoot := common.Hash{0x41}
		forkchoiceStore.HeadVal = baseBlockRoot
		forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
		if parked {
			forkchoiceStore.PendingEnvelopeRoots = map[common.Hash]struct{}{baseBlockRoot: {}}
		}
		postState.SetLatestBlockHash(common.Hash{0xa1})
		postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{BlockHash: common.Hash{0xb2}, Slot: postState.Slot()})
		clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
		clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(-time.Hour)).AnyTimes()
		handler.ethClock = clock
		var retries atomic.Int32
		forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) { retries.Add(1) }

		source, err := handler.resolveProductionPayloadSource(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion)
		require.NoError(t, err)
		require.Equal(t, parked, source.envelopeParked)
		require.Equal(t, parked, retries.Load() > 0, "parked=%v", parked)
	}
}

// An EMPTY head whose envelope is parked is an open decision, so production waits for it
// instead of building on the EMPTY variant right away. A parked copy of an envelope that is
// already persisted is stale and does not count.
func TestEnvelopeParkedFlagsOnlyAnUnpersistedParkedEnvelope(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	head := forkchoice.ForkChoiceNode{Root: common.Hash{1}, PayloadStatus: cltypes.PayloadStatusEmpty}

	require.False(t, handler.envelopeParked(head.Root))
	require.Equal(t, gloasPayloadPathEmpty, handler.gloasPayloadPathForHead(head, 10))
	fcu.PendingEnvelopeRoots = map[common.Hash]struct{}{head.Root: {}}
	require.True(t, handler.envelopeParked(head.Root))
	require.Equal(t, gloasPayloadPathEmpty, handler.gloasPayloadPathForHead(head, 10), "the path stays EMPTY so preparation primes the fallback")
	fcu.SetEnvelope(head.Root, &cltypes.SignedExecutionPayloadEnvelope{})
	require.False(t, handler.envelopeParked(head.Root))

	// An envelope whose apply is still in flight is neither parked nor persisted yet.
	inFlight := common.Hash{2}
	require.False(t, handler.envelopeParked(inFlight))
	fcu.ApplyInFlightRoots = map[common.Hash]struct{}{inFlight: {}}
	require.True(t, handler.envelopeParked(inFlight))
	fcu.SetEnvelope(inFlight, &cltypes.SignedExecutionPayloadEnvelope{})
	require.False(t, handler.envelopeParked(inFlight))

	// An envelope that arrived but was rejected is not waited for.
	rejected := common.Hash{3}
	fcu.ExecutionPayloadReceivedAt = map[common.Hash]time.Time{rejected: time.Now().Add(-time.Second)}
	require.False(t, handler.envelopeParked(rejected))
}

// A head change ends the parent wait; production then runs once more from the new head.
func TestRetryOnceOnHeadChange(t *testing.T) {
	headChanged := fmt.Errorf("%w: proposal parent a, current head b", errForkChoiceHeadChanged)
	for _, tc := range []struct {
		name  string
		errs  []error
		calls int
		err   error
	}{
		{name: "success", errs: []error{nil}, calls: 1},
		{name: "other error", errs: []error{errors.New("boom")}, calls: 1, err: errors.New("boom")},
		{name: "head change then success", errs: []error{headChanged, nil}, calls: 2},
		{name: "head change twice", errs: []error{headChanged, headChanged}, calls: 2, err: errForkChoiceHeadChanged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			result, err := retryOnceOnHeadChange(func() (int, error) {
				calls++
				return calls, tc.errs[calls-1]
			})
			require.Equal(t, tc.calls, calls)
			if tc.err == nil {
				require.NoError(t, err)
				require.Equal(t, tc.calls, result)
				return
			}
			require.ErrorContains(t, err, tc.err.Error())
		})
	}
}
