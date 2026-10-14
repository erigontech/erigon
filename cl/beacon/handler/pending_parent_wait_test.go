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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func noHeadChange() error { return nil }

func noRetry(<-chan struct{}) {}

// parkedFor reports the envelope parked for the first n polls.
func parkedFor(n int) func() bool {
	polls := 0
	return func() bool {
		polls++
		return polls <= n
	}
}

func TestAwaitParkedEnvelopeEndsOnceTheEnvelopeIsNoLongerParked(t *testing.T) {
	start := time.Now()
	require.NoError(t, awaitParkedEnvelope(t.Context(), start.Add(5*time.Second), time.Millisecond, parkedFor(3), noHeadChange, noRetry))
	require.Less(t, time.Since(start), time.Second)
}

func TestAwaitParkedEnvelopeEndsAtTheDeadline(t *testing.T) {
	start := time.Now()
	deadline := start.Add(50 * time.Millisecond)
	// A poll interval far longer than the deadline: the deadline itself must end the wait.
	require.NoError(t, awaitParkedEnvelope(t.Context(), deadline, time.Hour, func() bool { return true }, noHeadChange, noRetry))
	require.False(t, time.Now().Before(deadline), "the wait ended before the deadline")
	require.Less(t, time.Since(start), time.Second)
}

// A block that becomes head during the wait ends it at once instead of at the deadline.
func TestAwaitParkedEnvelopeReturnsAHeadChangeMidWait(t *testing.T) {
	checks := 0
	headErr := func() error {
		checks++
		if checks < 3 {
			return nil
		}
		return fmt.Errorf("%w: proposal parent a, current head b", errForkChoiceHeadChanged)
	}
	start := time.Now()
	err := awaitParkedEnvelope(t.Context(), start.Add(5*time.Second), time.Millisecond, func() bool { return true }, headErr, noRetry)
	require.ErrorIs(t, err, errForkChoiceHeadChanged)
	require.Less(t, time.Since(start), time.Second)
}

func TestAwaitParkedEnvelopeReturnsTheCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := awaitParkedEnvelope(ctx, time.Now().Add(5*time.Second), time.Hour, func() bool { return true }, noHeadChange, noRetry)
	require.ErrorIs(t, err, context.Canceled)
}

// A blocked retry does not hold the wait, and the retry is told to stop when the wait returns.
func TestAwaitParkedEnvelopeDoesNotWaitForABlockedRetry(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	stopped := make(chan struct{})
	retry := func(stop <-chan struct{}) {
		<-stop
		close(stopped)
		<-release
	}
	start := time.Now()
	require.NoError(t, awaitParkedEnvelope(t.Context(), start.Add(20*time.Millisecond), time.Millisecond, func() bool { return true }, noHeadChange, retry))
	require.Less(t, time.Since(start), time.Second)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the retry was not told to stop")
	}
}

// On 12 s slots the Gloas attestation is due at 3 s and the payload is collected by 2.25 s; the
// wait ends one publication margin (750 ms) before that.
func TestProposalWaitCutoff(t *testing.T) {
	slotStart := time.Unix(1000, 0)
	require.Equal(t, slotStart.Add(1500*time.Millisecond), proposalWaitCutoff(slotStart, &clparams.MainnetBeaconConfig, clparams.GloasVersion))
}

// pendingParentWaitFixture is an EMPTY head with a parked envelope, whose slot started so that
// the wait cutoff is remaining away. The envelope counts as applied once applied is set.
func pendingParentWaitFixture(t *testing.T, remaining time.Duration) (*ApiHandler, *mock_services.ForkChoiceStorageMock, *state.CachingBeaconState, common.Hash, *atomic.Bool) {
	t.Helper()
	postState, handler, _, forkchoiceStore, _ := setupGloasPreparationTest(t)
	baseBlockRoot := common.Hash{0x41}
	forkchoiceStore.HeadVal = baseBlockRoot
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
	forkchoiceStore.PendingEnvelopeRoots = map[common.Hash]struct{}{baseBlockRoot: {}}
	applied := &atomic.Bool{}
	forkchoiceStore.HasEnvelopeFunc = func(common.Hash) bool { return applied.Load() }
	postState.SetLatestBlockHash(common.Hash{0xa1})
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{BlockHash: common.Hash{0xb2}, Slot: postState.Slot()})
	handler.beaconChainCfg.SecondsPerSlot = 12
	sinceCutoff := proposalWaitCutoff(time.Unix(0, 0), handler.beaconChainCfg, clparams.GloasVersion).Sub(time.Unix(0, 0))
	slotStart := time.Now().Add(remaining - sinceCutoff)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(slotStart).AnyTimes()
	handler.ethClock = clock
	return handler, forkchoiceStore, postState, baseBlockRoot, applied
}

// Columns that complete after the first retry: the wait retries at the pace fork choice sets and
// ends as soon as the envelope is applied.
func TestAwaitPendingParentPayloadRetriesUntilTheEnvelopeIsApplied(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, applied := pendingParentWaitFixture(t, 5*time.Second)
	var retries atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		if retries.Add(1) == 1 {
			return time.Now().Add(20 * time.Millisecond) // the columns are still missing
		}
		applied.Store(true)
		return time.Time{}
	}

	start := time.Now()
	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	require.Less(t, time.Since(start), 2*time.Second)
	require.Equal(t, int32(2), retries.Load())
}

// An apply that keeps failing is retried at its pace until the cutoff, and no longer after it.
func TestAwaitPendingParentPayloadRetriesAFailingApplyUntilTheCutoff(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, _ := pendingParentWaitFixture(t, 300*time.Millisecond)
	var retries atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		retries.Add(1)
		return time.Now().Add(50 * time.Millisecond)
	}

	start := time.Now()
	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	require.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond)
	atCutoff := retries.Load()
	require.GreaterOrEqual(t, atCutoff, int32(3))
	require.LessOrEqual(t, atCutoff, int32(8), "the retries keep their pace")
	time.Sleep(150 * time.Millisecond)
	require.LessOrEqual(t, retries.Load(), atCutoff+1, "the retries stop with the wait")
}

// At or after the cutoff a proposal does not wait at all.
func TestAwaitPendingParentPayloadDoesNotWaitPastTheCutoff(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, _ := pendingParentWaitFixture(t, -time.Millisecond)
	var retries atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		retries.Add(1)
		return time.Now().Add(time.Millisecond)
	}

	start := time.Now()
	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	require.Less(t, time.Since(start), 100*time.Millisecond)
	require.Zero(t, retries.Load())
}

// An apply in flight at the cutoff is not cut short by the wait's return or by the request.
func TestAwaitPendingParentPayloadLetsTheApplyOutliveTheWait(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, _ := pendingParentWaitFixture(t, 50*time.Millisecond)
	release := make(chan struct{})
	applyCtxErr := make(chan error, 1)
	forkchoiceStore.RetryPendingEnvelopeFunc = func(ctx context.Context, _ common.Hash) time.Time {
		<-release
		applyCtxErr <- ctx.Err()
		return time.Time{}
	}

	requestCtx, cancelRequest := context.WithCancel(t.Context())
	require.NoError(t, handler.awaitPendingParentPayload(requestCtx, postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	cancelRequest()
	close(release)

	select {
	case err := <-applyCtxErr:
		require.NoError(t, err, "the wait's return must not cancel the apply")
	case <-time.After(5 * time.Second):
		t.Fatal("the apply did not finish")
	}
}

func TestAwaitPendingParentPayloadWaitsOnlyForAParkedEnvelope(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, _ := pendingParentWaitFixture(t, 5*time.Second)
	forkchoiceStore.PendingEnvelopeRoots = nil
	var retries atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		retries.Add(1)
		return time.Time{}
	}

	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	require.Zero(t, retries.Load())
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

// After a head change, production is retried only once the new head's state is published:
// the state it failed on would fail the same way.
func TestAwaitHeadStateAfterWaitsForTheNewHeadState(t *testing.T) {
	handler, _, postState, staleRoot, _ := pendingParentWaitFixture(t, 5*time.Second)
	handler.syncedData = synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	require.NoError(t, handler.syncedData.OnHeadStateWithBlockRoot(postState, staleRoot))
	newRoot := common.Hash{0x42}
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = handler.syncedData.OnHeadStateWithBlockRoot(postState, newRoot)
	}()

	start := time.Now()
	require.True(t, handler.awaitHeadStateAfter(t.Context(), staleRoot, postState.Slot()+1))
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
	root, _, _ := handler.syncedData.StateHead()
	require.Equal(t, newRoot, root)
}

func TestAwaitHeadStateAfterGivesUpAtTheCutoff(t *testing.T) {
	handler, _, postState, staleRoot, _ := pendingParentWaitFixture(t, 50*time.Millisecond)
	handler.syncedData = synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	require.NoError(t, handler.syncedData.OnHeadStateWithBlockRoot(postState, staleRoot))

	start := time.Now()
	require.False(t, handler.awaitHeadStateAfter(t.Context(), staleRoot, postState.Slot()+1))
	require.Less(t, time.Since(start), time.Second)
}
