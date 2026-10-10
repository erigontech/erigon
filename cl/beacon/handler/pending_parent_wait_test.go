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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func TestAwaitGloasPayloadSourceReturnsOnceDecided(t *testing.T) {
	calls := 0
	resolve := func() (executionPayloadSource, error) {
		calls++
		if calls < 3 {
			return executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}, nil
		}
		return executionPayloadSource{gloasPath: gloasPayloadPathFull}, nil
	}
	pending := executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}
	src := awaitGloasPayloadSource(context.Background(), time.Now().Add(time.Second), time.Millisecond, pending, func() {}, resolve)
	require.Equal(t, gloasPayloadPathFull, src.gloasPath)
	require.Equal(t, 3, calls)
}

func TestAwaitGloasPayloadSourceGivesUpAtDeadline(t *testing.T) {
	resolve := func() (executionPayloadSource, error) {
		return executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}, nil
	}
	pending := executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}
	start := time.Now()
	// A poll interval far longer than the deadline: the cutoff itself must end the wait.
	src := awaitGloasPayloadSource(context.Background(), start.Add(20*time.Millisecond), time.Second, pending, func() {}, resolve)
	require.True(t, src.envelopeParked)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestAwaitGloasPayloadSourceKeepsLastSourceOnResolveError(t *testing.T) {
	last := executionPayloadSource{gloasPath: gloasPayloadPathPending, head: common.Hash{1}}
	resolve := func() (executionPayloadSource, error) {
		return executionPayloadSource{}, errors.New("head changed")
	}
	src := awaitGloasPayloadSource(context.Background(), time.Now().Add(time.Second), time.Millisecond, last, func() {}, resolve)
	require.Equal(t, last, src)
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
	retry := func() { <-release }
	last := executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}
	resolve := func() (executionPayloadSource, error) { return last, nil }
	start := time.Now()
	src := awaitGloasPayloadSource(context.Background(), start.Add(20*time.Millisecond), time.Millisecond, last, retry, resolve)
	require.Equal(t, last, src)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

// Availability changes trigger their own retries, so the wait starts one retry and then only
// re-resolves.
func TestAwaitGloasPayloadSourceStartsOneRetry(t *testing.T) {
	var retries atomic.Int32
	pending := executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}
	resolve := func() (executionPayloadSource, error) { return pending, nil }
	awaitGloasPayloadSource(context.Background(), time.Now().Add(30*time.Millisecond), time.Millisecond, pending, func() { retries.Add(1) }, resolve)
	require.Equal(t, int32(1), retries.Load())
}

// A decision that lands in the last poll window must not be lost: the wait resolves once more
// after the cutoff.
func TestAwaitGloasPayloadSourceResolvesOnceMoreAtTheCutoff(t *testing.T) {
	parked := executionPayloadSource{gloasPath: gloasPayloadPathEmpty, envelopeParked: true}
	deadline := time.Now().Add(20 * time.Millisecond)
	resolve := func() (executionPayloadSource, error) {
		if time.Now().Before(deadline) {
			return parked, nil
		}
		return executionPayloadSource{gloasPath: gloasPayloadPathFull}, nil
	}
	src := awaitGloasPayloadSource(context.Background(), deadline, time.Second, parked, func() {}, resolve)
	require.Equal(t, gloasPayloadPathFull, src.gloasPath)
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

	handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion, source)
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
