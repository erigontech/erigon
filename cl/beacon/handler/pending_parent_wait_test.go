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

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
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
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	handler.ethClock = clock
	return handler, forkchoiceStore, postState, baseBlockRoot, applied
}

// Columns that complete after the first retry: the wait retries at the pace fork choice sets and
// ends as soon as the envelope is applied.
func TestAwaitPendingParentPayloadRetriesUntilTheEnvelopeIsApplied(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, applied := pendingParentWaitFixture(t, 5*time.Second)
	var attempts atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		if applied.Load() {
			return time.Time{}
		}
		if attempts.Add(1) == 1 {
			return time.Now().Add(20 * time.Millisecond) // the columns are still missing
		}
		applied.Store(true)
		return time.Time{}
	}

	start := time.Now()
	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	require.Less(t, time.Since(start), 2*time.Second)
	require.True(t, applied.Load())
	require.Equal(t, int32(2), attempts.Load())
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
	require.Less(t, time.Since(start), 500*time.Millisecond)
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

// An apply in flight when the wait starts can park the envelope later in the wait: the wait keeps
// retrying, so it applies the envelope once it is parked.
func TestAwaitPendingParentPayloadRetriesAnEnvelopeParkedDuringTheWait(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, applied := pendingParentWaitFixture(t, 5*time.Second)
	forkchoiceStore.PendingEnvelopeRoots = nil
	forkchoiceStore.ApplyInFlightRoots = map[common.Hash]struct{}{baseBlockRoot: {}}
	parkedAt := time.Now().Add(50 * time.Millisecond)
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		if time.Now().Before(parkedAt) {
			return time.Time{} // nothing is parked yet
		}
		applied.Store(true)
		return time.Time{}
	}

	start := time.Now()
	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	require.True(t, applied.Load())
	require.Less(t, time.Since(start), time.Second)
}

// When the PTC votes for a parent from the previous slot already rule out FULL, the proposal
// builds on EMPTY whatever the wait does, so it does not wait.
func TestAwaitPendingParentPayloadSkipsAParentThePtcVotesRuleOut(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, _ := pendingParentWaitFixture(t, 5*time.Second)
	forkchoiceStore.VotesRuleOutFullRoots = map[common.Hash]struct{}{baseBlockRoot: {}}
	var retries atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		retries.Add(1)
		return time.Now().Add(time.Millisecond)
	}
	targetSlot := postState.LatestBlockHeader().Slot + 1

	require.NoError(t, handler.awaitPendingParentPayload(canceledContext(t), postState, baseBlockRoot, targetSlot, clparams.GloasVersion))
	require.Zero(t, retries.Load())
}

// The votes decide only for a parent from the previous slot; an older parent is still waited for.
func TestAwaitPendingParentPayloadIgnoresVotesForAnOlderParent(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, applied := pendingParentWaitFixture(t, 5*time.Second)
	forkchoiceStore.VotesRuleOutFullRoots = map[common.Hash]struct{}{baseBlockRoot: {}}
	var retries atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		retries.Add(1)
		applied.Store(true)
		return time.Time{}
	}
	targetSlot := postState.LatestBlockHeader().Slot + 2

	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, targetSlot, clparams.GloasVersion))
	require.NotZero(t, retries.Load())
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
func TestAwaitHeadStateAfterChangeWaitsForTheNewHeadState(t *testing.T) {
	handler, forkchoiceStore, postState, staleRoot, _ := pendingParentWaitFixture(t, 5*time.Second)
	handler.syncedData = synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	require.NoError(t, handler.syncedData.OnHeadStateWithBlockRoot(postState, staleRoot))
	newRoot := common.Hash{0x42}
	forkchoiceStore.HeadVal = newRoot
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = handler.syncedData.OnHeadStateWithBlockRoot(postState, newRoot)
	}()

	start := time.Now()
	require.True(t, handler.awaitHeadStateAfterChange(t.Context(), postState.Slot()+1))
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
	root, _, _ := handler.syncedData.StateHead()
	require.Equal(t, newRoot, root)
}

func TestAwaitHeadStateAfterChangeGivesUpAtTheCutoff(t *testing.T) {
	handler, forkchoiceStore, postState, staleRoot, _ := pendingParentWaitFixture(t, 50*time.Millisecond)
	handler.syncedData = synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	require.NoError(t, handler.syncedData.OnHeadStateWithBlockRoot(postState, staleRoot))
	forkchoiceStore.HeadVal = common.Hash{0x42} // its state is never published

	start := time.Now()
	require.False(t, handler.awaitHeadStateAfterChange(t.Context(), postState.Slot()+1))
	require.Less(t, time.Since(start), time.Second)
}

// A published state that is not the fork-choice head would fail the same way: the head moved from
// A to B to C and only B's state is published.
func TestAwaitHeadStateAfterChangeWaitsForTheForkChoiceHeadState(t *testing.T) {
	handler, forkchoiceStore, postState, _, _ := pendingParentWaitFixture(t, 100*time.Millisecond)
	handler.syncedData = synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	require.NoError(t, handler.syncedData.OnHeadStateWithBlockRoot(postState, common.Hash{0x42}))
	forkchoiceStore.HeadVal = common.Hash{0x43}

	require.False(t, handler.awaitHeadStateAfterChange(t.Context(), postState.Slot()+1))
}

// After the cutoff the second attempt does not start, even when the new head state is published.
func TestAwaitHeadStateAfterChangeDoesNotRetryPastTheCutoff(t *testing.T) {
	handler, forkchoiceStore, postState, _, _ := pendingParentWaitFixture(t, -time.Millisecond)
	handler.syncedData = synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	newRoot := common.Hash{0x42}
	require.NoError(t, handler.syncedData.OnHeadStateWithBlockRoot(postState, newRoot))
	forkchoiceStore.HeadVal = newRoot

	require.False(t, handler.awaitHeadStateAfterChange(t.Context(), postState.Slot()+1))
}

// slotTimeClock starts every slot at slotStart, so a test controls how far the wait cutoff is.
type slotTimeClock struct {
	eth_clock.EthereumClock
	slotStart time.Time
}

func (c slotTimeClock) GetSlotTime(uint64) time.Time { return c.slotStart }

func (c slotTimeClock) IsSlotCurrentSlotWithMaximumClockDisparity(uint64) bool { return true }

// The envelope of the EMPTY head is parked at the start of production and applied by the wait's
// retry: production then builds on the FULL parent instead of the EMPTY fallback.
func TestProduceBlockBuildsOnTheParentAppliedDuringTheWait(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	handler.beaconChainCfg.FuluForkEpoch = 0
	handler.beaconChainCfg.GloasForkEpoch = 0
	handler.beaconChainCfg.InitializeForkSchedule()
	require.NoError(t, postState.UpgradeToFulu())
	require.NoError(t, postState.UpgradeToGloas())
	postState.GetBuilders().Append(&cltypes.Builder{Pubkey: common.Bytes48{0x42}})
	sinceCutoff := proposalWaitCutoff(time.Unix(0, 0), handler.beaconChainCfg, clparams.GloasVersion).Sub(time.Unix(0, 0))
	handler.ethClock = slotTimeClock{EthereumClock: handler.ethClock, slotStart: time.Now().Add(5*time.Second - sinceCutoff)}

	baseRoot := common.Hash{0x41}
	targetSlot := postState.Slot() + 1
	parentBid := postState.GetLatestExecutionPayloadBid()
	emptyHash, fullHash := parentBid.ParentBlockHash, parentBid.BlockHash
	require.NotEqual(t, emptyHash, fullHash)
	postState.SetLatestBlockHash(emptyHash)

	// The head is EMPTY with its envelope parked until the retry applies it.
	var applied atomic.Bool
	var retries atomic.Int32
	forkchoiceStore.GetHeadNodeFn = func() (forkchoice.ForkChoiceNode, uint64, error) {
		status := cltypes.PayloadStatusEmpty
		if applied.Load() {
			status = cltypes.PayloadStatusFull
		}
		return forkchoice.ForkChoiceNode{Root: baseRoot, PayloadStatus: status}, postState.Slot(), nil
	}
	forkchoiceStore.PendingEnvelopeRoots = map[common.Hash]struct{}{baseRoot: {}}
	forkchoiceStore.HasEnvelopeFunc = func(common.Hash) bool { return applied.Load() }
	forkchoiceStore.ReadEnvelopeFromDiskFunc = func(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
		if !applied.Load() {
			return nil, nil
		}
		return &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
			ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion),
		}}, nil
	}
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		retries.Add(1)
		applied.Store(true)
		return time.Time{}
	}

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("local execution unavailable")).AnyTimes()
	handler.engine = engine
	handler.epbsPool = pool.NewEpbsPool()
	for _, parentHash := range []common.Hash{emptyHash, fullHash} {
		forkchoiceStore.ExecutionPayloadGasLimitMap[parentHash] = parentBid.GasLimit
		bid := &cltypes.SignedExecutionPayloadBid{Message: newTestExecutionPayloadBid(targetSlot, 0, 0)}
		bid.Message.ParentBlockHash = parentHash
		bid.Message.ParentBlockRoot = baseRoot
		bid.Message.FeeRecipient = common.Address{}
		bid.Message.GasLimit = parentBid.GasLimit
		handler.epbsPool.StoreHighestBid(pool.HighestBidKey{Slot: targetSlot, ParentBlockHash: parentHash, ParentBlockRoot: baseRoot}, bid)
	}
	options := &gloasBlockProductionOptions{builderConfig: &cltypes.BuilderConfig{BuilderBoostFactor: 100}}
	ctx := context.WithValue(t.Context(), gloasBlockProductionOptionsKey{}, options)

	_, err := handler.produceBlock(ctx, 100, postState.Slot(), baseRoot, postState, targetSlot, common.Bytes96{}, common.Hash{})
	require.NoError(t, err)
	require.NotZero(t, retries.Load())
	require.NotNil(t, options.selectedBid)
	require.Equal(t, fullHash, options.selectedBid.bid.Message.ParentBlockHash)
}

// When the head moves from A to B and back to A, the published state of A is the head's state
// again, so production is retried at once.
func TestAwaitHeadStateAfterChangeRetriesAtOnceWhenTheHeadReturns(t *testing.T) {
	handler, forkchoiceStore, postState, root, _ := pendingParentWaitFixture(t, 5*time.Second)
	handler.syncedData = synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	require.NoError(t, handler.syncedData.OnHeadStateWithBlockRoot(postState, root))
	forkchoiceStore.HeadVal = root

	require.True(t, handler.awaitHeadStateAfterChange(canceledContext(t), postState.Slot()+1))
}

// canceledContext makes a test fail at once if the code under test enters its wait.
func canceledContext(t *testing.T) context.Context {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}

// notCurrentSlotClock reports every slot as not current, as for a request for a later slot.
func notCurrentSlotClock(t *testing.T) eth_clock.EthereumClock {
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Times(0)
	return clock
}

// A request for a slot that is not current does not wait: it would hold off payload preparation
// until that slot's cutoff.
func TestAwaitPendingParentPayloadDoesNotWaitForASlotThatIsNotCurrent(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, _ := pendingParentWaitFixture(t, 5*time.Second)
	handler.ethClock = notCurrentSlotClock(t)
	var retries atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		retries.Add(1)
		return time.Now().Add(time.Millisecond)
	}

	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	require.Zero(t, retries.Load())
}

func TestAwaitHeadStateAfterChangeDoesNotWaitForASlotThatIsNotCurrent(t *testing.T) {
	handler, forkchoiceStore, postState, staleRoot, _ := pendingParentWaitFixture(t, 5*time.Second)
	handler.ethClock = notCurrentSlotClock(t)
	handler.syncedData = synced_data.NewSyncedDataManager(handler.beaconChainCfg, true)
	require.NoError(t, handler.syncedData.OnHeadStateWithBlockRoot(postState, staleRoot))
	forkchoiceStore.HeadVal = common.Hash{0x42}

	require.False(t, handler.awaitHeadStateAfterChange(t.Context(), postState.Slot()+1))
}

// The time a retry gets back can be another caller's booking, which is dropped if that attempt is
// cut short. The wait checks again at the poll interval instead of sleeping until the dropped time.
func TestAwaitPendingParentPayloadRetriesBeforeAnotherCallersBookingEnds(t *testing.T) {
	handler, forkchoiceStore, postState, baseBlockRoot, applied := pendingParentWaitFixture(t, 5*time.Second)
	var retries atomic.Int32
	forkchoiceStore.RetryPendingEnvelopeFunc = func(context.Context, common.Hash) time.Time {
		if retries.Add(1) == 1 {
			return time.Now().Add(2 * time.Second) // booked by an attempt that is then cut short
		}
		applied.Store(true)
		return time.Time{}
	}

	start := time.Now()
	require.NoError(t, handler.awaitPendingParentPayload(t.Context(), postState, baseBlockRoot, postState.Slot()+1, clparams.GloasVersion))
	require.True(t, applied.Load())
	require.Less(t, time.Since(start), time.Second)
}
