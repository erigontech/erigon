package epbs

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

type manualRunnerClock struct {
	mu   sync.Mutex
	slot uint64
}

func (c *manualRunnerClock) GetCurrentSlot() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.slot
}

func (c *manualRunnerClock) set(slot uint64) {
	c.mu.Lock()
	c.slot = slot
	c.mu.Unlock()
}

type manualRunnerTicker struct {
	ticks chan time.Time
}

func (t *manualRunnerTicker) Chan() <-chan time.Time { return t.ticks }
func (t *manualRunnerTicker) Stop()                  {}
func (t *manualRunnerTicker) tick()                  { t.ticks <- time.Now() }

type runnerOutcome struct {
	bid *cltypes.SignedExecutionPayloadBid
	err error
}

type runnerCall struct {
	slot uint64
	root common.Hash
}

type recordingPreferencesCoordinator struct {
	mu          sync.Mutex
	calls       []runnerCall
	prunes      []uint64
	outcomes    map[runnerCall][]runnerOutcome
	started     chan runnerCall
	block       bool
	canceled    chan struct{}
	cancelOnce  sync.Once
	release     <-chan struct{}
	mutateFirst bool
}

func (c *recordingPreferencesCoordinator) HandleValidatedPreferences(ctx context.Context, preferences *cltypes.SignedProposerPreferences) (*cltypes.SignedExecutionPayloadBid, error) {
	call := runnerCall{slot: preferences.Message.ProposalSlot, root: preferences.Message.DependentRoot}
	c.mu.Lock()
	c.calls = append(c.calls, call)
	callCount := len(c.calls)
	outcomes := c.outcomes[call]
	var outcome runnerOutcome
	if len(outcomes) > 0 {
		outcome = outcomes[0]
		c.outcomes[call] = outcomes[1:]
	}
	c.mu.Unlock()
	if c.mutateFirst && callCount == 1 {
		preferences.Message.ProposalSlot = math.MaxUint64
		preferences.Message.DependentRoot = common.HexToHash("0xff")
	}
	if c.started != nil {
		c.started <- call
	}
	if c.block {
		select {
		case <-ctx.Done():
			if c.canceled != nil {
				c.cancelOnce.Do(func() { close(c.canceled) })
			}
			return nil, ctx.Err()
		case <-c.release:
			return outcome.bid, outcome.err
		}
	}
	return outcome.bid, outcome.err
}

func (c *recordingPreferencesCoordinator) PruneExpiredBeforeSlot(slot uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prunes = append(c.prunes, slot)
	return 0
}

func (c *recordingPreferencesCoordinator) snapshot() ([]runnerCall, []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]runnerCall(nil), c.calls...), append([]uint64(nil), c.prunes...)
}

func runnerPreferences(slot uint64, root common.Hash) *cltypes.SignedProposerPreferences {
	return &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{ProposalSlot: slot, DependentRoot: root}}
}

func startTestPreferencesRunner(t *testing.T, coordinator *recordingPreferencesCoordinator, clock *manualRunnerClock, maxPending int) (*ValidatedPreferencesRunner, *manualRunnerTicker, context.CancelFunc, <-chan error) {
	t.Helper()
	ticker := &manualRunnerTicker{ticks: make(chan time.Time, 8)}
	runner, err := newValidatedPreferencesRunner(coordinator, clock, maxPending, time.Second, func(time.Duration) runnerTicker { return ticker })
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	return runner, ticker, cancel, done
}

func waitForRunnerCalls(t *testing.T, coordinator *recordingPreferencesCoordinator, count int) []runnerCall {
	t.Helper()
	require.Eventually(t, func() bool {
		calls, _ := coordinator.snapshot()
		return len(calls) >= count
	}, time.Second, time.Millisecond)
	calls, _ := coordinator.snapshot()
	return calls
}

func waitForRunnerIdle(t *testing.T, runner *ValidatedPreferencesRunner) {
	t.Helper()
	require.Eventually(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.active == nil
	}, time.Second, time.Millisecond)
}

func stopTestPreferencesRunner(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestValidatedPreferencesRunnerRetainsOwnedFuturePreferenceUntilEligible(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{outcomes: make(map[runnerCall][]runnerOutcome)}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 4)
	root := common.HexToHash("0x12")
	preferences := runnerPreferences(12, root)
	runner.SubmitValidatedPreferences(preferences)
	preferences.Message.DependentRoot = common.HexToHash("0xff")
	time.Sleep(10 * time.Millisecond)
	calls, _ := coordinator.snapshot()
	require.Empty(t, calls)

	clock.set(11)
	ticker.tick()
	calls = waitForRunnerCalls(t, coordinator, 1)
	require.Equal(t, runnerCall{slot: 12, root: root}, calls[0])
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerCoalescesBySlotAndRoot(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	rootA := common.HexToHash("0xaa")
	rootB := common.HexToHash("0xbb")
	release := make(chan struct{})
	coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
		{slot: 11, root: rootA}: {{bid: new(cltypes.SignedExecutionPayloadBid)}},
		{slot: 11, root: rootB}: {{bid: new(cltypes.SignedExecutionPayloadBid)}},
	}, started: make(chan runnerCall, 2), block: true, release: release}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 4)
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootA))
	require.Equal(t, runnerCall{slot: 11, root: rootA}, <-coordinator.started)
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootA))
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootB))
	close(release)

	waitForRunnerCalls(t, coordinator, 1)
	waitForRunnerIdle(t, runner)
	ticker.tick()
	calls := waitForRunnerCalls(t, coordinator, 2)
	require.ElementsMatch(t, []runnerCall{{slot: 11, root: rootA}, {slot: 11, root: rootB}}, calls)
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerCapacityPrefersNearestDeterministically(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{outcomes: make(map[runnerCall][]runnerOutcome)}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 2)
	rootA := common.HexToHash("0x01")
	rootB := common.HexToHash("0x02")
	rootC := common.HexToHash("0x03")
	runner.SubmitValidatedPreferences(runnerPreferences(20, rootB))
	runner.SubmitValidatedPreferences(runnerPreferences(20, rootA))
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootC))
	rootD := common.HexToHash("0x04")
	runner.SubmitValidatedPreferences(runnerPreferences(30, rootD))

	calls := waitForRunnerCalls(t, coordinator, 1)
	require.Equal(t, runnerCall{slot: 11, root: rootC}, calls[0])
	clock.set(19)
	ticker.tick()
	calls = waitForRunnerCalls(t, coordinator, 2)
	require.Equal(t, runnerCall{slot: 20, root: rootA}, calls[1])
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerRetriesOnlyOnCadence(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	root := common.HexToHash("0x11")
	call := runnerCall{slot: 11, root: root}
	coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
		call: {{err: errors.New("head unavailable")}, {bid: new(cltypes.SignedExecutionPayloadBid)}},
	}}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 2)
	runner.SubmitValidatedPreferences(runnerPreferences(11, root))
	waitForRunnerCalls(t, coordinator, 1)
	waitForRunnerIdle(t, runner)
	calls, _ := coordinator.snapshot()
	require.Len(t, calls, 1)

	ticker.tick()
	waitForRunnerCalls(t, coordinator, 2)
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerTreatsPublishedAndTrackedAuctionsAsTerminal(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	rootA := common.HexToHash("0xaa")
	rootB := common.HexToHash("0xbb")
	callA := runnerCall{slot: 11, root: rootA}
	callB := runnerCall{slot: 11, root: rootB}
	coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
		callA: {{bid: new(cltypes.SignedExecutionPayloadBid), err: errors.New("publish failed")}},
		callB: {{err: ErrAuctionAlreadyTracked}},
	}}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 3)
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootA))
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootB))
	waitForRunnerCalls(t, coordinator, 1)
	waitForRunnerIdle(t, runner)
	ticker.tick()
	waitForRunnerCalls(t, coordinator, 2)
	waitForRunnerIdle(t, runner)
	ticker.tick()
	calls, _ := coordinator.snapshot()
	require.Len(t, calls, 2)
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerPrunesAndCancelsExpiredAttempt(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{
		outcomes: make(map[runnerCall][]runnerOutcome), started: make(chan runnerCall, 1),
		block: true, canceled: make(chan struct{}),
	}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 2)
	runner.SubmitValidatedPreferences(runnerPreferences(10, common.HexToHash("0x10")))
	require.Equal(t, uint64(10), (<-coordinator.started).slot)
	clock.set(11)
	ticker.tick()
	select {
	case <-coordinator.canceled:
	case <-time.After(time.Second):
		t.Fatal("expired attempt was not canceled")
	}
	require.Eventually(t, func() bool {
		_, prunes := coordinator.snapshot()
		return len(prunes) >= 2
	}, time.Second, time.Millisecond)
	_, prunes := coordinator.snapshot()
	require.Equal(t, []uint64{10, 11}, prunes[:2])
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerShutdownCancelsAndWaits(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{
		outcomes: make(map[runnerCall][]runnerOutcome), started: make(chan runnerCall, 1),
		block: true, canceled: make(chan struct{}),
	}
	runner, _, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 1)
	runner.SubmitValidatedPreferences(runnerPreferences(11, common.HexToHash("0x11")))
	<-coordinator.started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-coordinator.canceled:
	default:
		t.Fatal("runner returned before worker observed cancellation")
	}
}

func TestValidatedPreferencesRunnerMaxSlotHasNoWrappedNextSlot(t *testing.T) {
	clock := &manualRunnerClock{slot: math.MaxUint64}
	coordinator := &recordingPreferencesCoordinator{outcomes: make(map[runnerCall][]runnerOutcome)}
	runner, _, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 2)
	runner.SubmitValidatedPreferences(runnerPreferences(0, common.HexToHash("0x01")))
	runner.SubmitValidatedPreferences(runnerPreferences(math.MaxUint64, common.HexToHash("0xff")))
	calls := waitForRunnerCalls(t, coordinator, 1)
	require.Equal(t, uint64(math.MaxUint64), calls[0].slot)
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerRejectsInvalidCapacityAndIgnoresMalformedIngress(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{outcomes: make(map[runnerCall][]runnerOutcome)}
	_, err := NewValidatedPreferencesRunner(coordinator, clock, 0, time.Second)
	require.Error(t, err)
	_, err = NewValidatedPreferencesRunner(coordinator, clock, 1, minValidatedPreferencesRetryInterval-time.Nanosecond)
	require.Error(t, err)
	runner, err := NewValidatedPreferencesRunner(coordinator, clock, 1, time.Second)
	require.NoError(t, err)
	require.NotPanics(t, func() {
		runner.SubmitValidatedPreferences(nil)
		runner.SubmitValidatedPreferences(new(cltypes.SignedProposerPreferences))
	})
}

func TestValidatedPreferencesRunnerLateExpiredIngressCannotOccupyCapacity(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{outcomes: make(map[runnerCall][]runnerOutcome)}
	runner, _, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 1)
	runner.SubmitValidatedPreferences(runnerPreferences(9, common.HexToHash("0x09")))
	runner.SubmitValidatedPreferences(runnerPreferences(11, common.HexToHash("0x11")))
	calls := waitForRunnerCalls(t, coordinator, 1)
	require.Equal(t, uint64(11), calls[0].slot)
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerDropsIngressAfterShutdown(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{outcomes: make(map[runnerCall][]runnerOutcome)}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 1)
	stopTestPreferencesRunner(t, cancel, done)
	runner.SubmitValidatedPreferences(runnerPreferences(11, common.HexToHash("0x11")))

	ctx, secondCancel := context.WithCancel(t.Context())
	defer secondCancel()
	secondDone := make(chan error, 1)
	go func() { secondDone <- runner.Run(ctx) }()
	ticker.tick()
	waitForRunnerIdle(t, runner)
	calls, _ := coordinator.snapshot()
	require.Empty(t, calls)
	require.ErrorIs(t, <-secondDone, errValidatedPreferencesRunnerStopped)
}

func TestValidatedPreferencesRunnerDoesNotStartWithCanceledContext(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{outcomes: make(map[runnerCall][]runnerOutcome)}
	ticker := &manualRunnerTicker{ticks: make(chan time.Time, 1)}
	runner, err := newValidatedPreferencesRunner(coordinator, clock, 1, time.Second, func(time.Duration) runnerTicker { return ticker })
	require.NoError(t, err)
	runner.SubmitValidatedPreferences(runnerPreferences(11, common.HexToHash("0x11")))
	runner.mu.Lock()
	runner.attemptPermit = true
	runner.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	runner.startNext(ctx, 10)
	calls, _ := coordinator.snapshot()
	require.Empty(t, calls)
}

func TestValidatedPreferencesRunnerDoesNotReplaceActiveAttemptAtSameSlot(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	rootA := common.HexToHash("0xaa")
	rootB := common.HexToHash("0xbb")
	callA := runnerCall{slot: 11, root: rootA}
	callB := runnerCall{slot: 11, root: rootB}
	release := make(chan struct{})
	coordinator := &recordingPreferencesCoordinator{
		outcomes: map[runnerCall][]runnerOutcome{callB: {{err: errors.New("stale")}}, callA: {{bid: new(cltypes.SignedExecutionPayloadBid)}}},
		started:  make(chan runnerCall, 2), block: true, canceled: make(chan struct{}), release: release,
	}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 1)
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootB))
	require.Equal(t, callB, <-coordinator.started)
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootA))
	select {
	case <-coordinator.canceled:
		t.Fatal("same-slot ingress canceled active attempt")
	default:
	}
	close(release)
	ticker.tick()
	calls := waitForRunnerCalls(t, coordinator, 2)
	require.Equal(t, callA, calls[1])
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerPrunesFullStaleQueueBeforeAdmission(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{outcomes: make(map[runnerCall][]runnerOutcome)}
	ticker := &manualRunnerTicker{ticks: make(chan time.Time, 1)}
	runner, err := newValidatedPreferencesRunner(coordinator, clock, 2, time.Second, func(time.Duration) runnerTicker { return ticker })
	require.NoError(t, err)
	runner.SubmitValidatedPreferences(runnerPreferences(11, common.HexToHash("0x11")))
	runner.SubmitValidatedPreferences(runnerPreferences(12, common.HexToHash("0x12")))
	clock.set(20)
	runner.SubmitValidatedPreferences(runnerPreferences(21, common.HexToHash("0x21")))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	calls := waitForRunnerCalls(t, coordinator, 1)
	require.Equal(t, uint64(21), calls[0].slot)
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerClonesPreferenceForEveryAttempt(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	root := common.HexToHash("0x11")
	call := runnerCall{slot: 11, root: root}
	coordinator := &recordingPreferencesCoordinator{
		outcomes:    map[runnerCall][]runnerOutcome{call: {{err: errors.New("retry")}, {bid: new(cltypes.SignedExecutionPayloadBid)}}},
		mutateFirst: true,
	}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 1)
	runner.SubmitValidatedPreferences(runnerPreferences(11, root))
	waitForRunnerCalls(t, coordinator, 1)
	waitForRunnerIdle(t, runner)
	ticker.tick()
	calls := waitForRunnerCalls(t, coordinator, 2)
	require.Equal(t, call, calls[1])
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerActiveAttemptDoesNotConsumeWaitingCapacity(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	rootA := common.HexToHash("0xaa")
	rootB := common.HexToHash("0xbb")
	callA := runnerCall{slot: 11, root: rootA}
	callB := runnerCall{slot: 11, root: rootB}
	release := make(chan struct{})
	coordinator := &recordingPreferencesCoordinator{
		outcomes: map[runnerCall][]runnerOutcome{callA: {{err: errors.New("stale")}}, callB: {{bid: new(cltypes.SignedExecutionPayloadBid)}}},
		started:  make(chan runnerCall, 2), block: true, canceled: make(chan struct{}), release: release,
	}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 1)
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootA))
	require.Equal(t, callA, <-coordinator.started)
	runner.SubmitValidatedPreferences(runnerPreferences(11, rootB))
	select {
	case <-coordinator.canceled:
		t.Fatal("same-slot alternate canceled active attempt")
	default:
	}
	close(release)
	ticker.tick()
	calls := waitForRunnerCalls(t, coordinator, 2)
	require.Equal(t, callB, calls[1])
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerSubmitCancelsExpiredActiveAttempt(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	coordinator := &recordingPreferencesCoordinator{
		outcomes: make(map[runnerCall][]runnerOutcome), started: make(chan runnerCall, 2),
		block: true, canceled: make(chan struct{}),
	}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 1)
	runner.SubmitValidatedPreferences(runnerPreferences(10, common.HexToHash("0x10")))
	require.Equal(t, uint64(10), (<-coordinator.started).slot)
	clock.set(11)
	runner.SubmitValidatedPreferences(runnerPreferences(12, common.HexToHash("0x12")))
	select {
	case <-coordinator.canceled:
	case <-time.After(time.Second):
		t.Fatal("expired active attempt was not canceled during admission")
	}
	ticker.tick()
	calls := waitForRunnerCalls(t, coordinator, 2)
	require.Equal(t, uint64(12), calls[1].slot)
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerRecoversOnLaterTickInSameTargetSlot(t *testing.T) {
	clock := &manualRunnerClock{slot: 11}
	root := common.HexToHash("0x11")
	call := runnerCall{slot: 11, root: root}
	coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
		call: {{err: errors.New("first failure")}, {err: errors.New("second failure")}, {bid: new(cltypes.SignedExecutionPayloadBid)}},
	}}
	ticker := &manualRunnerTicker{ticks: make(chan time.Time, 3)}
	runner, err := newValidatedPreferencesRunner(coordinator, clock, 1, time.Second, func(time.Duration) runnerTicker { return ticker })
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	runner.SubmitValidatedPreferences(runnerPreferences(11, root))
	waitForRunnerCalls(t, coordinator, 1)
	waitForRunnerIdle(t, runner)
	calls, _ := coordinator.snapshot()
	require.Len(t, calls, 1)

	ticker.tick()
	waitForRunnerCalls(t, coordinator, 2)
	waitForRunnerIdle(t, runner)
	calls, _ = coordinator.snapshot()
	require.Len(t, calls, 2)

	ticker.tick()
	calls = waitForRunnerCalls(t, coordinator, 3)
	require.Equal(t, call, calls[2])
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerFreshIdentityReplacesBlockedSameSlot(t *testing.T) {
	for _, test := range []struct {
		name  string
		rootA common.Hash
		rootB common.Hash
	}{
		{name: "fresh root sorts lower", rootA: common.HexToHash("0xbb"), rootB: common.HexToHash("0xaa")},
		{name: "fresh root sorts higher", rootA: common.HexToHash("0xaa"), rootB: common.HexToHash("0xbb")},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := &manualRunnerClock{slot: 11}
			callA := runnerCall{slot: 11, root: test.rootA}
			callB := runnerCall{slot: 11, root: test.rootB}
			coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
				callA: {{err: errors.New("retry later")}},
				callB: {{bid: new(cltypes.SignedExecutionPayloadBid)}},
			}}
			runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 1)
			runner.SubmitValidatedPreferences(runnerPreferences(11, test.rootA))
			waitForRunnerCalls(t, coordinator, 1)
			waitForRunnerIdle(t, runner)
			runner.SubmitValidatedPreferences(runnerPreferences(11, test.rootB))
			ticker.tick()
			calls := waitForRunnerCalls(t, coordinator, 2)
			require.Equal(t, callB, calls[1])
			stopTestPreferencesRunner(t, cancel, done)
		})
	}
}

func TestValidatedPreferencesRunnerCadenceBoundsFreshIdentityBurst(t *testing.T) {
	clock := &manualRunnerClock{slot: 11}
	release := make(chan struct{})
	coordinator := &recordingPreferencesCoordinator{
		outcomes: make(map[runnerCall][]runnerOutcome),
		started:  make(chan runnerCall, 5), block: true, release: release,
	}
	roots := []common.Hash{
		common.HexToHash("0x01"),
		common.HexToHash("0x02"),
		common.HexToHash("0x03"),
		common.HexToHash("0x04"),
		common.HexToHash("0x05"),
	}
	for _, root := range roots {
		call := runnerCall{slot: 11, root: root}
		coordinator.outcomes[call] = []runnerOutcome{{bid: new(cltypes.SignedExecutionPayloadBid)}}
	}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, len(roots)-1)
	for _, root := range roots {
		runner.SubmitValidatedPreferences(runnerPreferences(11, root))
	}
	<-coordinator.started
	close(release)
	waitForRunnerIdle(t, runner)
	calls, _ := coordinator.snapshot()
	require.Len(t, calls, 1)

	ticker.tick()
	waitForRunnerCalls(t, coordinator, 2)
	waitForRunnerIdle(t, runner)
	calls, _ = coordinator.snapshot()
	require.Len(t, calls, 2)

	ticker.tick()
	waitForRunnerCalls(t, coordinator, 3)
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerUnattemptedCandidatePrecedesRetry(t *testing.T) {
	for _, test := range []struct {
		name  string
		rootA common.Hash
		rootB common.Hash
	}{
		{name: "retry root sorts lower", rootA: common.HexToHash("0xaa"), rootB: common.HexToHash("0xbb")},
		{name: "retry root sorts higher", rootA: common.HexToHash("0xbb"), rootB: common.HexToHash("0xaa")},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := &manualRunnerClock{slot: 11}
			callA := runnerCall{slot: 11, root: test.rootA}
			callB := runnerCall{slot: 11, root: test.rootB}
			coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
				callA: {{err: errors.New("retryable")}, {err: errors.New("retryable again")}},
				callB: {{bid: new(cltypes.SignedExecutionPayloadBid)}},
			}}
			runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 2)
			runner.SubmitValidatedPreferences(runnerPreferences(11, test.rootA))
			waitForRunnerCalls(t, coordinator, 1)
			waitForRunnerIdle(t, runner)
			runner.SubmitValidatedPreferences(runnerPreferences(11, test.rootB))
			ticker.tick()
			calls := waitForRunnerCalls(t, coordinator, 2)
			require.Equal(t, callB, calls[1])
			stopTestPreferencesRunner(t, cancel, done)
		})
	}
}

func TestValidatedPreferencesRunnerCurrentSlotRetryPrecedesNextSlotFirstAttempts(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	currentRoot := common.HexToHash("0xff")
	currentCall := runnerCall{slot: 10, root: currentRoot}
	nextCallA := runnerCall{slot: 11, root: common.HexToHash("0x01")}
	nextCallB := runnerCall{slot: 11, root: common.HexToHash("0x02")}
	coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
		currentCall: {{err: errors.New("retryable")}, {bid: new(cltypes.SignedExecutionPayloadBid)}},
		nextCallA:   {{bid: new(cltypes.SignedExecutionPayloadBid)}},
		nextCallB:   {{bid: new(cltypes.SignedExecutionPayloadBid)}},
	}}
	runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 3)
	runner.SubmitValidatedPreferences(runnerPreferences(currentCall.slot, currentCall.root))
	waitForRunnerCalls(t, coordinator, 1)
	waitForRunnerIdle(t, runner)
	runner.SubmitValidatedPreferences(runnerPreferences(nextCallA.slot, nextCallA.root))
	runner.SubmitValidatedPreferences(runnerPreferences(nextCallB.slot, nextCallB.root))
	ticker.tick()
	calls := waitForRunnerCalls(t, coordinator, 2)
	require.Equal(t, currentCall, calls[1])
	stopTestPreferencesRunner(t, cancel, done)
}

func TestValidatedPreferencesRunnerFreshCandidatePrecedesUnselectedRetry(t *testing.T) {
	for _, test := range []struct {
		name      string
		freshRoot common.Hash
	}{
		{name: "fresh root sorts lower", freshRoot: common.HexToHash("0x00")},
		{name: "fresh root sorts higher", freshRoot: common.HexToHash("0x03")},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := &manualRunnerClock{slot: 11}
			retryLow := runnerCall{slot: 11, root: common.HexToHash("0x01")}
			retryHigh := runnerCall{slot: 11, root: common.HexToHash("0x02")}
			fresh := runnerCall{slot: 11, root: test.freshRoot}
			coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
				retryLow:  {{err: errors.New("retry low first")}, {err: errors.New("retry low again")}},
				retryHigh: {{err: errors.New("retry high first")}},
				fresh:     {{bid: new(cltypes.SignedExecutionPayloadBid)}},
			}}
			runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 3)
			runner.SubmitValidatedPreferences(runnerPreferences(retryLow.slot, retryLow.root))
			waitForRunnerCalls(t, coordinator, 1)
			waitForRunnerIdle(t, runner)
			runner.SubmitValidatedPreferences(runnerPreferences(retryHigh.slot, retryHigh.root))
			ticker.tick()
			waitForRunnerCalls(t, coordinator, 2)
			waitForRunnerIdle(t, runner)

			ticker.tick()
			calls := waitForRunnerCalls(t, coordinator, 3)
			require.Equal(t, retryLow, calls[2])
			waitForRunnerIdle(t, runner)
			runner.SubmitValidatedPreferences(runnerPreferences(fresh.slot, fresh.root))

			ticker.tick()
			calls = waitForRunnerCalls(t, coordinator, 4)
			require.Equal(t, fresh, calls[3])
			stopTestPreferencesRunner(t, cancel, done)
		})
	}
}

func TestValidatedPreferencesRunnerSlotAdvancePreservesFreshBeforeRetry(t *testing.T) {
	for _, test := range []struct {
		name      string
		retryRoot common.Hash
		freshRoot common.Hash
	}{
		{name: "retry root sorts lower", retryRoot: common.HexToHash("0x01"), freshRoot: common.HexToHash("0x02")},
		{name: "retry root sorts higher", retryRoot: common.HexToHash("0x02"), freshRoot: common.HexToHash("0x01")},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := &manualRunnerClock{slot: 10}
			retry := runnerCall{slot: 11, root: test.retryRoot}
			fresh := runnerCall{slot: 11, root: test.freshRoot}
			coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
				retry: {{err: errors.New("retryable")}},
				fresh: {{bid: new(cltypes.SignedExecutionPayloadBid)}},
			}}
			runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 2)
			runner.SubmitValidatedPreferences(runnerPreferences(retry.slot, retry.root))
			waitForRunnerCalls(t, coordinator, 1)
			waitForRunnerIdle(t, runner)
			runner.SubmitValidatedPreferences(runnerPreferences(fresh.slot, fresh.root))
			clock.set(11)

			ticker.tick()
			calls := waitForRunnerCalls(t, coordinator, 2)
			require.Equal(t, fresh, calls[1])
			stopTestPreferencesRunner(t, cancel, done)
		})
	}
}

func TestValidatedPreferencesRunnerRotatesBlockedRetries(t *testing.T) {
	for _, test := range []struct {
		name       string
		firstRoot  common.Hash
		secondRoot common.Hash
	}{
		{name: "lower root admitted first", firstRoot: common.HexToHash("0x01"), secondRoot: common.HexToHash("0x02")},
		{name: "higher root admitted first", firstRoot: common.HexToHash("0x02"), secondRoot: common.HexToHash("0x01")},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := &manualRunnerClock{slot: 11}
			low := runnerCall{slot: 11, root: common.HexToHash("0x01")}
			high := runnerCall{slot: 11, root: common.HexToHash("0x02")}
			coordinator := &recordingPreferencesCoordinator{outcomes: map[runnerCall][]runnerOutcome{
				low:  {{err: errors.New("retryable")}, {err: errors.New("retryable again")}},
				high: {{err: errors.New("retryable")}, {bid: new(cltypes.SignedExecutionPayloadBid)}},
			}}
			runner, ticker, cancel, done := startTestPreferencesRunner(t, coordinator, clock, 2)
			runner.SubmitValidatedPreferences(runnerPreferences(11, test.firstRoot))
			waitForRunnerCalls(t, coordinator, 1)
			waitForRunnerIdle(t, runner)
			runner.SubmitValidatedPreferences(runnerPreferences(11, test.secondRoot))
			ticker.tick()
			waitForRunnerCalls(t, coordinator, 2)
			waitForRunnerIdle(t, runner)

			ticker.tick()
			calls := waitForRunnerCalls(t, coordinator, 3)
			require.Equal(t, low, calls[2])
			waitForRunnerIdle(t, runner)

			ticker.tick()
			calls = waitForRunnerCalls(t, coordinator, 4)
			require.Equal(t, high, calls[3])
			stopTestPreferencesRunner(t, cancel, done)
		})
	}
}
