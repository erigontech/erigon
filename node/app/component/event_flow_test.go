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

package component_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/app"
	"github.com/erigontech/erigon/node/app/component"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/app/util"
)

// --- Test ExecPool (implements util.ExecPool) ---

type testPool struct {
	wg sync.WaitGroup
}

func (p *testPool) Exec(task func()) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		task()
	}()
}

func (p *testPool) PoolSize() int  { return 4 }
func (p *testPool) QueueSize() int { return 0 }

var _ util.ExecPool = (*testPool)(nil)

func newTestBus() (event.EventBus, *testPool) {
	pool := &testPool{}
	return event.NewEventBus(pool), pool
}

// --- Event types ---

type FileCreated struct{ Path string }
type FileDeleted struct{ Path string }
type DownloadComplete struct{ Path string }

// --- Event-driven provider ---

type eventProvider struct {
	name     string
	received []interface{}
	mu       sync.Mutex
	tracker  *orderTracker
}

func (p *eventProvider) Configure(ctx context.Context, opts ...app.Option) error {
	p.tracker.record(p.name + ":configure")
	return nil
}
func (p *eventProvider) Initialize(ctx context.Context, opts ...app.Option) error {
	p.tracker.record(p.name + ":initialize")
	return nil
}
func (p *eventProvider) Recover(ctx context.Context) error { return nil }
func (p *eventProvider) Activate(ctx context.Context) error {
	p.tracker.record(p.name + ":activate")
	return nil
}
func (p *eventProvider) Deactivate(ctx context.Context) error {
	p.tracker.record(p.name + ":deactivate")
	return nil
}

func (p *eventProvider) HandleFileCreated(evt FileCreated) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.received = append(p.received, evt)
	p.tracker.record(p.name + ":FileCreated:" + evt.Path)
}

func (p *eventProvider) HandleFileDeleted(evt FileDeleted) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.received = append(p.received, evt)
	p.tracker.record(p.name + ":FileDeleted:" + evt.Path)
}

func (p *eventProvider) getReceived() []interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]interface{}, len(p.received))
	copy(out, p.received)
	return out
}

// --- Functional Tests ---

func TestEventDeliveryBetweenComponents(t *testing.T) {
	bus, _ := newTestBus()

	tracker := &orderTracker{}
	dl := &eventProvider{name: "downloader", tracker: tracker}

	err := bus.SubscribeAsync(dl.HandleFileCreated)
	require.NoError(t, err)

	count := bus.Publish(FileCreated{Path: "headers.seg"})
	require.Equal(t, 1, count)

	bus.WaitAsync()

	received := dl.getReceived()
	require.Len(t, received, 1)
	require.Equal(t, FileCreated{Path: "headers.seg"}, received[0])
}

func TestEventNotDeliveredToUnsubscribed(t *testing.T) {
	bus, _ := newTestBus()

	count := bus.Publish(FileCreated{Path: "headers.seg"})
	require.Equal(t, 0, count, "no subscribers — event should be undelivered (dead letter)")
}

func TestMultipleSubscribers(t *testing.T) {
	bus, _ := newTestBus()

	tracker := &orderTracker{}
	dl := &eventProvider{name: "downloader", tracker: tracker}
	sm := &eventProvider{name: "snap-mgr", tracker: tracker}

	require.NoError(t, bus.SubscribeAsync(dl.HandleFileCreated))
	require.NoError(t, bus.SubscribeAsync(sm.HandleFileCreated))

	count := bus.Publish(FileCreated{Path: "bodies.seg"})
	require.Equal(t, 2, count)

	bus.WaitAsync()

	require.Len(t, dl.getReceived(), 1)
	require.Len(t, sm.getReceived(), 1)
}

func TestEventTypeRouting(t *testing.T) {
	bus, _ := newTestBus()

	tracker := &orderTracker{}
	dl := &eventProvider{name: "downloader", tracker: tracker}

	require.NoError(t, bus.SubscribeAsync(dl.HandleFileCreated))

	// FileDeleted should NOT be received by FileCreated subscriber
	count := bus.Publish(FileDeleted{Path: "old.seg"})
	require.Equal(t, 0, count)

	count = bus.Publish(FileCreated{Path: "new.seg"})
	require.Equal(t, 1, count)

	bus.WaitAsync()

	received := dl.getReceived()
	require.Len(t, received, 1)
	require.Equal(t, FileCreated{Path: "new.seg"}, received[0])
}

// --- Load Tests ---

func TestEventLoadCompleteness(t *testing.T) {
	bus, _ := newTestBus()

	var received atomic.Int64
	handler := func(evt FileCreated) {
		received.Add(1)
	}
	require.NoError(t, bus.SubscribeAsync(handler))

	const N = 10000
	for i := 0; i < N; i++ {
		bus.Publish(FileCreated{Path: "file"})
	}

	bus.WaitAsync()

	require.Equal(t, int64(N), received.Load(), "all events must be delivered under load")
}

func TestEventLoadMultipleSubscribers(t *testing.T) {
	bus, _ := newTestBus()

	var count1, count2 atomic.Int64
	handler1 := func(evt FileCreated) { count1.Add(1) }
	handler2 := func(evt FileCreated) { count2.Add(1) }

	require.NoError(t, bus.SubscribeAsync(handler1))
	require.NoError(t, bus.SubscribeAsync(handler2))

	const N = 5000
	for i := 0; i < N; i++ {
		bus.Publish(FileCreated{Path: "file"})
	}

	bus.WaitAsync()

	require.Equal(t, int64(N), count1.Load())
	require.Equal(t, int64(N), count2.Load())
}

// --- Failure Behavior Tests ---

func TestBlockingHandlerDoesNotBlockPublisher(t *testing.T) {
	bus, _ := newTestBus()

	var slowDone atomic.Bool
	slowHandler := func(evt FileCreated) {
		time.Sleep(100 * time.Millisecond)
		slowDone.Store(true)
	}
	require.NoError(t, bus.SubscribeAsync(slowHandler))

	start := time.Now()
	bus.Publish(FileCreated{Path: "file"})
	elapsed := time.Since(start)

	require.Less(t, elapsed, 50*time.Millisecond,
		"publish must not block on slow async handler")
	require.False(t, slowDone.Load())

	bus.WaitAsync()
	require.True(t, slowDone.Load())
}

func TestPanickingHandlerDoesNotCrashBus(t *testing.T) {
	bus, _ := newTestBus()

	var goodReceived atomic.Bool
	panicHandler := func(evt FileCreated) { panic("boom") }
	goodHandler := func(evt FileCreated) { goodReceived.Store(true) }

	require.NoError(t, bus.SubscribeAsync(panicHandler))
	require.NoError(t, bus.SubscribeAsync(goodHandler))

	bus.Publish(FileCreated{Path: "file"})
	bus.WaitAsync()

	require.True(t, goodReceived.Load(),
		"good handler must receive event despite panic in other handler")
}

func TestSyncHandlerTotalOrdering(t *testing.T) {
	bus, _ := newTestBus()

	var order []int
	var mu sync.Mutex

	for i := 0; i < 5; i++ {
		idx := i
		handler := func(evt FileCreated) {
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
		}
		require.NoError(t, bus.Subscribe(handler))
	}

	bus.Publish(FileCreated{Path: "file"})

	// Sync handlers execute within Publish under the bus lock —
	// they should execute in registration order.
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int{0, 1, 2, 3, 4}, order,
		"sync handlers must execute in registration order (total ordering)")
}

// --- Component Lifecycle + Events Integration ---

func TestComponentLifecycleWithEvents(t *testing.T) {
	domain, err := component.NewComponentDomain(t.Context(), "evt-lifecycle")
	require.NoError(t, err)

	tracker := &orderTracker{}

	storage, err := component.NewComponent[eventProvider](t.Context(),
		component.WithId("storage"),
		component.WithDomain(domain),
		component.WithProvider(&eventProvider{name: "storage", tracker: tracker}))
	require.NoError(t, err)

	dl, err := component.NewComponent[eventProvider](t.Context(),
		component.WithId("downloader"),
		component.WithDomain(domain),
		component.WithProvider(&eventProvider{name: "downloader", tracker: tracker}),
		component.WithDependencies(storage))
	require.NoError(t, err)

	// Start
	err = dl.Activate(t.Context())
	require.NoError(t, err)
	_, err = dl.AwaitState(t.Context(), component.Active)
	require.NoError(t, err)
	require.Equal(t, component.Active, storage.State())

	// Stop
	err = dl.Deactivate(t.Context())
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	_, err = dl.AwaitState(waitCtx, component.Deactivated)
	require.NoError(t, err)
	_, err = storage.AwaitState(waitCtx, component.Deactivated)
	require.NoError(t, err)

	// Verify ordering
	events := tracker.get()
	sAct := indexOf(events, "storage:activate")
	dAct := indexOf(events, "downloader:activate")
	dDeact := indexOf(events, "downloader:deactivate")
	sDeact := indexOf(events, "storage:deactivate")

	require.True(t, sAct < dAct, "storage activates before downloader")
	require.True(t, dDeact < sDeact, "downloader deactivates before storage")
}
