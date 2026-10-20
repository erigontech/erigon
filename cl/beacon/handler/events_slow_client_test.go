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
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
)

type stalledStreamWriter struct {
	header    http.Header
	once      sync.Once
	started   chan struct{}
	release   chan struct{}
	expire    chan struct{}
	abort     chan struct{}
	abortOnce sync.Once

	deadline atomic.Int64
}

func newStalledStreamWriter() *stalledStreamWriter {
	return &stalledStreamWriter{
		header:  http.Header{},
		started: make(chan struct{}),
		release: make(chan struct{}),
		expire:  make(chan struct{}),
		abort:   make(chan struct{}),
	}
}

func (w *stalledStreamWriter) Header() http.Header { return w.header }

func (w *stalledStreamWriter) WriteHeader(int) {}

func (w *stalledStreamWriter) Flush() {}

func (w *stalledStreamWriter) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		w.deadline.Store(0)
		return nil
	}
	w.deadline.Store(t.UnixNano())
	if !t.After(time.Now()) {
		w.abortOnce.Do(func() { close(w.abort) })
	}
	return nil
}

func (w *stalledStreamWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	select {
	case <-w.release:
		return len(b), nil
	case <-w.abort:
		return 0, os.ErrDeadlineExceeded
	case <-w.expire:
		if w.deadline.Load() != 0 {
			return 0, os.ErrDeadlineExceeded
		}
		<-w.release
		return len(b), nil
	}
}

func startEventStream(t *testing.T, w http.ResponseWriter) (*ApiHandler, <-chan struct{}) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	a := &ApiHandler{emitters: beaconevents.NewEventEmitter(), beaconChainCfg: &cfg}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/events?topics=block", nil)
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		a.EventSourceGetV1Events(w, req)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for a.emitters.State().SendBlock(&beaconevents.BlockData{}) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("event stream never subscribed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return a, handlerDone
}

func TestEventStreamSlowClientDoesNotBlockEmitters(t *testing.T) {
	w := newStalledStreamWriter()
	defer close(w.release)
	a, handlerDone := startEventStream(t, w)
	select {
	case <-w.started:
	case <-time.After(5 * time.Second):
		t.Fatal("event stream never wrote")
	}

	sendBlocks(t, a, 1000, "a stalled event stream client blocked the event emitter")
	close(w.expire)
	waitDone(t, handlerDone, "event stream did not close after its write deadline expired")
}

func sendBlocks(t *testing.T, a *ApiHandler, n int, failure string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range n {
			a.emitters.State().SendBlock(&beaconevents.BlockData{})
		}
	}()
	waitDone(t, done, failure)
}

func waitDone(t *testing.T, done <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(failure)
	}
}

func TestEventStreamOverflowClosesStalledStream(t *testing.T) {
	w := newStalledStreamWriter()
	defer close(w.release)
	a, handlerDone := startEventStream(t, w)
	select {
	case <-w.started:
	case <-time.After(5 * time.Second):
		t.Fatal("event stream never wrote")
	}

	sendBlocks(t, a, 2*eventStreamWriteQueueSize, "a stalled event stream client blocked the event emitter")
	waitDone(t, handlerDone, "overflowed event stream did not close while the client was stalled")
}

func TestEventStreamOverflowDoesNotDrainQueueToSlowClient(t *testing.T) {
	w := &slowStreamWriter{ResponseRecorder: httptest.NewRecorder(), delay: 10 * time.Millisecond}
	a, handlerDone := startEventStream(t, w)

	sendBlocks(t, a, 2*eventStreamWriteQueueSize, "a slow event stream client blocked the event emitter")
	waitDone(t, handlerDone, "overflowed event stream kept draining its queue to a slow client")
	require.Less(t, w.writes, eventStreamWriteQueueSize/2)
}

func TestEventStreamQueueRejectsMessagesOverByteBudget(t *testing.T) {
	q := newEventStreamQueue()
	msg := make([]byte, eventStreamWriteQueueBytes/4)

	require.True(t, q.push(make([]byte, 2*eventStreamWriteQueueBytes)), "an empty queue accepts a single oversized message")
	<-q.msgs
	q.release(2 * eventStreamWriteQueueBytes)

	for range 4 {
		require.True(t, q.push(msg))
	}
	require.False(t, q.push(msg))
	q.release(len(msg))
	require.True(t, q.push(msg))
}

type failingStreamWriter struct {
	*httptest.ResponseRecorder
	writes int
}

func (w *failingStreamWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, errors.New("connection reset")
}

type slowStreamWriter struct {
	*httptest.ResponseRecorder
	delay  time.Duration
	writes int
}

func (w *slowStreamWriter) Write(b []byte) (int, error) {
	time.Sleep(w.delay)
	w.writes++
	return len(b), nil
}

func TestEventStreamWriteErrorClosesStream(t *testing.T) {
	w := &failingStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	_, handlerDone := startEventStream(t, w)

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("event stream kept running after a write error")
	}
	require.Equal(t, 1, w.writes)
}

type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	w.deadlines = append(w.deadlines, t)
	return nil
}

func TestWriteEventStreamClearsDeadlineAfterEachWrite(t *testing.T) {
	w := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	q := newEventStreamQueue()
	require.True(t, q.push([]byte("a")))
	require.True(t, q.push([]byte("b")))
	close(q.msgs)

	require.NoError(t, writeEventStream(w, q, nil))
	require.Equal(t, "ab", w.Body.String())
	require.Len(t, w.deadlines, 4)
	for i, d := range w.deadlines {
		require.Equal(t, i%2 == 1, d.IsZero(), "deadline %d", i)
	}
}

func TestWriteEventStreamWithoutDeadlineSupport(t *testing.T) {
	w := httptest.NewRecorder()
	q := newEventStreamQueue()
	require.True(t, q.push([]byte("a")))
	close(q.msgs)

	require.NoError(t, writeEventStream(w, q, nil))
	require.Equal(t, "a", w.Body.String())
}

func TestWriteEventStreamReleasesQueuedBytes(t *testing.T) {
	w := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	q := newEventStreamQueue()
	require.True(t, q.push([]byte("a")))
	require.True(t, q.push([]byte("bc")))
	close(q.msgs)

	require.NoError(t, writeEventStream(w, q, nil))
	require.Zero(t, q.queued.Load())
}

// flushUntilAbortWriter blocks Flush until a past write deadline arrives, so the writer is still inside Flush
// when the handler aborts it and can clear or re-arm the deadline afterwards.
type flushUntilAbortWriter struct {
	header       http.Header
	flushOnce    sync.Once
	flushStarted chan struct{}
	abortOnce    sync.Once
	abort        chan struct{}
	deadline     atomic.Int64
}

func newFlushUntilAbortWriter() *flushUntilAbortWriter {
	return &flushUntilAbortWriter{header: http.Header{}, flushStarted: make(chan struct{}), abort: make(chan struct{})}
}

func (w *flushUntilAbortWriter) Header() http.Header         { return w.header }
func (w *flushUntilAbortWriter) WriteHeader(int)             {}
func (w *flushUntilAbortWriter) Write(b []byte) (int, error) { return len(b), nil }

func (w *flushUntilAbortWriter) Flush() {
	w.flushOnce.Do(func() { close(w.flushStarted) })
	<-w.abort
}

func (w *flushUntilAbortWriter) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		w.deadline.Store(0)
		return nil
	}
	w.deadline.Store(t.UnixNano())
	if !t.After(time.Now()) {
		w.abortOnce.Do(func() { close(w.abort) })
	}
	return nil
}

func TestEventStreamTeardownLeavesPastWriteDeadline(t *testing.T) {
	w := newFlushUntilAbortWriter()
	a, handlerDone := startEventStream(t, w)
	waitDone(t, w.flushStarted, "event stream never flushed")

	sendBlocks(t, a, 2*eventStreamWriteQueueSize, "a stalled event stream client blocked the event emitter")
	waitDone(t, handlerDone, "overflowed event stream did not close")

	d := w.deadline.Load()
	require.NotZero(t, d, "write deadline was cleared after teardown")
	require.False(t, time.Unix(0, d).After(time.Now()), "write deadline was re-armed after teardown")
}
