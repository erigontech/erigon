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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
)

type stalledStreamWriter struct {
	header  http.Header
	once    sync.Once
	started chan struct{}
	release chan struct{}
	expire  chan struct{}

	mu       sync.Mutex
	deadline time.Time
}

func newStalledStreamWriter() *stalledStreamWriter {
	return &stalledStreamWriter{
		header:  http.Header{},
		started: make(chan struct{}),
		release: make(chan struct{}),
		expire:  make(chan struct{}),
	}
}

func (w *stalledStreamWriter) Header() http.Header { return w.header }

func (w *stalledStreamWriter) WriteHeader(int) {}

func (w *stalledStreamWriter) Flush() {}

func (w *stalledStreamWriter) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadline = t
	return nil
}

func (w *stalledStreamWriter) deadlineArmed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.deadline.IsZero()
}

func (w *stalledStreamWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	select {
	case <-w.release:
		return len(b), nil
	case <-w.expire:
		if w.deadlineArmed() {
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
	a, _ := startEventStream(t, w)
	select {
	case <-w.started:
	case <-time.After(5 * time.Second):
		t.Fatal("event stream never wrote")
	}

	done := make(chan struct{})
	go func() {
		for range 1000 {
			a.emitters.State().SendBlock(&beaconevents.BlockData{})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled event stream client blocked the event emitter")
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

	for range 2 * eventStreamWriteQueueSize {
		a.emitters.State().SendBlock(&beaconevents.BlockData{})
	}
	close(w.expire)

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("overflowed event stream did not close while the client was stalled")
	}
}

type failingStreamWriter struct {
	header http.Header
	mu     sync.Mutex
	writes int
}

func (w *failingStreamWriter) Header() http.Header { return w.header }

func (w *failingStreamWriter) WriteHeader(int) {}

func (w *failingStreamWriter) Flush() {}

func (w *failingStreamWriter) Write([]byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes++
	return 0, errors.New("connection reset")
}

func TestEventStreamWriteErrorClosesStream(t *testing.T) {
	w := &failingStreamWriter{header: http.Header{}}
	_, handlerDone := startEventStream(t, w)

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("event stream kept running after a write error")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
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
	writeCh := make(chan []byte, 2)
	writeCh <- []byte("a")
	writeCh <- []byte("b")
	close(writeCh)

	require.NoError(t, writeEventStream(w, writeCh))
	require.Equal(t, "ab", w.Body.String())
	require.Len(t, w.deadlines, 4)
	for i, d := range w.deadlines {
		require.Equal(t, i%2 == 1, d.IsZero(), "deadline %d", i)
	}
}

func TestWriteEventStreamWithoutDeadlineSupport(t *testing.T) {
	w := httptest.NewRecorder()
	writeCh := make(chan []byte, 1)
	writeCh <- []byte("a")
	close(writeCh)

	require.NoError(t, writeEventStream(w, writeCh))
	require.Equal(t, "a", w.Body.String())
}
