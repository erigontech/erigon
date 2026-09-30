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
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
)

type stalledStreamWriter struct {
	header  http.Header
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (w *stalledStreamWriter) Header() http.Header { return w.header }

func (w *stalledStreamWriter) WriteHeader(int) {}

func (w *stalledStreamWriter) Flush() {}

func (w *stalledStreamWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(b), nil
}

func TestEventStreamSlowClientDoesNotBlockEmitters(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	a := &ApiHandler{emitters: beaconevents.NewEventEmitter(), beaconChainCfg: &cfg}

	w := &stalledStreamWriter{header: http.Header{}, started: make(chan struct{}), release: make(chan struct{})}
	defer close(w.release)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/events?topics=block", nil)
	go a.EventSourceGetV1Events(w, req)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a.emitters.State().SendBlock(&beaconevents.BlockData{}) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
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
