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

package p2p

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestNoopNotifierNeverFires(t *testing.T) {
	t.Parallel()
	n := noopNotifier{}
	select {
	case <-n.Events():
		t.Fatal("noop notifier must never fire")
	case <-time.After(50 * time.Millisecond):
	}
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type fakeNotifier struct{ ch chan struct{} }

func (f *fakeNotifier) Events() <-chan struct{} { return f.ch }
func (f *fakeNotifier) Close() error            { return nil }

// countingNAT is concurrency-safe and counts ExternalIP calls.
type countingNAT struct {
	mu    sync.Mutex
	ips   []net.IP
	calls int
}

func (c *countingNAT) ExternalIP() (net.IP, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.calls
	if i >= len(c.ips) {
		i = len(c.ips) - 1
	}
	c.calls++
	return c.ips[i], nil
}

func (c *countingNAT) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *countingNAT) AddMapping(string, int, int, string, time.Duration) error { return nil }
func (c *countingNAT) DeleteMapping(string, int, int) error                     { return nil }
func (c *countingNAT) SupportsMapping() bool                                    { return false }
func (c *countingNAT) String() string                                           { return "countingNAT" }

func TestNetworkChangeEventTriggersRefresh(t *testing.T) {
	t.Parallel()
	ip1 := net.ParseIP("203.0.113.7")
	ip2 := net.ParseIP("198.51.100.9")
	applied := make(chan net.IP, 4)
	tr := &externalIPTracker{
		nat:    &countingNAT{ips: []net.IP{ip1, ip2}},
		set:    func(ip net.IP) { applied <- ip },
		logger: log.New(),
	}
	fn := &fakeNotifier{ch: make(chan struct{}, 1)}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		tr.runWithNotifier(stop, fn, time.Hour, 10*time.Millisecond)
		close(done)
	}()

	expectApplied(t, applied, ip1) // initial resolve
	fn.ch <- struct{}{}
	expectApplied(t, applied, ip2) // event-driven resolve, after debounce

	close(stop)
	expectClosed(t, done)
}

func TestNetworkChangeEventsCoalesce(t *testing.T) {
	t.Parallel()
	ip1 := net.ParseIP("203.0.113.7")
	nat := &countingNAT{ips: []net.IP{ip1}}
	applied := make(chan net.IP, 1)
	tr := &externalIPTracker{
		nat:    nat,
		set:    func(ip net.IP) { applied <- ip },
		logger: log.New(),
	}
	fn := &fakeNotifier{ch: make(chan struct{}, 16)}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		tr.runWithNotifier(stop, fn, time.Hour, 250*time.Millisecond)
		close(done)
	}()

	expectApplied(t, applied, ip1) // initial resolve -> 1 call

	for range 8 { // burst, buffered, no inter-event sleeps
		fn.ch <- struct{}{}
	}
	waitForCount(t, nat, 2) // the burst coalesces into a single extra resolve

	// No further events arrive, so the count must stay at 2 (coalesced, not per-event).
	time.Sleep(80 * time.Millisecond)
	if got := nat.callCount(); got != 2 {
		t.Fatalf("expected a burst to coalesce into 1 extra resolve (2 calls total), got %d", got)
	}

	close(stop)
	expectClosed(t, done)
}

func waitForCount(t *testing.T, nat *countingNAT, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if nat.callCount() >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d resolves, got %d", want, nat.callCount())
		case <-time.After(time.Millisecond):
		}
	}
}

func expectApplied(t *testing.T, ch <-chan net.IP, want net.IP) {
	t.Helper()
	select {
	case got := <-ch:
		if !got.Equal(want) {
			t.Fatalf("applied %v, want %v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %v to be applied", want)
	}
}

func expectClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("runWithNotifier did not return after stop")
	}
}
