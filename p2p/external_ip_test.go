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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestUsableExternalIP(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ip   net.IP
		want bool
	}{
		{nil, false},
		{net.IPv4zero, false},
		{net.ParseIP("127.0.0.1"), false},
		{net.ParseIP("10.0.0.1"), false},
		{net.ParseIP("192.168.1.5"), false},
		{net.ParseIP("172.16.4.4"), false},
		{net.ParseIP("169.254.1.1"), false},
		{net.ParseIP("224.0.0.1"), false},
		{net.IPv4bcast, false},
		{net.ParseIP("203.0.113.7"), true},
		{net.ParseIP("8.8.8.8"), true},
	}
	for _, c := range cases {
		if got := usableExternalIP(c.ip); got != c.want {
			t.Errorf("usableExternalIP(%v) = %v, want %v", c.ip, got, c.want)
		}
	}
}

// fakeNAT returns a scripted sequence of ExternalIP results, repeating the last
// entry once exhausted.
type fakeNAT struct {
	results []fakeResult
	calls   int
}

type fakeResult struct {
	ip  net.IP
	err error
}

func (f *fakeNAT) ExternalIP() (net.IP, error) {
	i := f.calls
	if i >= len(f.results) {
		i = len(f.results) - 1
	}
	f.calls++
	return f.results[i].ip, f.results[i].err
}

func (f *fakeNAT) AddMapping(string, int, int, string, time.Duration) error { return nil }
func (f *fakeNAT) DeleteMapping(string, int, int) error                     { return nil }
func (f *fakeNAT) SupportsMapping() bool                                    { return false }
func (f *fakeNAT) String() string                                           { return "fakeNAT" }

func newTracker(results []fakeResult) (*externalIPTracker, *[]net.IP) {
	var applied []net.IP
	tr := &externalIPTracker{
		nat:    &fakeNAT{results: results},
		set:    func(ip net.IP) { applied = append(applied, ip) },
		logger: log.New(),
	}
	return tr, &applied
}

func TestExternalIPTrackerDetectsChange(t *testing.T) {
	t.Parallel()
	ip1 := net.ParseIP("203.0.113.7")
	ip2 := net.ParseIP("198.51.100.9")
	tr, applied := newTracker([]fakeResult{{ip: ip1}, {ip: ip2}})

	if !tr.refresh() {
		t.Fatal("first refresh should apply the resolved IP")
	}
	if !tr.refresh() {
		t.Fatal("second refresh should apply the changed IP")
	}
	if len(*applied) != 2 || !(*applied)[0].Equal(ip1) || !(*applied)[1].Equal(ip2) {
		t.Fatalf("expected [%v %v], got %v", ip1, ip2, *applied)
	}
}

func TestExternalIPTrackerSkipsUnchanged(t *testing.T) {
	t.Parallel()
	ip1 := net.ParseIP("203.0.113.7")
	tr, applied := newTracker([]fakeResult{{ip: ip1}, {ip: ip1}})

	if !tr.refresh() {
		t.Fatal("first refresh should apply the resolved IP")
	}
	if tr.refresh() {
		t.Fatal("second refresh with same IP must not re-apply")
	}
	if len(*applied) != 1 {
		t.Fatalf("expected a single apply, got %v", *applied)
	}
}

func TestExternalIPTrackerRejectsUnusable(t *testing.T) {
	t.Parallel()
	tr, applied := newTracker([]fakeResult{{ip: net.ParseIP("192.168.1.5")}})
	if tr.refresh() {
		t.Fatal("a private IP must not be applied")
	}
	if len(*applied) != 0 {
		t.Fatalf("expected no apply, got %v", *applied)
	}
}

func TestExternalIPTrackerIgnoresError(t *testing.T) {
	t.Parallel()
	tr, applied := newTracker([]fakeResult{{err: errors.New("stun unreachable")}})
	if tr.refresh() {
		t.Fatal("a resolution error must not be applied")
	}
	if len(*applied) != 0 {
		t.Fatalf("expected no apply, got %v", *applied)
	}
}

func TestExternalIPTrackerRunStops(t *testing.T) {
	t.Parallel()
	ip1 := net.ParseIP("203.0.113.7")
	applied := make(chan net.IP, 1)
	tr := &externalIPTracker{
		nat:    &fakeNAT{results: []fakeResult{{ip: ip1}}},
		set:    func(ip net.IP) { applied <- ip },
		logger: log.New(),
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		tr.run(stop, time.Hour)
		close(done)
	}()

	select {
	case got := <-applied:
		if !got.Equal(ip1) {
			t.Fatalf("run applied %v, want %v", got, ip1)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not resolve the initial IP")
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after stop")
	}
}
