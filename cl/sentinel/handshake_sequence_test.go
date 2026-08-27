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

package sentinel

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/sentinel/peers"
)

func testSentinel() *Sentinel {
	// The ban bookkeeping this exercises never touches the host.
	return &Sentinel{peers: peers.NewPool(nil), handshakeGate: newHandshakeGate()}
}

func noop(peer.ID) {}

// banObserver wraps the pool's BanStatus and counts every read that happens without this
// peer's handshake slot being held. Reading the ban before admission is the defect under
// test, and it is invisible to a test that only checks outcomes. Violations are counted
// rather than asserted on the spot: this runs on handshake goroutines, where a failed
// require would Goexit and deadlock the test instead of reporting.
type banObserver struct {
	s          *Sentinel
	violations atomic.Int64
}

func (b *banObserver) read(pid peer.ID) bool {
	if !b.s.handshakeGate.holds(pid) {
		b.violations.Add(1)
	}
	return b.s.peers.BanStatus(pid)
}

func (b *banObserver) assertAlwaysHeld(t *testing.T) {
	t.Helper()
	require.Zero(t, b.violations.Load(),
		"the ban must be read while this peer's handshake slot is held, not before admission")
}

func waitForInFlight(t *testing.T, s *Sentinel, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.handshakeGate.inFlight() != want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d handshake(s) in flight, have %d", want, s.handshakeGate.inFlight())
		}
		time.Sleep(time.Millisecond)
	}
}

// The full sequence: one handshake paused mid-flight, a second event for the same peer
// arriving while it runs, an unrelated peer proceeding regardless, three failures reaching
// the pool ban, and the next event for the banned peer closing it without a handshake.
func TestHandshakeSequenceSerializesPerPeerAndReachesTheBan(t *testing.T) {
	s := testSentinel()
	obs := &banObserver{s: s}
	banned := obs.read
	pidA, pidB := peer.ID("peer-a"), peer.ID("peer-b")

	var validatedA, validatedB, closedA int
	var mu sync.Mutex
	countA := func() { mu.Lock(); validatedA++; mu.Unlock() }

	resume := make(chan struct{})
	entered := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		// First event for peer A: holds the slot until released.
		s.exchangeStatus(pidA, banned, func() (bool, error) {
			countA()
			close(entered)
			<-resume
			return false, errors.New("stream reset")
		}, noop, noop)
	})

	<-entered
	waitForInFlight(t, s, 1)

	// A second event for the same peer, arriving while the first handshake is in flight.
	require.False(t, s.exchangeStatus(pidA, banned, func() (bool, error) {
		t.Error("a second handshake started for a peer already being handshaked")
		return false, nil
	}, noop, noop))

	// An unrelated peer must not be held up by peer A's in-flight handshake.
	require.True(t, s.exchangeStatus(pidB, banned, func() (bool, error) {
		validatedB++
		return true, nil
	}, noop, noop))
	require.Equal(t, 1, validatedB)

	close(resume)
	wg.Wait()

	// Two more failures reach the pool's three-strike ban.
	for range 2 {
		require.True(t, s.exchangeStatus(pidA, banned, func() (bool, error) {
			countA()
			return false, errors.New("stream reset")
		}, noop, noop))
	}
	require.Equal(t, 3, validatedA)
	require.True(t, s.peers.BanStatus(pidA), "three handshake failures must ban the peer")

	// The next event closes the banned peer instead of handshaking it again.
	require.False(t, s.exchangeStatus(pidA, banned, func() (bool, error) {
		t.Error("a banned peer must not be handshaked")
		return false, nil
	}, func(peer.ID) { closedA++ }, noop))
	require.Equal(t, 3, validatedA)
	require.Equal(t, 1, closedA)
	require.Zero(t, s.handshakeGate.inFlight(), "every exit path must release the slot")
	obs.assertAlwaysHeld(t)
}

// The connection handler must route through the serialized path: a banned peer entering
// handleNewConnection is closed without its handshake being attempted.
func TestHandleNewConnectionRefusesABannedPeerWithoutHandshaking(t *testing.T) {
	s := testSentinel()
	s.p2p = stubP2P{host: newTestHost(t)}
	s.cfg = &SentinelConfig{P2PConfig: p2p.P2PConfig{MaxPeerCount: 100}}
	pidA := peer.ID("peer-a")
	s.peers.SetBanStatus(pidA, true)

	require.False(t, s.handleNewConnection(pidA, func() (bool, error) {
		t.Error("a banned peer must not be handshaked")
		return false, nil
	}))
	require.Zero(t, s.handshakeGate.inFlight())
}

// A peer that is not banned reaches its handshake through the handler.
func TestHandleNewConnectionHandshakesAnUnbannedPeer(t *testing.T) {
	s := testSentinel()
	s.p2p = stubP2P{host: newTestHost(t)}
	s.cfg = &SentinelConfig{P2PConfig: p2p.P2PConfig{MaxPeerCount: 100}}

	validated := 0
	require.True(t, s.handleNewConnection(peer.ID("peer-a"), func() (bool, error) {
		validated++
		return true, nil
	}))
	require.Equal(t, 1, validated)
}

// A completed handshake reporting the wrong fork is dropped and must not count toward the ban.
func TestExchangeStatusDropsAForkMismatchWithoutRecordingAFailure(t *testing.T) {
	s := testSentinel()
	pid := peer.ID("peer-a")

	dropped := 0
	obs := &banObserver{s: s}
	require.False(t, s.exchangeStatus(pid, obs.read,
		func() (bool, error) { return false, nil },
		noop, func(peer.ID) { dropped++ }))
	require.Equal(t, 1, dropped)
	require.False(t, s.peers.BanStatus(pid))
	require.Zero(t, s.handshakeGate.inFlight(), "the fork-mismatch exit must release the slot")
	obs.assertAlwaysHeld(t)
}
