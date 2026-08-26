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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// Every connection event is handled on its own goroutine, so a burst of events for one
// peer produced a burst of concurrent handshakes. The three-strike ban could not stop it:
// all of them completed before any pushed the counter to 3. Measured on a gnosis node at
// 101 attempts against a single peer in two minutes, until libp2p's own dialer refused.
func TestHandshakeGateAdmitsOneAttemptPerPeerAtATime(t *testing.T) {
	g := newHandshakeGate()
	pid := peer.ID("peer-a")

	require.True(t, g.tryAcquire(pid))
	require.False(t, g.tryAcquire(pid), "a second concurrent attempt on the same peer must be refused")

	g.release(pid)
	require.True(t, g.tryAcquire(pid), "the peer is attemptable again once the first finishes")
}

// Serialising per peer must not serialise the whole node: unrelated peers still proceed.
func TestHandshakeGateDoesNotBlockOtherPeers(t *testing.T) {
	g := newHandshakeGate()

	require.True(t, g.tryAcquire(peer.ID("peer-a")))
	require.True(t, g.tryAcquire(peer.ID("peer-b")))
	require.True(t, g.tryAcquire(peer.ID("peer-c")))
}

// The burst is the thing being fixed, so drive it concurrently rather than in sequence.
func TestHandshakeGateUnderConcurrentBurst(t *testing.T) {
	g := newHandshakeGate()
	pid := peer.ID("peer-a")

	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if g.tryAcquire(pid) {
				admitted.Add(1)
			}
		})
	}
	wg.Wait()

	require.Equal(t, int64(1), admitted.Load(), "64 simultaneous events admitted %d attempts", admitted.Load())
}

func TestHandshakeGateForgetsReleasedPeers(t *testing.T) {
	g := newHandshakeGate()
	pid := peer.ID("peer-a")

	require.True(t, g.tryAcquire(pid))
	g.release(pid)

	require.Zero(t, g.inFlight(), "a released peer must not be retained")
}
