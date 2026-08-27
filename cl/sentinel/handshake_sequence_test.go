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
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/sentinel/peers"
)

func testSentinel() *Sentinel {
	// The ban bookkeeping this exercises never touches the host.
	return &Sentinel{peers: peers.NewPool(nil), handshakeGate: newHandshakeGate()}
}

func noop(peer.ID) {}

// The pool bans a peer after three handshake failures. Every connection event must run
// through the same serialized path, so the third failure is reached and the ban is applied
// to the next event instead of another handshake being started.
func TestExchangeStatusReachesTheBanAndThenRefusesThePeer(t *testing.T) {
	s := testSentinel()
	pid := peer.ID("peer-a")

	validated, closed, dropped := 0, 0, 0
	validate := func() (bool, error) {
		validated++
		return false, errors.New("stream reset")
	}
	onClose := func(peer.ID) { closed++ }
	onDrop := func(peer.ID) { dropped++ }

	for range 3 {
		require.True(t, s.exchangeStatus(pid, validate, onClose, onDrop),
			"a transport error keeps the peer: it may still serve gossip")
	}
	require.Equal(t, 3, validated)
	require.True(t, s.peers.BanStatus(pid), "three handshake failures must ban the peer")

	require.False(t, s.exchangeStatus(pid, validate, onClose, onDrop))
	require.Equal(t, 3, validated, "a banned peer must not be handshaked again")
	require.Equal(t, 1, closed)
	require.Zero(t, dropped)
	require.Zero(t, s.handshakeGate.inFlight(), "the gate must be released on the banned exit")
}

// A ban installed by a handshake that completed while this event was waiting must be seen:
// the ban is read inside the serialized section, not before it.
func TestExchangeStatusRefusesAPeerBannedWhileTheEventWaited(t *testing.T) {
	s := testSentinel()
	pid := peer.ID("peer-a")

	s.peers.SetBanStatus(pid, true)
	closed := 0
	require.False(t, s.exchangeStatus(pid, func() (bool, error) {
		t.Fatal("a banned peer must not be handshaked")
		return false, nil
	}, func(peer.ID) { closed++ }, noop))
	require.Equal(t, 1, closed)
}

// Serializing per peer is the point: while one handshake is in flight, a second event for
// the same peer must not start another.
func TestExchangeStatusAdmitsOneHandshakePerPeerAtATime(t *testing.T) {
	s := testSentinel()
	pid := peer.ID("peer-a")

	outer := 0
	require.True(t, s.exchangeStatus(pid, func() (bool, error) {
		outer++
		require.False(t, s.exchangeStatus(pid, func() (bool, error) {
			t.Fatal("a second handshake started while one was in flight")
			return false, nil
		}, noop, noop), "the concurrent event must be refused")
		return true, nil
	}, noop, noop))
	require.Equal(t, 1, outer)
	require.Zero(t, s.handshakeGate.inFlight())
}

// A completed handshake reporting the wrong fork is not a failure to retry: the peer is
// dropped outright and must not count toward the ban.
func TestExchangeStatusDropsAForkMismatchWithoutRecordingAFailure(t *testing.T) {
	s := testSentinel()
	pid := peer.ID("peer-a")

	dropped := 0
	require.False(t, s.exchangeStatus(pid, func() (bool, error) { return false, nil },
		noop, func(peer.ID) { dropped++ }))
	require.Equal(t, 1, dropped)
	require.False(t, s.peers.BanStatus(pid))
	require.Zero(t, s.handshakeGate.inFlight(), "the gate must be released on the fork-mismatch exit")
}
