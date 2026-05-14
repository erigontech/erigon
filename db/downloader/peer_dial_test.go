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

package downloader

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPeerDialIPs guards the same-host loopback-fallback contract: the
// extra 127.0.0.1 candidate is added iff the peer's advertised IP equals
// our own external IP — and never otherwise (so it stays a no-op in real
// deployments).
func TestPeerDialIPs(t *testing.T) {
	pub := net.ParseIP("79.137.89.113")
	other := net.ParseIP("1.2.3.4")
	lo := net.ParseIP("127.0.0.1")

	t.Run("peer != self: single candidate", func(t *testing.T) {
		got := PeerDialIPs(pub, other)
		require.Len(t, got, 1)
		require.True(t, got[0].Equal(pub))
	})

	t.Run("peer == self: adds loopback fallback", func(t *testing.T) {
		got := PeerDialIPs(pub, pub)
		require.Len(t, got, 2)
		require.True(t, got[0].Equal(pub))
		require.True(t, got[1].IsLoopback())
	})

	t.Run("self unknown: single candidate", func(t *testing.T) {
		got := PeerDialIPs(pub, nil)
		require.Len(t, got, 1)
		require.True(t, got[0].Equal(pub))
	})

	t.Run("peer already loopback: no duplicate", func(t *testing.T) {
		got := PeerDialIPs(lo, lo)
		require.Len(t, got, 1)
		require.True(t, got[0].IsLoopback())
	})

	t.Run("nil peer: nil", func(t *testing.T) {
		require.Nil(t, PeerDialIPs(nil, pub))
	})
}
