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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBALPeerMisses(t *testing.T) {
	t.Parallel()
	var misses balPeerMisses
	p1 := *PeerIdFromUint64(1)
	require.True(t, misses.mayHave(p1, 5))
	misses.mark(p1, 100)
	require.False(t, misses.mayHave(p1, 5))
	require.False(t, misses.mayHave(p1, 100))
	require.True(t, misses.mayHave(p1, 101))
	misses.mark(p1, 50)
	require.False(t, misses.mayHave(p1, 100))
	require.True(t, misses.mayHave(p1, 101))
	require.True(t, misses.mayHave(*PeerIdFromUint64(2), 1))
}
