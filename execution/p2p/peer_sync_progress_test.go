// Copyright 2024 The Erigon Authors
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
	"time"

	"github.com/stretchr/testify/require"
)

func TestPeerMayHaveBlockNum(t *testing.T) {
	t.Parallel()

	psp := peerSyncProgress{
		peerId: PeerIdFromUint64(1),
	}

	// base cases
	require.True(t, psp.peerMayHaveBlockNum(0))
	require.True(t, psp.peerMayHaveBlockNum(1_000))

	psp.blockNumMissing(501)
	require.True(t, psp.peerMayHaveBlockNum(0))
	require.True(t, psp.peerMayHaveBlockNum(200))
	require.True(t, psp.peerMayHaveBlockNum(500))
	require.False(t, psp.peerMayHaveBlockNum(501))
	require.False(t, psp.peerMayHaveBlockNum(1_000))

	// expired timestamp
	psp.minMissingBlockNumTs = psp.minMissingBlockNumTs.Add(-missingBlockNumExpiry).Add(-time.Second)
	require.True(t, psp.peerMayHaveBlockNum(0))
	require.True(t, psp.peerMayHaveBlockNum(200))
	require.True(t, psp.peerMayHaveBlockNum(500))
	require.True(t, psp.peerMayHaveBlockNum(501))
	require.True(t, psp.peerMayHaveBlockNum(1_000))

	// block num present clears previous missing block num if >= missing block num
	psp.blockNumMissing(700)
	require.False(t, psp.peerMayHaveBlockNum(700))
	psp.blockNumPresent(800)
	require.True(t, psp.peerMayHaveBlockNum(700))
}
