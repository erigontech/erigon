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
