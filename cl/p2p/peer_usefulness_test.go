package p2p

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const usefulPeer = peer.ID("useful-peer")

type remoteConn struct {
	network.Conn
	remote peer.ID
}

func (c remoteConn) RemotePeer() peer.ID { return c.remote }

type connectednessNetwork struct {
	network.Network
	connected bool
}

func (n connectednessNetwork) Connectedness(peer.ID) network.Connectedness {
	if n.connected {
		return network.Connected
	}
	return network.NotConnected
}

func TestPeerUsefulnessStartsAtConnectionAndFollowsMarks(t *testing.T) {
	u := newPeerUsefulness()
	now := time.Unix(1000, 0)
	u.now = func() time.Time { return now }
	c := remoteConn{remote: usefulPeer}

	_, ok := u.lastUseful(usefulPeer)
	require.False(t, ok)
	u.Connected(nil, c)
	last, ok := u.lastUseful(usefulPeer)
	require.True(t, ok)
	require.Equal(t, now, last)

	now = now.Add(time.Minute)
	u.Connected(nil, c)
	last, _ = u.lastUseful(usefulPeer)
	require.Equal(t, time.Unix(1000, 0), last, "a second connection does not reset the window")

	u.mark(usefulPeer)
	last, _ = u.lastUseful(usefulPeer)
	require.Equal(t, now, last)
}

func TestPeerUsefulnessForgetsAPeerOnlyWhenItsLastConnectionCloses(t *testing.T) {
	u := newPeerUsefulness()
	c := remoteConn{remote: usefulPeer}
	u.Connected(nil, c)

	u.Disconnected(connectednessNetwork{connected: true}, c)
	_, ok := u.lastUseful(usefulPeer)
	require.True(t, ok)

	u.Disconnected(connectednessNetwork{connected: false}, c)
	_, ok = u.lastUseful(usefulPeer)
	require.False(t, ok)
}
