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

func TestPeerUsefulnessIgnoresAMarkForAPeerThatIsGone(t *testing.T) {
	u := newPeerUsefulness()
	now := time.Unix(1000, 0)
	u.now = func() time.Time { return now }
	c := remoteConn{remote: usefulPeer}
	u.Connected(nil, c)
	u.Disconnected(connectednessNetwork{connected: false}, c)

	u.mark(usefulPeer)
	_, ok := u.lastUseful(usefulPeer)
	require.False(t, ok, "a late mark keeps no entry for a peer that is gone")

	now = now.Add(2 * time.Hour)
	u.Connected(nil, c)
	last, _ := u.lastUseful(usefulPeer)
	require.Equal(t, now, last, "a reconnected peer gets a full window")
}

// reconnectingNetwork reports the peer as gone, then reconnects it before returning: a Connected
// notification races with the Disconnected one that is checking connectedness.
type reconnectingNetwork struct {
	network.Network
	u *peerUsefulness
	c network.Conn
}

func (n reconnectingNetwork) Connectedness(peer.ID) network.Connectedness {
	done := make(chan struct{})
	go func() {
		n.u.Connected(nil, n.c)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
	}
	return network.NotConnected
}

func TestPeerUsefulnessKeepsAPeerThatReconnectsWhileItsDisconnectIsHandled(t *testing.T) {
	u := newPeerUsefulness()
	c := remoteConn{remote: usefulPeer}
	u.Connected(nil, c)

	u.Disconnected(reconnectingNetwork{u: u, c: c}, c)
	require.Eventually(t, func() bool {
		_, ok := u.lastUseful(usefulPeer)
		return ok
	}, time.Second, 10*time.Millisecond, "the reconnected peer keeps an entry")
}

type peersNetwork struct {
	network.Network
	connected map[peer.ID]bool
}

func (n peersNetwork) Peers() []peer.ID {
	peers := make([]peer.ID, 0, len(n.connected))
	for pid := range n.connected {
		peers = append(peers, pid)
	}
	return peers
}

func (n peersNetwork) Connectedness(pid peer.ID) network.Connectedness {
	if n.connected[pid] {
		return network.Connected
	}
	return network.NotConnected
}

func TestPeerUsefulnessAddsPeersConnectedBeforeItWasRegistered(t *testing.T) {
	u := newPeerUsefulness()
	gone := peer.ID("gone-peer")
	u.addConnected(peersNetwork{connected: map[peer.ID]bool{usefulPeer: true, gone: false}})

	_, ok := u.lastUseful(usefulPeer)
	require.True(t, ok)
	_, ok = u.lastUseful(gone)
	require.False(t, ok, "a peer that disconnected in between is not added")
}
