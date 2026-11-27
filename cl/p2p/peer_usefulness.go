package p2p

import (
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// peerUsefulness remembers when each connected peer last gave us something useful. A peer starts
// at its connection time, so a new peer gets a full window before it can count as idle. Only a
// connection creates an entry: a mark can arrive after the last connection closed.
type peerUsefulness struct {
	mu   sync.Mutex
	last map[peer.ID]time.Time
	now  func() time.Time
}

func newPeerUsefulness() *peerUsefulness {
	return &peerUsefulness{last: make(map[peer.ID]time.Time), now: time.Now}
}

func (u *peerUsefulness) mark(pid peer.ID) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, ok := u.last[pid]; ok {
		u.last[pid] = u.now()
	}
}

func (u *peerUsefulness) lastUseful(pid peer.ID) (time.Time, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	t, ok := u.last[pid]
	return t, ok
}

// Connected starts a new window when the peer has no other open connection, even if the Disconnected
// notification of its last connection has not run yet.
func (u *peerUsefulness) Connected(n network.Network, c network.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !hasOtherOpenConn(n, c) {
		u.last[c.RemotePeer()] = u.now()
		return
	}
	u.addLocked(c.RemotePeer())
}

func hasOtherOpenConn(n network.Network, c network.Conn) bool {
	return slices.ContainsFunc(n.ConnsToPeer(c.RemotePeer()), func(other network.Conn) bool {
		return other != c && !other.IsClosed()
	})
}

// addConnected adds the peers that connected before u was registered for notifications.
func (u *peerUsefulness) addConnected(n network.Network) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, pid := range n.Peers() {
		if n.Connectedness(pid) == network.Connected {
			u.addLocked(pid)
		}
	}
}

func (u *peerUsefulness) addLocked(pid peer.ID) {
	if _, ok := u.last[pid]; !ok {
		u.last[pid] = u.now()
	}
}

// Disconnected checks connectedness under u.mu, so a Connected notification of a reconnect runs
// either before the check, which then keeps the entry, or after the delete, which then adds it back.
func (u *peerUsefulness) Disconnected(n network.Network, c network.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if n.Connectedness(c.RemotePeer()) != network.Connected {
		delete(u.last, c.RemotePeer())
	}
}

func (u *peerUsefulness) Listen(network.Network, ma.Multiaddr)      {}
func (u *peerUsefulness) ListenClose(network.Network, ma.Multiaddr) {}
