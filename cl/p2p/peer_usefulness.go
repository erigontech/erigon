package p2p

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// peerUsefulness remembers when each connected peer last gave us something we kept. A peer starts
// at its connection time, so a new peer gets a full window before it can count as idle.
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
	u.last[pid] = u.now()
}

func (u *peerUsefulness) lastUseful(pid peer.ID) (time.Time, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	t, ok := u.last[pid]
	return t, ok
}

func (u *peerUsefulness) Connected(_ network.Network, c network.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, ok := u.last[c.RemotePeer()]; !ok {
		u.last[c.RemotePeer()] = u.now()
	}
}

func (u *peerUsefulness) Disconnected(n network.Network, c network.Conn) {
	if n.Connectedness(c.RemotePeer()) == network.Connected {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.last, c.RemotePeer())
}

func (u *peerUsefulness) Listen(network.Network, ma.Multiaddr)      {}
func (u *peerUsefulness) ListenClose(network.Network, ma.Multiaddr) {}
