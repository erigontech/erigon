package peers

import (
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Record Peer data.
type Peer struct {
	Penalties int
	Banned    bool
	InRequest bool

	// gc data
	lastTouched time.Time

	// acts as the mutex. channel used to avoid use of TryLock
	working chan struct{}

	// peer id
	pid peer.ID
	// backref to the manager that owns this peer
	m *Manager
}

func (p *Peer) Penalize() {
	p.Penalties++
}

func (p *Peer) Forgive() {
	if p.Penalties > 0 {
		p.Penalties--
	}
}

func (p *Peer) IsBad() (bad bool) {
	if p.Banned {
		bad = true
		return
	}
	bad = p.Penalties > MaxBadResponses
	return
}

func (p *Peer) Disconnect() {
	log.Trace("[Sentinel Peers] disconnecting from peer", "peer-id", p.pid)
	p.m.host.Peerstore().RemovePeer(p.pid)
	p.m.host.Network().ClosePeer(p.pid)
	p.Penalties = 0
}
func (p *Peer) Ban() {
	log.Debug("[Sentinel Peers] bad peers has been banned", "peer-id", p)
	p.Banned = true
	p.Disconnect()
	return
}
