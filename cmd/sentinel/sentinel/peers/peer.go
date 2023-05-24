package peers

import (
	"strings"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Record Peer data.
type Peer struct {
	Penalties int
	Banned    bool
	InRequest bool

	// request info
	lastRequest  time.Time
	successCount int
	useCount     int
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
	log.Debug("[Sentinel Peers] peer penalized", "peer-id", p.pid)
	p.Penalties++
}

func (p *Peer) Forgive() {
	log.Debug("[Sentinel Peers] peer forgiven", "peer-id", p.pid)
	if p.Penalties > 0 {
		p.Penalties--
	}
}

func (p *Peer) MarkUsed() {
	p.useCount++
	log.Debug("[Sentinel Peers] peer used", "peer-id", p.pid, "uses", p.useCount)
	p.lastRequest = time.Now()
}

func (p *Peer) MarkReplied() {
	p.successCount++
	log.Debug("[Sentinel Peers] peer replied", "peer-id", p.pid, "uses", p.useCount, "success", p.successCount)
}

func (p *Peer) IsAvailable() (available bool) {
	if p.Banned {
		return false
	}
	if p.Penalties > MaxBadResponses {
		return false
	}
	if time.Now().Sub(p.lastRequest) > 0*time.Second {
		return true
	}
	return false
}

func (p *Peer) IsBad() (bad bool) {
	if p.Banned {
		bad = true
		return
	}
	bad = p.Penalties > MaxBadResponses
	return
}

var skipReasons = []string{
	"bad handshake",
	"context",
	"security protocol",
	"connect:",
	"dial backoff",
}

func anySetInString(set []string, in string) bool {
	for _, v := range skipReasons {
		if strings.Contains(in, v) {
			return true
		}
	}
	return false
}

func (p *Peer) Disconnect(reason ...string) {
	rzn := strings.Join(reason, " ")
	if !anySetInString(skipReasons, rzn) {
		log.Debug("[Sentinel Peers] disconnecting from peer", "peer-id", p.pid, "reason", strings.Join(reason, " "))
	}
	p.m.host.Peerstore().RemovePeer(p.pid)
	p.m.host.Network().ClosePeer(p.pid)
	p.Penalties = 0
}
func (p *Peer) Ban(reason ...string) {
	log.Debug("[Sentinel Peers] bad peers has been banned", "peer-id", p.pid, "reason", strings.Join(reason, " "))
	p.Banned = true
	p.Disconnect(reason...)
	return
}
