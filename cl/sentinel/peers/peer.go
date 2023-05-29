package peers

import (
	"strings"
	"sync"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/peer"
)

const USERAGENT_UNKNOWN = "unknown"

// Record Peer data.
type Peer struct {
	penalties int
	banned    bool

	// request info
	lastRequest  time.Time
	successCount int
	useCount     int
	// gc data
	lastTouched time.Time

	mu sync.Mutex

	// peer id
	pid peer.ID

	// acts as the mutex for making requests. channel used to avoid use of TryLock
	working chan struct{}
	// backref to the manager that owns this peer
	m *Manager
}

func (p *Peer) do(fn func(p *Peer)) {
	if fn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(p)
}

func (p *Peer) UserAgent() string {
	rawVer, err := p.m.host.Peerstore().Get(p.pid, "AgentVersion")
	if err == nil {
		if str, ok := rawVer.(string); ok {
			return str
		}
	}
	return USERAGENT_UNKNOWN
}

func (p *Peer) Penalize() {
	log.Debug("[Sentinel Peers] peer penalized", "peer-id", p.pid)
	p.do(func(p *Peer) {
		p.penalties++
	})
}

func (p *Peer) Forgive() {
	log.Debug("[Sentinel Peers] peer forgiven", "peer-id", p.pid)
	p.do(func(p *Peer) {
		if p.penalties > 0 {
			p.penalties--
		}
	})
}

func (p *Peer) MarkUsed() {
	p.do(func(p *Peer) {
		p.useCount++
		p.lastRequest = time.Now()
	})
	log.Debug("[Sentinel Peers] peer used", "peer-id", p.pid, "uses", p.useCount)
}

func (p *Peer) MarkReplied() {
	p.do(func(p *Peer) {
		p.successCount++
	})
	log.Debug("[Sentinel Peers] peer replied", "peer-id", p.pid, "uses", p.useCount, "success", p.successCount)
}

func (p *Peer) IsAvailable() (available bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.banned {
		return false
	}
	if p.penalties > MaxBadResponses {
		return false
	}
	if time.Now().Sub(p.lastRequest) > 0*time.Second {
		return true
	}
	return false
}

func (p *Peer) IsBad() (bad bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.banned {
		bad = true
		return
	}
	bad = p.penalties > MaxBadResponses
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
	p.do(func(p *Peer) {
		p.penalties = 0
	})
}

func (p *Peer) Ban(reason ...string) {
	log.Debug("[Sentinel Peers] bad peers has been banned", "peer-id", p.pid, "reason", strings.Join(reason, " "))
	p.do(func(p *Peer) {
		p.banned = true
	})
	p.Disconnect(reason...)
	return
}
