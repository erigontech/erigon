package peers

import (
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Record Peer data.
type Peer struct {
	Penalties int
	Banned    bool

	lastTouched time.Time
	working     chan struct{}

	pid peer.ID
	m   *Manager
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
func (p *Peer) Penalize() {
	p.Penalties++
}

func newPeer() *Peer {
	return &Peer{
		lastTouched: time.Now(),
		working:     make(chan struct{}),
	}
}

type Manager struct {
	host        host.Host
	peers       map[peer.ID]*Peer
	peerTimeout time.Duration

	mu sync.Mutex
}

func NewManager(ctx context.Context, host host.Host) *Manager {
	m := &Manager{
		peerTimeout: 1 * time.Hour,
	}
	go m.run(ctx)
	return m
}

func (m *Manager) IsBadPeer(pid peer.ID) (bad bool) {
	m.WithPeer(pid, func(peer *Peer, newPeer bool) {
		if peer.Banned {
			bad = true
			return
		}
		bad = peer.Penalties > MaxBadResponses
	})
	return
}

func (m *Manager) Forgive(pid peer.ID) {
	m.WithPeer(pid, func(peer *Peer, newPeer bool) {
		if peer.Penalties > 0 {
			peer.Penalties--
		}
	})
}

// WithPeer will get the peer with id and run your lambda with it. it will update the last queried time
// It will do all synchronization and so you can use the peer thread safe inside
func (m *Manager) WithPeer(id peer.ID, fn func(peer *Peer, newPeer bool)) {
	m.mu.Lock()
	p, ok := m.peers[id]
	if !ok {
		p = &Peer{
			pid:         id,
			working:     make(chan struct{}),
			lastTouched: time.Now(),
			m:           m,
			Penalties:   0,
			Banned:      false,
		}
		m.peers[id] = p
	}
	m.mu.Unlock()
	p.working <- struct{}{}
	p.lastTouched = time.Now()
	fn(p, !ok)
	<-p.working
}

func (m *Manager) run(ctx context.Context) {
	m1 := time.NewTimer(1 * time.Minute)
	select {
	case <-m1.C:
		m.gc()
	case <-ctx.Done():
		m1.Stop()
		return
	}
}

func (m *Manager) gc() {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := time.Now()
	for k, v := range m.peers {
		select {
		case v.working <- struct{}{}:
			defer func() { <-v.working }()
		default:
			continue
		}
		if n.Sub(v.lastTouched) > m.peerTimeout {
			delete(m.peers, k)
		}
	}
}
