package peers

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	maxBadPeers       = 50000
	maxPeerRecordSize = 1000
	DefaultMaxPeers   = 33
	MaxBadResponses   = 50
)

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
func (m *Manager) TryPeer(id peer.ID, fn func(peer *Peer, ok bool)) {
	m.mu.Lock()
	p, ok := m.peers[id]
	if !ok {
		p = &Peer{
			pid:       id,
			working:   make(chan struct{}),
			m:         m,
			Penalties: 0,
			Banned:    false,
		}
		m.peers[id] = p
	}
	p.lastTouched = time.Now()
	m.mu.Unlock()
	select {
	case p.working <- struct{}{}:
	default:
		fn(nil, false)
		return
	}
	fn(p, true)
	<-p.working
}

// WithPeer will get the peer with id and run your lambda with it. it will update the last queried time
// It will do all synchronization and so you can use the peer thread safe inside
func (m *Manager) WithPeer(id peer.ID, fn func(peer *Peer)) {
	m.mu.Lock()
	p, ok := m.peers[id]
	if !ok {
		p = &Peer{
			pid:       id,
			working:   make(chan struct{}),
			m:         m,
			Penalties: 0,
			Banned:    false,
		}
		m.peers[id] = p
	}
	p.lastTouched = time.Now()
	m.mu.Unlock()
	p.working <- struct{}{}
	fn(p)
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
