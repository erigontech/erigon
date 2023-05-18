package peers

import (
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/metrics/methelp"
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
	peers       *lru.Cache[peer.ID, *Peer]
	peerTimeout time.Duration

	mu sync.Mutex
}

func NewManager(ctx context.Context, host host.Host) *Manager {
	c, err := lru.New[peer.ID, *Peer]("beacon_peer_manager", 500)
	if err != nil {
		panic(err)
	}
	m := &Manager{
		peerTimeout: 8 * time.Hour,
		peers:       c,
		host:        host,
	}
	go m.run(ctx)
	return m
}

func (m *Manager) getPeer(id peer.ID) (peer *Peer) {
	m.mu.Lock()
	p, ok := m.peers.Get(id)
	if !ok {
		p = &Peer{
			pid:       id,
			working:   make(chan struct{}, 1),
			m:         m,
			Penalties: 0,
			Banned:    false,
		}
		m.peers.Add(id, p)
	}
	p.lastTouched = time.Now()
	m.mu.Unlock()
	return p
}
func (m *Manager) CtxPeer(ctx context.Context, id peer.ID, fn func(peer *Peer)) error {
	p := m.getPeer(id)
	select {
	case p.working <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	fn(p)
	<-p.working
	return nil
}

func (m *Manager) TryPeer(id peer.ID, fn func(peer *Peer, ok bool)) {
	p := m.getPeer(id)
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
	p := m.getPeer(id)
	p.working <- struct{}{}
	fn(p)
	<-p.working
}

func (m *Manager) run(ctx context.Context) {
	m1 := time.NewTicker(1 * time.Hour)
	for {
		select {
		case <-m1.C:
			m.gc()
		case <-ctx.Done():
			m1.Stop()
			return
		}
	}
}

// any extra GC policies that the lru does not suffice.
// maybe we dont need
func (m *Manager) gc() {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := methelp.NewHistTimer("beacon_peer_manager_gc_time")
	defer t.PutSince()
	deleted := 0
	saw := 0
	n := time.Now()
	for _, k := range m.peers.Keys() {
		v, ok := m.peers.Get(k)
		if !ok {
			continue
		}
		saw = saw + 1
		if n.Sub(v.lastTouched) > m.peerTimeout {
			deleted = deleted + 1
			m.peers.Remove(k)
		}
	}
}
