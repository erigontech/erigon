package peers

import (
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
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

func NewManager(host host.Host) *Manager {
	c, err := lru.NewWithEvict[peer.ID, *Peer]("beacon_peer_manager", 1024, func(i peer.ID, p *Peer) {
	})
	if err != nil {
		panic(err)
	}
	m := &Manager{
		peerTimeout: 8 * time.Hour,
		peers:       c,
		host:        host,
	}
	return m
}

func (m *Manager) ListPeers(ctx context.Context) (peer.IDSlice, error) {
	peers := m.host.Peerstore().Peers()
	return peers, nil
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
	if fn == nil {
		return
	}
	p := m.getPeer(id)
	p.working <- struct{}{}
	defer func() {
		<-p.working
	}()
	fn(p)
}
