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

func NewManager(ctx context.Context, host host.Host) *Manager {
	c, err := lru.NewWithEvict("beacon_peer_manager", 500, func(i peer.ID, p *Peer) {
		p.Disconnect("booted for inactivity")
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

func (m *Manager) GetPeer(id peer.ID) (peer *Peer) {
	m.mu.Lock()
	p, ok := m.peers.Get(id)
	if !ok {
		p = &Peer{
			pid:       id,
			working:   make(chan struct{}, 1),
			m:         m,
			penalties: 0,
			banned:    false,
		}
		m.peers.Add(id, p)
	}
	p.lastTouched = time.Now()
	m.mu.Unlock()
	return p
}
