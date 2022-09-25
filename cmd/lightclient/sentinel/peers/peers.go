package peers

import (
	"sync"

	"github.com/anacrolix/log"
	lru "github.com/hashicorp/golang-lru"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	maxBadPeers     = 10_000
	DefaultMaxPeers = 33
	MaxBadResponses = 10
)

type Peers struct {
	badPeers     *lru.Cache
	badResponses map[peer.ID]int
	host         host.Host
	mu           sync.Mutex
}

func New(host host.Host) *Peers {
	badPeers, err := lru.New(maxBadPeers)
	if err != nil {
		panic(err)
	}
	return &Peers{
		badPeers:     badPeers,
		badResponses: make(map[peer.ID]int),
		host:         host,
	}
}

func (p *Peers) IsBadPeer(pid peer.ID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.badPeers.Contains(pid)
}

func (p *Peers) Penalize(pid peer.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, has := p.badResponses[pid]; !has {
		p.badResponses[pid] = 1
	}

	p.badResponses[pid]++
	// Drop peer and delete the map element.
	if p.badResponses[pid] > MaxBadResponses {
		p.markBadPeer(pid)
		delete(p.badResponses, pid)
	}
}

func (p *Peers) markBadPeer(pid peer.ID) {
	p.host.Peerstore().RemovePeer(pid)
	p.badPeers.Add(pid, []byte{0})
	log.Info("[Sentinel] peer=%s is marked as bad", pid)
}
