package peers

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	maxBadPeers     = 10_000
	DefaultMaxPeers = 15
	MaxBadResponses = 10
)

type Peers struct {
	badPeers     *lru.Cache
	badResponses map[peer.ID]int
}

func New() *Peers {
	badPeers, err := lru.New(maxBadPeers)
	if err != nil {
		panic(err)
	}
	return &Peers{
		badPeers:     badPeers,
		badResponses: make(map[peer.ID]int),
	}
}

func (p *Peers) IsBadPeer(pid peer.ID) bool {
	return p.badPeers.Contains(pid)
}

func (p *Peers) IncrementBadResponse(pid peer.ID) {
	if _, has := p.badResponses[pid]; !has {
		p.badResponses[pid] = 1
	}
	p.badResponses[pid]++
	// TODO(Giulio2002): drop peer.
}
