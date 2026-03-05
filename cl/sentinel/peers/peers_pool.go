// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package peers

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type PeeredObject[T any] struct {
	Peer string
	Data T
}

var (
	MaxBadResponses = 50
	ErrNoPeers      = errors.New("no peers")
)

// Item is an item in the pool
type Item struct {
	id    peer.ID
	score atomic.Int64
	uses  int
}

func (i *Item) Id() peer.ID {
	return i.id
}

func (i *Item) String() string {
	return i.id.String()
}

func (i *Item) Score() int {
	return int(i.score.Load())
}

func (i *Item) Add(n int) int {
	return int(i.score.Add(int64(n)))
}

// PeerPool is a pool of peers
type Pool struct {
	host host.Host

	bannedPeers *lru.CacheWithTTL[peer.ID, struct{}]

	mu sync.Mutex
}

func NewPool(h host.Host) *Pool {
	return &Pool{
		host:        h,
		bannedPeers: lru.NewWithTTL[peer.ID, struct{}]("bannedPeers", 100_000, 30*time.Minute),
	}
}

func (p *Pool) BanStatus(pid peer.ID) bool {
	_, ok := p.bannedPeers.Get(pid)
	return ok
}

func (p *Pool) LenBannedPeers() int {
	return p.bannedPeers.Len()
}

func (p *Pool) AddPeer(pid peer.ID) {
}

func (p *Pool) SetBanStatus(pid peer.ID, banned bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if banned {
		p.bannedPeers.Add(pid, struct{}{})
	} else {
		p.bannedPeers.Remove(pid)
	}
}

func (p *Pool) RemovePeer(pid peer.ID) {
}

// Request a peer from the pool
// caller MUST call the done function when done with peer IFF err != nil
func (p *Pool) Request() (pid *Item, done func(), err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peers := p.host.Network().Peers()
	// select a random peer index from the list
	if len(peers) == 0 {
		return nil, nil, ErrNoPeers
	}
	randIndex := rand.Intn(len(peers))
	randPeer := peers[randIndex]
	return &Item{id: randPeer}, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
	}, nil
}
