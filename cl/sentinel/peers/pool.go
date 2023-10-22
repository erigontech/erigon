package peers

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/common/ring"
	"github.com/libp2p/go-libp2p/core/peer"
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

	// allowedPeers are the peers that are allowed.
	// peers not on this list will be silently discarded
	// when returned, and skipped when requesting
	peerData map[peer.ID]*Item

	bannedPeers map[peer.ID]struct{}
	queue       *ring.Buffer[*Item]

	mu sync.Mutex
}

func NewPool() *Pool {
	return &Pool{
		peerData:    make(map[peer.ID]*Item),
		bannedPeers: map[peer.ID]struct{}{},
		queue:       ring.NewBuffer[*Item](0, 1024),
	}
}

func (p *Pool) BanStatus(pid peer.ID) bool {
	_, ok := p.bannedPeers[pid]
	return ok
}

func (p *Pool) AddPeer(pid peer.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// if peer banned, return immediately
	if _, ok := p.bannedPeers[pid]; ok {
		return
	}
	// if peer already here, return immediately
	if _, ok := p.peerData[pid]; ok {
		return
	}
	newItem := &Item{
		id: pid,
	}
	p.peerData[pid] = newItem
	// add it to our queue as a new item
	p.queue.PushBack(newItem)
}

func (p *Pool) SetBanStatus(pid peer.ID, banned bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if banned {
		p.bannedPeers[pid] = struct{}{}
		delete(p.peerData, pid)
	} else {
		delete(p.bannedPeers, pid)
	}
}

func (p *Pool) RemovePeer(pid peer.ID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.peerData, pid)
}

// returnPeer is an internal function to return per to the pool. assume has lock
func (p *Pool) returnPeer(i *Item) {
	// if peer not in our map, return and do not return peer
	if _, ok := p.peerData[i.id]; !ok {
		return
	}
	// append peer to the end of our ring buffer
	p.queue.PushBack(i)
}

// nextPeer gets next peer, skipping bad peers. assume has lock
func (p *Pool) nextPeer() (i *Item, ok bool) {
	val, ok := p.queue.PopFront()
	if !ok {
		return nil, false
	}
	// if peer been banned, get next peer
	if _, ok := p.bannedPeers[val.id]; ok {
		return p.nextPeer()
	}
	// if peer not in set, get next peer
	if _, ok := p.peerData[val.id]; !ok {
		return p.nextPeer()
	}
	return val, true
}

// Request a peer from the pool
// caller MUST call the done function when done with peer IFF err != nil
func (p *Pool) Request() (pid *Item, done func(), err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	//grab a peer from our ringbuffer
	val, ok := p.queue.PopFront()
	if !ok {
		return nil, nil, fmt.Errorf("no peers? (  :(  > ")
	}
	return val, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		val.uses = val.uses + 1
		p.returnPeer(val)
	}, nil
}
