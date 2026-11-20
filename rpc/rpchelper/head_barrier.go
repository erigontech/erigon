package rpchelper

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// headsFirstWait caps how long the logs of a block wait for the heads of the same block, so a
// stalled head subscriber cannot hold back the logs of everybody.
const headsFirstWait = 3 * time.Millisecond

// headBarrier lets the logs of a block go out after its head is written on every websocket
// subscription: both compete for the same CPUs and sockets, and the head comes first.
type headBarrier struct {
	mu     sync.Mutex
	blocks map[uint64]*blockHeads
}

// blockHeads counts the head writes still pending for one block. It starts at one, held by
// the header delivery until every subscriber has been counted.
type blockHeads struct {
	pending atomic.Int64
	done    chan struct{}
}

func newBlockHeads() *blockHeads {
	bh := &blockHeads{done: make(chan struct{})}
	bh.pending.Store(1)
	return bh
}

// Written marks one head write of the block as done; nil is a no-op.
func (bh *blockHeads) Written() {
	if bh != nil && bh.pending.Add(-1) == 0 {
		close(bh.done)
	}
}

func (b *headBarrier) get(block uint64) *blockHeads {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.blocks == nil {
		b.blocks = map[uint64]*blockHeads{}
	}
	bh, ok := b.blocks[block]
	if !ok {
		bh = newBlockHeads()
		b.blocks[block] = bh
	}
	for n := range b.blocks {
		if n+2 < block {
			delete(b.blocks, n)
		}
	}
	return bh
}

// wait returns when the heads of block are written, after headsFirstWait, or when ctx ends.
func (b *headBarrier) wait(ctx context.Context, block uint64) {
	t := time.NewTimer(headsFirstWait)
	defer t.Stop()
	select {
	case <-b.get(block).done:
	case <-t.C:
	case <-ctx.Done():
	}
}
