// Copyright 2026 The Erigon Authors
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

package mdbx

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync"

	"github.com/erigontech/mdbx-go/mdbx"
)

// roTxPool is sharded because a single lock serializes every read-tx begin and end.
// A shard owns up to size txns, idle or in use: an owned txn holds no roTxsLimiter
// slot, so the ownership bound is what keeps the limiter meaningful.
type roTxPool struct {
	shards []roTxPoolShard
	mask   uint32
}

type roTxPoolShard struct {
	mu    sync.Mutex
	idle  []*mdbx.Txn
	owned int
	size  int
	_     [64]byte
}

func newRoTxPool(size int) *roTxPool {
	n := 1 << bits.Len(uint(runtime.GOMAXPROCS(0)-1))
	for n > 1 && n > size {
		n >>= 1
	}
	p := &roTxPool{shards: make([]roTxPoolShard, n), mask: uint32(n - 1)}
	for i := range p.shards {
		p.shards[i].size = (size + i) / n
	}
	return p
}

// get returns an idle txn together with its owning shard, or nil.
func (p *roTxPool) get() (*mdbx.Txn, *roTxPoolShard) {
	start := rand.Uint32()
	for i := uint32(0); i <= p.mask; i++ {
		s := &p.shards[(start+i)&p.mask]
		if tx := s.pop(); tx != nil {
			return tx, s
		}
	}
	return nil, nil
}

// adopt hands a reset txn to a shard with spare ownership; false means the caller keeps it.
func (p *roTxPool) adopt(tx *mdbx.Txn) bool {
	start := rand.Uint32()
	for i := uint32(0); i <= p.mask; i++ {
		if p.shards[(start+i)&p.mask].adopt(tx) {
			return true
		}
	}
	return false
}

func (p *roTxPool) drain() {
	for i := range p.shards {
		p.shards[i].drain()
	}
}

func (s *roTxPoolShard) pop() *mdbx.Txn {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.idle)
	if n == 0 {
		return nil
	}
	tx := s.idle[n-1]
	s.idle[n-1] = nil
	s.idle = s.idle[:n-1]
	return tx
}

// put returns an owned txn; ownership guarantees room.
func (s *roTxPoolShard) put(tx *mdbx.Txn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idle = append(s.idle, tx)
}

func (s *roTxPoolShard) adopt(tx *mdbx.Txn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owned >= s.size {
		return false
	}
	s.owned++
	s.idle = append(s.idle, tx)
	return true
}

// disown drops an owned txn that was aborted.
func (s *roTxPoolShard) disown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owned--
}

func (s *roTxPoolShard) drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tx := range s.idle {
		tx.Abort()
	}
	s.idle = nil
}
