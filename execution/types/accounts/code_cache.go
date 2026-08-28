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

package accounts

import (
	"bytes"
	"sync"
)

// Deploying the same bytecode at many addresses is the norm — proxies, token
// clones, factory output — and each deployment otherwise keeps its own copy of
// bytes that Code's invariant already forbids mutating. Interning by the
// interned CodeHash collapses them to one.
//
// The budget bounds what interning can hold; a shard over it drops its entries
// rather than evicting one at a time, since a slice already handed out stays
// valid on its own and the only cost of dropping is copying that code again.
const (
	codeCacheShards   = 32
	codeCacheMaxBytes = 128 << 20 // whole-cache budget
	codeCacheShardMax = codeCacheMaxBytes / codeCacheShards

	// Above this a single entry would take a large share of a shard's budget,
	// and the deployments that repeat are far below it.
	codeCacheMaxEntryBytes = 64 << 10
)

type codeCacheShard struct {
	mu    sync.RWMutex
	m     map[CodeHash][]byte
	bytes int
}

var codeCache [codeCacheShards]codeCacheShard

// shardFor spreads by the handle's pointer, which unique.Make has already
// canonicalised per distinct hash.
func shardFor(h CodeHash) *codeCacheShard {
	v := h.Value()
	return &codeCache[uint(v[0])%codeCacheShards]
}

// internCodeBytes returns the cache's copy of code. The cache clones on insert
// rather than adopting: callers pass slices that alias a domain mmap a
// background merge can unmap, and this outlives any of them. The result must
// not be modified — one backing array serves every caller with the same hash.
func internCodeBytes(hash CodeHash, code []byte) []byte {
	if len(code) > codeCacheMaxEntryBytes {
		return code
	}
	s := shardFor(hash)

	s.mu.RLock()
	shared, ok := s.m[hash]
	s.mu.RUnlock()
	if ok {
		return shared
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if shared, ok := s.m[hash]; ok {
		return shared
	}
	if s.m == nil {
		s.m = make(map[CodeHash][]byte)
	}
	if s.bytes+len(code) > codeCacheShardMax {
		clear(s.m)
		s.bytes = 0
	}
	stored := bytes.Clone(code)
	s.m[hash] = stored
	s.bytes += len(stored)
	return stored
}
