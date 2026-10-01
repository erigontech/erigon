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

package crypto

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/maphash"
)

const keccakCacheMaxInput = 87

type keccakCacheEntry = KeccakCacheEntry

// KeccakCacheEntry is one memoized hash: the input it was computed from and its digest.
type KeccakCacheEntry struct {
	hash common.Hash
	n    uint8
	in   [keccakCacheMaxInput]byte
}

// KeccakLRU is an alternative store for the memo, keyed by the input's maphash.
type KeccakLRU interface {
	Get(key uint64) (*KeccakCacheEntry, bool)
	Add(key uint64, e *KeccakCacheEntry) bool
}

var keccakLRU KeccakLRU

// SetKeccakLRU replaces the fixed table with l; it must be called before any hashing.
func SetKeccakLRU(l KeccakLRU) { keccakLRU, keccakCacheOn = l, l != nil }

var (
	keccakCacheOn    = dbg.EnvBool("KECCAK_CACHE", false)
	keccakCacheStats = dbg.EnvBool("KECCAK_CACHE_STATS", false)
	keccakCacheSlots [1 << 17]atomic.Pointer[keccakCacheEntry]
	keccakHits       atomic.Uint64
	keccakMisses     atomic.Uint64
	keccakBypass     atomic.Uint64
)

func init() {
	if !keccakCacheStats {
		return
	}
	go func() {
		for range time.Tick(10 * time.Second) {
			h, m, b := keccakHits.Load(), keccakMisses.Load(), keccakBypass.Load()
			fmt.Fprintf(os.Stderr, "[keccak-cache] on=%v hits=%d misses=%d bypass(>87B)=%d hit-rate=%.1f%%\n", keccakCacheOn, h, m, b, 100*float64(h)/float64(max(h+m, 1)))
		}
	}()
}

func cachedKeccak256(data []byte) common.Hash {
	if len(data) == 0 || len(data) > keccakCacheMaxInput {
		if keccakCacheStats {
			keccakBypass.Add(1)
		}
		return keccak.Sum256(data)
	}
	key := maphash.Hash(data)
	if keccakLRU != nil {
		return lruKeccak256(key, data)
	}
	slot := &keccakCacheSlots[key&(uint64(len(keccakCacheSlots))-1)]
	if e := slot.Load(); e != nil && int(e.n) == len(data) && bytes.Equal(e.in[:e.n], data) {
		if keccakCacheStats {
			keccakHits.Add(1)
		}
		return e.hash
	}
	if keccakCacheStats {
		keccakMisses.Add(1)
	}
	e := &keccakCacheEntry{hash: keccak.Sum256(data), n: uint8(len(data))}
	copy(e.in[:], data)
	if keccakCacheOn {
		slot.Store(e)
	}
	return e.hash
}

func lruKeccak256(key uint64, data []byte) common.Hash {
	if e, ok := keccakLRU.Get(key); ok && int(e.n) == len(data) && bytes.Equal(e.in[:e.n], data) {
		if keccakCacheStats {
			keccakHits.Add(1)
		}
		return e.hash
	}
	if keccakCacheStats {
		keccakMisses.Add(1)
	}
	e := &keccakCacheEntry{hash: keccak.Sum256(data), n: uint8(len(data))}
	copy(e.in[:], data)
	keccakLRU.Add(key, e)
	return e.hash
}
