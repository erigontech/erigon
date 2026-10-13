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

package cache

import (
	"sync"
	"sync/atomic"
)

// bucketWays is how many entries share a bucket: 7 pointers and the meta word
// fill one 64 B cache line.
const bucketWays = 7

// refShift is the meta bit of way 0's CLOCK reference bit; bytes 0-6 hold the
// ways' tags.
const refShift = 56

// bucket is a set of bucketWays slots. Readers take no lock. Writers hold the
// shard mutex, store a slot's pointer before its tag and clear the tag before
// the pointer, so a reader that sees a stale tag gets a key mismatch or a miss.
//
// ponytail: eviction is CLOCK inside one bucket, so a bucket of hot keys
// thrashes while other buckets hold cold ones; raise bucketWays if hit rate drops.
type bucket[T any] struct {
	meta  atomic.Uint64
	slots [bucketWays]atomic.Pointer[entry[T]]
}

func tagOf(h uint64) uint64 { return max(h&0xff, 1) }

func refBit(way int) uint64 { return 1 << (refShift + way) }

// bucketOf maps hash bits 16-47 onto n buckets. Growing n by an integer factor
// k sends old bucket b into buckets [k*b, k*b+k-1], so a migration never
// overfills one.
func bucketOf(h uint64, n int) uint64 { return (h >> 16 & 0xffffffff) * uint64(n) >> 32 }

func (b *bucket[T]) find(h uint64, key []byte) (int, *entry[T]) {
	m := b.meta.Load()
	tag := tagOf(h)
	for way := range bucketWays {
		if m>>(8*way)&0xff != tag {
			continue
		}
		if e := b.slots[way].Load(); e != nil && e.key == string(key) {
			return way, e
		}
	}
	return -1, nil
}

func (b *bucket[T]) touch(way int) {
	if bit := refBit(way); b.meta.Load()&bit == 0 {
		b.meta.Or(bit)
	}
}

func (b *bucket[T]) freeWay() int {
	m := b.meta.Load()
	for way := range bucketWays {
		if m>>(8*way)&0xff == 0 {
			return way
		}
	}
	return -1
}

// setTag publishes way's tag and clears its reference bit. A CAS loop, because
// readers set reference bits concurrently.
func (b *bucket[T]) setTag(way int, tag uint64) {
	for {
		m := b.meta.Load()
		if b.meta.CompareAndSwap(m, m&^(0xff<<(8*way)|refBit(way))|tag<<(8*way)) {
			return
		}
	}
}

func (b *bucket[T]) store(way int, tag uint64, e *entry[T]) {
	b.slots[way].Store(e)
	b.setTag(way, tag)
}

func (b *bucket[T]) clear(way int) {
	b.setTag(way, 0)
	b.slots[way].Store(nil)
}

// victim is the CLOCK choice: the first unreferenced way from a start the key
// hash picks, clearing the reference bits it passes.
func (b *bucket[T]) victim(h uint64) int {
	m := b.meta.Load()
	start := int(h>>8&0xff) % bucketWays
	var passed uint64
	for i := range bucketWays {
		way := (start + i) % bucketWays
		if m&refBit(way) == 0 {
			start = way
			break
		}
		passed |= refBit(way)
	}
	if passed != 0 {
		b.meta.And(^passed)
	}
	return start
}

// shard is padded to a cache line so readers loading tbl do not share a line
// with another shard's writers.
type shard[T any] struct {
	mu  sync.Mutex
	tbl atomic.Pointer[[]bucket[T]]
	n   atomic.Int64
	_   [40]byte
}

func (s *shard[T]) bucket(h uint64) *bucket[T] {
	t := *s.tbl.Load()
	return &t[bucketOf(h, len(t))]
}

func newTable[T any](buckets uint32) *[]bucket[T] {
	t := make([]bucket[T], buckets)
	return &t
}

// newEntry allocates the key on its own. Co-allocating it with the entry made
// the address interner slower: interned addresses share the key's 24 B size
// class, and spans holding only them make every weak-handle insert walk longer.
func newEntry[T any](key []byte, e entry[T]) *entry[T] {
	e.key = string(key)
	return &e
}
