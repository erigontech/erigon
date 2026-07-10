package maphash

import (
	"hash/maphash"
	"unsafe"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/puzpuzpuz/xsync/v4"
)

var seed maphash.Seed

func init() {
	seed = maphash.MakeSeed()
}

func SetSeed(s uint64) {
	*(*uint64)(unsafe.Pointer(&seed)) = s
}

// Hash computes a uint64 hash for a byte slice using the global seed.
func Hash(key []byte) uint64 {
	return maphash.Bytes(seed, key)
}

// Map is a concurrent map that uses maphash to hash []byte keys.
type Map[V any] struct {
	m *xsync.Map[uint64, V]
}

// NewMap creates a new Map.
func NewMap[V any]() *Map[V] {
	return &Map[V]{m: xsync.NewMap[uint64, V]()}
}

// Get retrieves a value by key.
func (m *Map[V]) Get(key []byte) (V, bool) {
	h := Hash(key)
	return m.m.Load(h)
}

// Set stores a value with the given key.
func (m *Map[V]) Set(key []byte, value V) {
	h := Hash(key)
	m.m.Store(h, value)
}

// LoadOrStore atomically returns the existing value (loaded=true) or stores and
// returns value (loaded=false). Size/count accounting must act only on !loaded.
func (m *Map[V]) LoadOrStore(key []byte, value V) (actual V, loaded bool) {
	h := Hash(key)
	return m.m.LoadOrStore(h, value)
}

// Delete removes a key from the map.
func (m *Map[V]) Delete(key []byte) {
	h := Hash(key)
	m.m.Delete(h)
}

// LoadAndDelete atomically deletes the key, returning the previous value.
// loaded is true only for the caller that removed it, so size/count accounting
// must act only on loaded.
func (m *Map[V]) LoadAndDelete(key []byte) (value V, loaded bool) {
	h := Hash(key)
	return m.m.LoadAndDelete(h)
}

// Len returns the number of entries in the map.
func (m *Map[V]) Len() int {
	return m.m.Size()
}

// Clear removes all entries from the map.
func (m *Map[V]) Clear() {
	m.m.Clear()
}

// Range iterates over every (hash, value) pair. Iteration order is
// unspecified. Return false from fn to stop early. Concurrent
// modification during Range is permitted by the underlying xsync.Map.
//
// The original byte-key is not recoverable — Set hashes-and-discards.
// Pair with DeleteByHash to evict entries discovered via Range.
func (m *Map[V]) Range(fn func(hash uint64, v V) bool) {
	m.m.Range(func(key uint64, value V) bool {
		return fn(key, value)
	})
}

// DeleteByHash removes the entry under the pre-computed hash. Use
// alongside Range when the byte-key is unknown.
func (m *Map[V]) DeleteByHash(hash uint64) {
	m.m.Delete(hash)
}

// NonConcurrentMap is a non-thread-safe map that uses maphash to hash []byte keys.
// Use this when you don't need concurrent access for better performance.
type NonConcurrentMap[V any] struct {
	m map[uint64]V
}

// NewNonConcurrentMap creates a new NonConcurrentMap.
func NewNonConcurrentMap[V any]() *NonConcurrentMap[V] {
	return &NonConcurrentMap[V]{m: make(map[uint64]V)}
}

// Get retrieves a value by key.
func (m *NonConcurrentMap[V]) Get(key []byte) (V, bool) {
	h := Hash(key)
	v, ok := m.m[h]
	return v, ok
}

// Set stores a value with the given key.
func (m *NonConcurrentMap[V]) Set(key []byte, value V) {
	h := Hash(key)
	m.m[h] = value
}

// Delete removes a key from the map.
func (m *NonConcurrentMap[V]) Delete(key []byte) {
	h := Hash(key)
	delete(m.m, h)
}

// Len returns the number of entries in the map.
func (m *NonConcurrentMap[V]) Len() int {
	return len(m.m)
}

// Clear removes all entries from the map.
func (m *NonConcurrentMap[V]) Clear() {
	clear(m.m)
}

// LRU is a thread-safe LRU cache that uses maphash to hash []byte keys.
type LRU[V any] struct {
	cache *lru.Cache[uint64, V]
}

// NewLRU creates a new LRU cache with the given size.
func NewLRU[V any](size int) (*LRU[V], error) {
	cache, err := lru.New[uint64, V](size)
	if err != nil {
		return nil, err
	}
	return &LRU[V]{cache: cache}, nil
}

// Get retrieves a value by key.
func (l *LRU[V]) Get(key []byte) (V, bool) {
	h := Hash(key)
	return l.cache.Get(h)
}

// Peek returns the value for key without updating its LRU recency. Use for
// diagnostic reads that must not perturb eviction order.
func (l *LRU[V]) Peek(key []byte) (V, bool) {
	h := Hash(key)
	return l.cache.Peek(h)
}

// Set stores a value with the given key.
func (l *LRU[V]) Set(key []byte, value V) {
	h := Hash(key)
	l.cache.Add(h, value)
}

// Delete removes a key from the cache.
func (l *LRU[V]) Delete(key []byte) {
	h := Hash(key)
	l.cache.Remove(h)
}

// Len returns the number of entries in the cache.
func (l *LRU[V]) Len() int {
	return l.cache.Len()
}

// Contains checks if a key exists in the cache without updating recency.
func (l *LRU[V]) Contains(key []byte) bool {
	h := Hash(key)
	return l.cache.Contains(h)
}

// Purge clears all entries from the cache.
func (l *LRU[V]) Purge() {
	l.cache.Purge()
}

// ShardedLRU is a thread-safe LRU split into independently-locked shards keyed
// by the low bits of the maphash. A single LRU serializes every Get on one
// mutex (Get promotes recency); sharding cuts that lock traffic by the shard
// count, which is what makes it usable from many concurrent readers.
type ShardedLRU[V any] struct {
	shards []*LRU[V]
	mask   uint64
}

// NewShardedLRU creates a sharded LRU holding size entries total, partitioned
// as evenly as possible across shards. The shard count is the requested count
// capped at size, rounded down to a power of two.
func NewShardedLRU[V any](size, shards int) (*ShardedLRU[V], error) {
	if shards > size {
		shards = size
	}
	n := 1
	for n*2 <= shards {
		n *= 2
	}
	base, extra := size/n, size%n
	s := &ShardedLRU[V]{shards: make([]*LRU[V], n), mask: uint64(n - 1)}
	for i := range s.shards {
		shardSize := base
		if i < extra {
			shardSize++
		}
		c, err := NewLRU[V](shardSize)
		if err != nil {
			return nil, err
		}
		s.shards[i] = c
	}
	return s, nil
}

func (l *ShardedLRU[V]) shardOf(hash uint64) *LRU[V] {
	return l.shards[hash&l.mask]
}

func (l *ShardedLRU[V]) Get(key []byte) (V, bool) {
	h := Hash(key)
	return l.shardOf(h).GetByHash(h)
}

func (l *ShardedLRU[V]) Set(key []byte, value V) {
	h := Hash(key)
	l.shardOf(h).SetByHash(h, value)
}

func (l *ShardedLRU[V]) Delete(key []byte) {
	h := Hash(key)
	l.shardOf(h).DeleteByHash(h)
}

// GetByHash and SetByHash operate on a pre-computed hash, for callers that
// already hold the maphash of the key (the same hash used for shard routing).
func (l *ShardedLRU[V]) GetByHash(hash uint64) (V, bool) {
	return l.shardOf(hash).GetByHash(hash)
}

func (l *ShardedLRU[V]) SetByHash(hash uint64, value V) {
	l.shardOf(hash).SetByHash(hash, value)
}

// DeleteByHash removes the entry under a pre-computed hash; pair with Range,
// which yields the same hashes used for shard routing.
func (l *ShardedLRU[V]) DeleteByHash(hash uint64) {
	l.shardOf(hash).DeleteByHash(hash)
}

func (l *ShardedLRU[V]) Len() int {
	n := 0
	for _, c := range l.shards {
		n += c.Len()
	}
	return n
}

func (l *ShardedLRU[V]) Purge() {
	for _, c := range l.shards {
		c.Purge()
	}
}

// Range iterates every (hash, value) pair without affecting recency. Return
// false from fn to stop early. The original byte-key is not recoverable.
func (l *ShardedLRU[V]) Range(fn func(hash uint64, v V) bool) {
	for _, c := range l.shards {
		stopped := false
		c.Range(func(h uint64, v V) bool {
			if !fn(h, v) {
				stopped = true
				return false
			}
			return true
		})
		if stopped {
			return
		}
	}
}

// GetByHash retrieves a value by a pre-computed hash.
func (l *LRU[V]) GetByHash(hash uint64) (V, bool) {
	return l.cache.Get(hash)
}

// SetByHash stores a value with a pre-computed hash.
func (l *LRU[V]) SetByHash(hash uint64, value V) {
	l.cache.Add(hash, value)
}

// ContainsByHash checks if a hash exists in the cache.
func (l *LRU[V]) ContainsByHash(hash uint64) bool {
	return l.cache.Contains(hash)
}

// DeleteByHash removes the entry under the pre-computed hash. Use
// alongside Range when the byte-key is unknown.
func (l *LRU[V]) DeleteByHash(hash uint64) {
	l.cache.Remove(hash)
}

// Range iterates over every (hash, value) pair without affecting LRU
// recency (uses Peek under the hood). Iteration order is unspecified.
// Return false from fn to stop early.
//
// The original byte-key is not recoverable; use the hash as identity (same key → same hash, collisions aside).
func (l *LRU[V]) Range(fn func(hash uint64, v V) bool) {
	for _, h := range l.cache.Keys() {
		v, ok := l.cache.Peek(h)
		if !ok {
			continue
		}
		if !fn(h, v) {
			return
		}
	}
}
