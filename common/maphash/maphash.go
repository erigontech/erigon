package maphash

import (
	"hash/maphash"
	"sync"
	"unsafe"

	lru "github.com/hashicorp/golang-lru/v2"
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

// Map is a non-thread-safe map that uses maphash to hash []byte keys.
type Map[V any] struct {
	m  map[uint64]V
	mu sync.RWMutex
}

// NewMap creates a new Map.
func NewMap[V any]() *Map[V] {
	return &Map[V]{
		m: make(map[uint64]V),
	}
}

// Get retrieves a value by key.
func (m *Map[V]) Get(key []byte) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h := Hash(key)
	e, ok := m.m[h]
	if !ok {
		var zero V
		return zero, false
	}
	return e, true
}

// Set stores a value with the given key.
func (m *Map[V]) Set(key []byte, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := Hash(key)
	m.m[h] = value
}

// Update atomically retrieves the value for a key and applies an update function to it.
// If the key exists, the function is called with the value and the result is stored back.
// Returns the updated value and whether the key was found.
func (m *Map[V]) Update(key []byte, fn func(V) V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := Hash(key)
	v, ok := m.m[h]
	if !ok {
		var zero V
		return zero, false
	}
	v = fn(v)
	m.m[h] = v
	return v, true
}

// Delete removes a key from the map.
func (m *Map[V]) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := Hash(key)
	delete(m.m, h)
}

// Len returns the number of entries in the map.
func (m *Map[V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.m)
}

// Clear removes all entries from the map.
func (m *Map[V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
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
