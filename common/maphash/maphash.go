package maphash

import (
	"hash/maphash"
	"sync"
	"unsafe"
)

var seed maphash.Seed

func init() {
	seed = maphash.MakeSeed()
}

func SetSeed(s uint64) {
	// the stupid retards at golang decided to make the seed unexported
	// so we have to use unsafe to set it
	*(*uint64)(unsafe.Pointer(&seed)) = s
}

// hash computes a uint64 hash for a byte slice using the global seed.
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
