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

package concurrent

import "sync"

// NewSyncMap initializes and returns a new instance of SyncMap.
func NewSyncMap[K comparable, T any]() *SyncMap[K, T] {
	return &SyncMap[K, T]{
		m: make(map[K]T),
	}
}

// SyncMap is a generic map that uses a read-write mutex to ensure thread-safe access.
type SyncMap[K comparable, T any] struct {
	m  map[K]T
	mu sync.RWMutex
}

// Get retrieves the value associated with the given key.
func (m *SyncMap[K, T]) Get(k K) (res T, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res, ok = m.m[k]
	return res, ok
}

// Put sets the value for the given key, returning the previous value if present.
func (m *SyncMap[K, T]) Put(k K, v T) (T, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.m[k]
	m.m[k] = v
	return old, ok
}

// Do performs a custom operation on the value associated with the given key.
func (m *SyncMap[K, T]) Do(k K, fn func(T, bool) (T, bool)) (after T, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.m[k]
	nv, save := fn(val, ok)
	if save {
		m.m[k] = nv
	} else {
		delete(m.m, k)
	}
	return nv, ok
}

// DoAndStore performs a custom operation on the value associated with the given key and stores the result.
func (m *SyncMap[K, T]) DoAndStore(k K, fn func(t T, ok bool) T) (after T, ok bool) {
	return m.Do(k, func(t T, b bool) (T, bool) {
		res := fn(t, b)
		return res, true
	})
}

// Range calls a function for each key-value pair in the map.
func (m *SyncMap[K, T]) Range(fn func(k K, v T) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.m {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes the value associated with the given key, if present.
func (m *SyncMap[K, T]) Delete(k K) (t T, deleted bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.m[k]
	if !ok {
		return t, false
	}
	delete(m.m, k)
	return val, true
}
