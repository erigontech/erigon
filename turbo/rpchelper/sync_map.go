package rpchelper

import (
	"sync"
)

func NewSyncMap[K comparable, T any]() *SyncMap[K, T] {
	return &SyncMap[K, T]{
		m: make(map[K]T),
	}
}

type SyncMap[K comparable, T any] struct {
	m map[K]T
	sync.RWMutex
}

func (m *SyncMap[K, T]) Get(k K) (res T, ok bool) {
	m.RLock()
	defer m.RUnlock()
	res, ok = m.m[k]
	return res, ok
}

func (m *SyncMap[K, T]) Put(k K, v T) bool {
	m.Lock()
	defer m.Unlock()
	_, ok := m.m[k]
	m.m[k] = v
	return ok
}
func (m *SyncMap[K, T]) View(k K, fn func(T, bool)) (ok bool) {
	m.RLock()
	defer m.RUnlock()
	val, ok := m.m[k]
	fn(val, ok)
	return ok
}
func (m *SyncMap[K, T]) Do(k K, fn func(T, bool) (T, bool)) (after T, ok bool) {
	m.Lock()
	defer m.Unlock()
	val, ok := m.m[k]
	nv, save := fn(val, ok)
	if save {
		m.m[k] = nv
	}
	return nv, ok
}
func (m *SyncMap[K, T]) DoAndStore(k K, fn func(t T, ok bool) T) (after T, ok bool) {
	return m.Do(k, func(t T, b bool) (T, bool) {
		res := fn(t, b)
		return res, true
	})
}

func (m *SyncMap[K, T]) Range(fn func(k K, v T) error) error {
	m.RLock()
	defer m.RUnlock()
	for k, v := range m.m {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}
func (m *SyncMap[K, T]) Delete(k K) (T, bool) {
	var t T
	m.Lock()
	defer m.Unlock()
	val, ok := m.m[k]
	if !ok {
		return t, false
	}
	delete(m.m, k)
	return val, true
}
