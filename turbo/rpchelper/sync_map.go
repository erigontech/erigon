package rpchelper

import "sync"

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
	m.RLock()
	_, ok := m.m[k]
	m.RUnlock()
	if !ok {
		return t, false
	}
	m.Lock()
	defer m.Unlock()
	val, ok := m.m[k]
	if !ok {
		return t, false
	}
	delete(m.m, k)
	return val, true
}
