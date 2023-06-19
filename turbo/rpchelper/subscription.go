package rpchelper

import (
	"sync"
	"sync/atomic"
)

// a simple interface for subscriptions for rpc helper
type Sub[T any] interface {
	Send(T)
	Close()
}

type chan_sub[T any] struct {
	ch     chan T
	closed atomic.Bool
}

// buffered channel
func newChanSub[T any](size int) *chan_sub[T] {
	// set min size to 8.
	if size < 8 {
		size = 8
	}
	o := &chan_sub[T]{}
	o.ch = make(chan T, size)
	return o
}
func (s *chan_sub[T]) Send(x T) {
	if s.closed.Load() {
		return
	}

	select {
	case s.ch <- x:
	default: // the sub is overloaded, dispose message
	}
}
func (s *chan_sub[T]) Close() {
	if swapped := s.closed.CompareAndSwap(false, true); !swapped {
		return
	}
	close(s.ch)
}

func NewSyncMap[K comparable, T any]() *SyncMap[K, T] {
	return &SyncMap[K, T]{
		m: make(map[K]T),
	}
}

type SyncMap[K comparable, T any] struct {
	m  map[K]T
	mu sync.RWMutex
}

func (m *SyncMap[K, T]) Get(k K) (res T, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res, ok = m.m[k]
	return res, ok
}

func (m *SyncMap[K, T]) Put(k K, v T) (T, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.m[k]
	m.m[k] = v
	return old, ok
}

func (m *SyncMap[K, T]) Do(k K, fn func(T, bool) (T, bool)) (after T, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.m {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

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
