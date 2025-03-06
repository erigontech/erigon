package rpchelper

import (
	"sync"
)

// a simple interface for subscriptions for rpc helper
type Sub[T any] interface {
	Send(T)
	Close()
}

type chan_sub[T any] struct {
	lock   sync.Mutex // protects all fileds of this struct
	ch     chan T
	closed bool
}

// newChanSub - buffered channel
func newChanSub[T any](size int) *chan_sub[T] {
	if size < 8 { // set min size to 8
		size = 8
	}
	o := &chan_sub[T]{}
	o.ch = make(chan T, size)
	return o
}
func (s *chan_sub[T]) Send(x T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return
	}
	select {
	case s.ch <- x:
	default: // the sub is overloaded, dispose message
	}
}
func (s *chan_sub[T]) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}
