package rpchelper

import (
	"context"
)

type SubManager[K comparable, T any] interface {
	Get(K) (res Sub[T], ok bool)
	Delete(K) (res Sub[T], ok bool)
	Put(K, Sub[T]) bool
	Range(func(k K, t Sub[T]) error) error
}

type Sub[T any] interface {
	Send(T)
	Recv() (res T, done bool)
	Close()
}

type chan_sub[T any] struct {
	ch chan T

	closed chan struct{}
	ctx    context.Context
	cn     context.CancelFunc
}

func NewChanSub[T any](ch chan T) *chan_sub[T] {
	o := &chan_sub[T]{}
	o.ch = ch
	o.ctx, o.cn = context.WithCancel(context.Background())
	o.closed = make(chan struct{})

	return o
}

func (s *chan_sub[T]) Send(x T) {
	select {
	case <-s.ctx.Done():
		return
	default:
		s.ch <- x
	}
}
func (s *chan_sub[T]) Close() {
	select {
	case <-s.ctx.Done():
		return
	default:
		s.cn()
	}
	// ensure that channel is only closed once
	select {
	case s.closed <- struct{}{}:
		close(s.ch)
		// drain channel
		for _ = range s.ch {
		}
	default:
	}
}
func (s *chan_sub[T]) Recv() (T, bool) {
	var t T
	select {
	case <-s.closed:
		return t, false
	default:
	}
	select {
	case val := <-s.ch:
		return val, true
	default:
		return t, false
	}
}
