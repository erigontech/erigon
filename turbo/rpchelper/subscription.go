package rpchelper

import (
	"context"
)

type Sub[T any] interface {
	// Send should sends T to the subscriber. should be safe to send after Close()
	Send(T)
	// Recv should receive T and whether or not the sub is done. should be safe to recv after Close() is called
	Recv() (res T, done bool)
	// Close should mark the subscription closed. It should be safe to call Close multiple times
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
	}
	// at this point, mark the subscription as cancelled
	s.cn()
	// its possible for multiple goroutines to get to this point
	select {
	case s.closed <- struct{}{}:
		// but it is not possible for multiple goroutines to get to this point
		// close the channel
		close(s.ch)
		// drain the channel
		for _ = range s.ch {
		}
	default:
	}
}
func (s *chan_sub[T]) Recv() (T, bool) {
	var t T
	select {
	case <-s.ctx.Done():
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
