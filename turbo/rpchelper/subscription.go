package rpchelper

import (
	"context"
	"sync"
)

type Sub[T any] interface {
	// Send should sends T to the subscriber. should be safe to send after Close()
	Send(T)
	// Close should mark the subscription closed. It should be safe to call Close multiple times
	Close()
}

type chan_sub[T any] struct {
	ch chan T
	in chan T

	closed chan struct{}

	ctx context.Context
	cn  context.CancelFunc

	c sync.Cond
}

func NewChanSub[T any](ch chan T) *chan_sub[T] {
	o := &chan_sub[T]{}
	o.ch = ch
	o.in = make(chan T)
	o.closed = make(chan struct{})
	o.ctx, o.cn = context.WithCancel(context.Background())
	go o.loop()
	return o
}

func (s *chan_sub[T]) loop() {
	for {
		select {
		// if canceled, exit loop
		case <-s.ctx.Done():
			return
			// if there are any entries in the input buffer, grab
		case v := <-s.in:
			select {
			// send to output channel
			case s.ch <- v:
			case <-s.ctx.Done():
				return
			}
		}
	}
}

func (s *chan_sub[T]) Send(x T) {
	select {
	// if the output buffer is empty, send
	case s.ch <- x:
		// if sub is canceled, dispose message
	case <-s.ctx.Done():
	default:
		select {
		// if input buffer is empty, send
		case s.in <- x:
			// if sub is canceled, dispose message
		case <-s.ctx.Done():
		}
	}
}
func (s *chan_sub[T]) Close() {
	select {
	case <-s.ctx.Done():
		return
	default:
	}
	// its possible for multiple goroutines to get to this point, if Close is called twice at the same time
	// close the context - allows any sends to exit
	s.cn()
	select {
	case s.closed <- struct{}{}:
		// but it is not possible for multiple goroutines to get to this point
		// drain the channel
		for range s.ch {
		}
		close(s.ch)
	default:
	}
}
