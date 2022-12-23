package rpchelper

import (
	"context"

	"github.com/google/uuid"
)

type Sub[T any] interface {
	// Send should sends T to the subscriber. should be safe to send after Close()
	Send(T)
	// Close should mark the subscription closed. It should be safe to call Close multiple times
	Close()
}

type chan_sub[T any] struct {
	ch chan T

	closed chan struct{}
	ctx    context.Context
	cn     context.CancelFunc
	uuid   uuid.UUID
}

func NewChanSub[T any](ch chan T) *chan_sub[T] {
	o := &chan_sub[T]{}
	o.uuid = uuid.New()
	o.ch = ch
	o.ctx, o.cn = context.WithCancel(context.Background())
	o.closed = make(chan struct{}, 1)
	return o
}

func (s *chan_sub[T]) Send(x T) {
	//log.Println(s.uuid, "send")
	//defer log.Println(s.uuid, "send exit")
	select {
	case s.ch <- x:
	case <-s.ctx.Done():
	}
}
func (s *chan_sub[T]) Close() {
	//log.Println(s.uuid, "close")
	//defer log.Println(s.uuid, "close exit")
	s.cn()
	select {
	case <-s.ctx.Done():
		return
	default:
	}
}
