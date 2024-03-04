package p2p

import "context"

type MessageObserver[M any] interface {
	Notify(M)
}

func NewChanMessageObserver[M any](ctx context.Context) ChanMessageObserver[M] {
	return ChanMessageObserver[M]{
		ctx:     ctx,
		channel: make(chan M),
	}
}

type ChanMessageObserver[M any] struct {
	ctx     context.Context
	channel chan M
}

func (cmo ChanMessageObserver[M]) Notify(msg M) {
	select {
	case <-cmo.ctx.Done():
		return
	case cmo.channel <- msg:
		// no-op
	}
}

func (cmo ChanMessageObserver[M]) MessageChan() chan M {
	return cmo.channel
}
