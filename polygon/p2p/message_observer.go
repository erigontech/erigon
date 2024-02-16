package p2p

type MessageObserver[M any] interface {
	Notify(M)
}

type ChanMessageObserver[M any] chan M

func (cmo ChanMessageObserver[M]) Notify(msg M) {
	cmo <- msg
}
