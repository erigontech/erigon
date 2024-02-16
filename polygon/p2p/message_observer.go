package p2p

type messageObserver[M any] interface {
	Notify(M)
}

type chanMessageObserver[M any] chan M

func (cmo chanMessageObserver[M]) Notify(msg M) {
	cmo <- msg
}
