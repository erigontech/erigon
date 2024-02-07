package p2p

import (
	protosentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

type msgObserver interface {
	Notify(*protosentry.InboundMessage)
}

type chanMsgObserver chan *protosentry.InboundMessage

func (cmo chanMsgObserver) Notify(msg *protosentry.InboundMessage) {
	cmo <- msg
}
