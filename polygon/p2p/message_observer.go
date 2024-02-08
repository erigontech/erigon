package p2p

import (
	protosentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

type messageObserver interface {
	Notify(*protosentry.InboundMessage)
}

type chanMessageObserver chan *protosentry.InboundMessage

func (cmo chanMessageObserver) Notify(msg *protosentry.InboundMessage) {
	cmo <- msg
}
