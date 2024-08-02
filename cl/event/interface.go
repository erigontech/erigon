package event

import (
	ethevent "github.com/erigontech/erigon/event"
)

var (
	_ Feed = (*ethevent.Feed)(nil)
)

type Feed interface {
	Subscribe(channel interface{}) ethevent.Subscription
	Send(value interface{}) (nsent int)
}
