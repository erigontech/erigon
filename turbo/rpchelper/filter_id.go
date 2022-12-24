package rpchelper

import (
	"fmt"
	"sync/atomic"
)

type (
	SubscriptionID    string
	HeadsSubID        SubscriptionID
	PendingLogsSubID  SubscriptionID
	PendingBlockSubID SubscriptionID
	PendingTxsSubID   SubscriptionID
	LogsSubID         uint64
)

var globalSubscriptionId atomic.Uint64

func generateSubscriptionID() SubscriptionID {
	id := globalSubscriptionId.Add(1)
	return SubscriptionID(fmt.Sprintf("%016x", id))
}
