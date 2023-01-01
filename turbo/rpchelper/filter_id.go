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

var globalSubscriptionId uint64

func generateSubscriptionID() SubscriptionID {
	id := atomic.AddUint64(&globalSubscriptionId, 1)
	return SubscriptionID(fmt.Sprintf("%016x", id))
}
