package rpchelper

import (
	"crypto/rand"
	"fmt"

	"github.com/ledgerwatch/log/v3"
)

type (
	SubscriptionID    string
	HeadsSubID        SubscriptionID
	PendingLogsSubID  SubscriptionID
	PendingBlockSubID SubscriptionID
	PendingTxsSubID   SubscriptionID
	LogsSubID         uint64
)

func generateSubscriptionID() SubscriptionID {
	var id [32]byte

	_, err := rand.Read(id[:])
	if err != nil {
		log.Crit("rpc filters: error creating random id", "err", err)
	}

	return SubscriptionID(fmt.Sprintf("%x", id))
}
