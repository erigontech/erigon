package rpchelper

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"sync/atomic"
)

type (
	SubscriptionID    string
	HeadsSubID        SubscriptionID
	PendingLogsSubID  SubscriptionID
	PendingBlockSubID SubscriptionID
	PendingTxsSubID   SubscriptionID
	LogsSubID         SubscriptionID
)

var globalSubscriptionId uint64

func generateSubscriptionID() SubscriptionID {
	id := [16]byte{}
	sb := new(strings.Builder)
	hex := hex.NewEncoder(sb)
	binary.LittleEndian.PutUint64(id[:], atomic.AddUint64(&globalSubscriptionId, 1))
	// try 4 times to generate an id
	for i := 0; i < 4; i++ {
		_, err := rand.Read(id[8:])
		if err == nil {
			break
		}
	}
	// if the computer has no functioning secure rand source, it will just use the incrementing number
	hex.Write(id[:])
	return SubscriptionID(sb.String())
}
