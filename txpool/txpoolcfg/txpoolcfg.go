package txpoolcfg

import (
	"math/big"
	"time"
)

type Config struct {
	DBDir                 string
	TracedSenders         []string // List of senders for which tx pool should print out debugging info
	SyncToNewPeersEvery   time.Duration
	ProcessRemoteTxsEvery time.Duration
	CommitEvery           time.Duration
	LogEvery              time.Duration
	PendingSubPoolLimit   int
	BaseFeeSubPoolLimit   int
	QueuedSubPoolLimit    int
	MinFeeCap             uint64
	AccountSlots          uint64 // Number of executable transaction slots guaranteed per account
	PriceBump             uint64 // Price bump percentage to replace an already existing transaction
	OverrideShanghaiTime  *big.Int
}

var DefaultConfig = Config{
	SyncToNewPeersEvery:   2 * time.Minute,
	ProcessRemoteTxsEvery: 100 * time.Millisecond,
	CommitEvery:           15 * time.Second,
	LogEvery:              30 * time.Second,

	PendingSubPoolLimit: 10_000,
	BaseFeeSubPoolLimit: 10_000,
	QueuedSubPoolLimit:  10_000,

	MinFeeCap:            1,
	AccountSlots:         16, //TODO: to choose right value (16 to be compatible with Geth)
	PriceBump:            10, // Price bump percentage to replace an already existing transaction
	OverrideShanghaiTime: nil,
}
