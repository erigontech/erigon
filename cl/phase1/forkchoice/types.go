package forkchoice

import (
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// LatestMessage represents the latest message from a validator.
// [Modified in Gloas:EIP7732] Added Slot and PayloadPresent.
type LatestMessage struct {
	Epoch          uint64
	Slot           uint64 // [New in Gloas:EIP7732]
	Root           common.Hash
	PayloadPresent bool // [New in Gloas:EIP7732]
}

// ForkChoiceNode tracks the payload status for a block root in the fork choice store.
// [New in Gloas:EIP7732]
type ForkChoiceNode struct {
	Root          common.Hash
	PayloadStatus cltypes.PayloadStatus
}
