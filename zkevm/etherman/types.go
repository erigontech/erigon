package etherman

import (
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zkevm/etherman/smartcontracts/polygonzkevm"
)

// Block struct
type Block struct {
	BlockNumber           uint64
	BlockHash             common.Hash
	ParentHash            common.Hash
	GlobalExitRoots       []GlobalExitRoot
	ForcedBatches         []ForcedBatch
	SequencedBatches      [][]SequencedBatch
	VerifiedBatches       []VerifiedBatch
	SequencedForceBatches [][]SequencedForceBatch
	ForkIDs               []ForkID
	ReceivedAt            time.Time
}

// GlobalExitRoot struct
type GlobalExitRoot struct {
	BlockNumber     uint64
	MainnetExitRoot common.Hash
	RollupExitRoot  common.Hash
	GlobalExitRoot  common.Hash
}

// SequencedBatch represents virtual batch
type SequencedBatch struct {
	BatchNumber   uint64
	SequencerAddr common.Address
	TxHash        common.Hash
	Nonce         uint64
	Coinbase      common.Address
	polygonzkevm.PolygonZkEVMBatchData
}

// ForcedBatch represents a ForcedBatch
type ForcedBatch struct {
	BlockNumber       uint64
	ForcedBatchNumber uint64
	Sequencer         common.Address
	GlobalExitRoot    common.Hash
	RawTxsData        []byte
	ForcedAt          time.Time
}

// VerifiedBatch represents a VerifiedBatch
type VerifiedBatch struct {
	BlockNumber uint64
	BatchNumber uint64
	Aggregator  common.Address
	StateRoot   common.Hash
	TxHash      common.Hash
}

// SequencedForceBatch is a sturct to track the ForceSequencedBatches event.
type SequencedForceBatch struct {
	BatchNumber uint64
	Coinbase    common.Address
	TxHash      common.Hash
	Timestamp   time.Time
	Nonce       uint64
	polygonzkevm.PolygonZkEVMForcedBatchData
}

// ForkID is a sturct to track the ForkID event.
type ForkID struct {
	BatchNumber uint64
	ForkID      uint64
	Version     string
}
