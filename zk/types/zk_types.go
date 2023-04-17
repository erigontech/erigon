package types

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/common"

	ethTypes "github.com/ledgerwatch/erigon/core/types"
)

const EFFECTIVE_GAS_PRICE_PERCENTAGE_DISABLED = 0

type L1BatchInfo struct {
	BatchNo   uint64
	L1BlockNo uint64
	L1TxHash  common.Hash
	StateRoot common.Hash
}

// Batch struct
type Batch struct {
	BatchNumber    uint64
	Coinbase       common.Address
	BatchL2Data    []byte
	StateRoot      common.Hash
	LocalExitRoot  common.Hash
	AccInputHash   common.Hash
	Timestamp      time.Time
	Transactions   []ethTypes.Transaction
	GlobalExitRoot common.Hash
	ForcedBatchNum *uint64
}
