package types

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/common"

	"encoding/binary"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cl/utils"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
)

const EFFECTIVE_GAS_PRICE_PERCENTAGE_DISABLED = 0

var EFFECTIVE_GAS_PRICE_MAX_VAL = new(uint256.Int).SetUint64(256)

type L1BatchInfo struct {
	BatchNo    uint64
	L1BlockNo  uint64
	L1TxHash   common.Hash
	StateRoot  common.Hash
	L1InfoRoot common.Hash
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

type L1InfoTreeUpdate struct {
	Index           uint64
	GER             common.Hash
	MainnetExitRoot common.Hash
	RollupExitRoot  common.Hash
	ParentHash      common.Hash
	Timestamp       uint64
}

func (l *L1InfoTreeUpdate) Marshall() []byte {
	result := make([]byte, 8+32+32+32+32+8)
	idx := utils.Uint64ToLE(l.Index)
	copy(result[:8], idx)
	copy(result[8:], l.GER[:])
	copy(result[40:], l.MainnetExitRoot[:])
	copy(result[72:], l.RollupExitRoot[:])
	copy(result[104:], l.ParentHash[:])
	copy(result[136:], utils.Uint64ToLE(l.Timestamp))
	return result
}

func (l *L1InfoTreeUpdate) Unmarshall(input []byte) {
	l.Index = binary.LittleEndian.Uint64(input[:8])
	copy(l.GER[:], input[8:40])
	copy(l.MainnetExitRoot[:], input[40:72])
	copy(l.RollupExitRoot[:], input[72:104])
	copy(l.ParentHash[:], input[104:136])
	l.Timestamp = binary.LittleEndian.Uint64(input[136:])
}
