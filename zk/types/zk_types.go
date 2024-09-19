package types

import (
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"

	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cl/utils"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
)

const EFFECTIVE_GAS_PRICE_PERCENTAGE_DISABLED = 0
const EFFECTIVE_GAS_PRICE_PERCENTAGE_MAXIMUM = 255

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
	BlockNumber     uint64
}

func (l *L1InfoTreeUpdate) Marshall() []byte {
	result := make([]byte, 8+32+32+32+32+8+8)
	idx := utils.Uint64ToLE(l.Index)
	copy(result[:8], idx)
	copy(result[8:], l.GER[:])
	copy(result[40:], l.MainnetExitRoot[:])
	copy(result[72:], l.RollupExitRoot[:])
	copy(result[104:], l.ParentHash[:])
	copy(result[136:], utils.Uint64ToLE(l.Timestamp))
	copy(result[144:], utils.Uint64ToLE(l.BlockNumber))
	return result
}

func (l *L1InfoTreeUpdate) Unmarshall(input []byte) {
	l.Index = binary.LittleEndian.Uint64(input[:8])
	copy(l.GER[:], input[8:40])
	copy(l.MainnetExitRoot[:], input[40:72])
	copy(l.RollupExitRoot[:], input[72:104])
	copy(l.ParentHash[:], input[104:136])
	l.Timestamp = binary.LittleEndian.Uint64(input[136:])
	// this was added later and could cause an already running sequencer to panic
	if len(input) > 144 {
		l.BlockNumber = binary.LittleEndian.Uint64(input[144:])
	}
}

type L1InjectedBatch struct {
	L1BlockNumber      uint64
	Timestamp          uint64
	L1BlockHash        common.Hash
	L1ParentHash       common.Hash
	LastGlobalExitRoot common.Hash
	Sequencer          common.Address
	Transaction        []byte
}

func (ib *L1InjectedBatch) Marshall() []byte {
	result := make([]byte, 0)
	result = append(result, utils.Uint64ToLE(ib.L1BlockNumber)...)
	result = append(result, utils.Uint64ToLE(ib.Timestamp)...)
	result = append(result, ib.L1BlockHash[:]...)
	result = append(result, ib.L1ParentHash[:]...)
	result = append(result, ib.LastGlobalExitRoot[:]...)
	result = append(result, ib.Sequencer[:]...)
	result = append(result, ib.Transaction...)
	return result
}

func (ib *L1InjectedBatch) Unmarshall(input []byte) error {
	if len(input) < 132 {
		return fmt.Errorf("unmarshall error, input is too short")
	}
	err := binary.Read(bytes.NewReader(input[:8]), binary.LittleEndian, &ib.L1BlockNumber)
	if err != nil {
		return err
	}
	err = binary.Read(bytes.NewReader(input[8:16]), binary.LittleEndian, &ib.Timestamp)
	if err != nil {
		return err
	}
	copy(ib.L1BlockHash[:], input[16:48])
	copy(ib.L1ParentHash[:], input[48:80])
	copy(ib.LastGlobalExitRoot[:], input[80:112])
	copy(ib.Sequencer[:], input[112:132])
	ib.Transaction = append([]byte{}, input[132:]...)
	return nil
}

type ForkInterval struct {
	ForkID          uint64
	FromBatchNumber uint64
	ToBatchNumber   uint64
	BlockNumber     uint64
}
