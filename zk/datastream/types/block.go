package types

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
)

const (
	startL2BlockDataLength = 78
	endL2BlockDataLength   = 72

	// EntryTypeL2Block represents a L2 block
	EntryTypeStartL2Block EntryType = 1
	EntryTypeEndL2Block   EntryType = 3
)

// StartL2Block represents a zkEvm block
type StartL2Block struct {
	BatchNumber    uint64         // 8 bytes
	L2BlockNumber  uint64         // 8 bytes
	Timestamp      int64          // 8 bytes
	GlobalExitRoot common.Hash    // 32 bytes
	Coinbase       common.Address // 20 bytes
	ForkId         uint16         // 2 bytes
}

// decodes a StartL2Block from a byte array
func DecodeStartL2Block(data []byte) (*StartL2Block, error) {
	if len(data) != startL2BlockDataLength {
		return &StartL2Block{}, fmt.Errorf("expected data length: %d, got: %d", startL2BlockDataLength, len(data))
	}

	var ts int64
	buf := bytes.NewBuffer(data[16:24])
	if err := binary.Read(buf, binary.LittleEndian, &ts); err != nil {
		return &StartL2Block{}, err
	}

	return &StartL2Block{
		BatchNumber:    binary.LittleEndian.Uint64(data[:8]),
		L2BlockNumber:  binary.LittleEndian.Uint64(data[8:16]),
		Timestamp:      ts,
		GlobalExitRoot: common.BytesToHash(data[24:56]),
		Coinbase:       common.BytesToAddress(data[56:76]),
		ForkId:         binary.LittleEndian.Uint16(data[76:78]),
	}, nil
}

func EncodeStartL2Block(block *StartL2Block) []byte {
	bytes := make([]byte, 0)
	bytes = binary.LittleEndian.AppendUint64(bytes, block.BatchNumber)
	bytes = binary.LittleEndian.AppendUint64(bytes, block.L2BlockNumber)
	bytes = binary.LittleEndian.AppendUint64(bytes, uint64(block.Timestamp))
	bytes = append(bytes, block.GlobalExitRoot.Bytes()...)
	bytes = append(bytes, block.Coinbase.Bytes()...)
	bytes = binary.LittleEndian.AppendUint16(bytes, block.ForkId)
	return bytes
}

type EndL2Block struct {
	L2BlockNumber uint64      // 8 bytes
	L2Blockhash   common.Hash // 32 bytes
	StateRoot     common.Hash // 32 bytes
}

// DecodeEndL2Block decodes a EndL2Block from a byte array
func DecodeEndL2Block(data []byte) (*EndL2Block, error) {
	if len(data) != endL2BlockDataLength {
		return &EndL2Block{}, fmt.Errorf("expected data length: %d, got: %d", endL2BlockDataLength, len(data))
	}

	return &EndL2Block{
		L2BlockNumber: binary.LittleEndian.Uint64(data[:8]),
		L2Blockhash:   common.BytesToHash(data[8:40]),
		StateRoot:     common.BytesToHash(data[40:72]),
	}, nil
}

func EncodeEndL2Block(end *EndL2Block) []byte {
	bytes := make([]byte, 0)
	bytes = binary.LittleEndian.AppendUint64(bytes, end.L2BlockNumber)
	bytes = append(bytes, end.L2Blockhash[:]...)
	bytes = append(bytes, end.StateRoot[:]...)
	return bytes
}

type FullL2Block struct {
	BatchNumber    uint64
	L2BlockNumber  uint64
	Timestamp      int64
	GlobalExitRoot common.Hash
	Coinbase       common.Address
	ForkId         uint16
	L2Blockhash    common.Hash
	StateRoot      common.Hash
	L2Txs          []L2Transaction
	ParentHash     common.Hash
}

// ParseFullL2Block parses a FullL2Block from a StartL2Block, EndL2Block and a slice of L2Transactions
func ParseFullL2Block(startL2Block *StartL2Block, endL2Block *EndL2Block, l2Txs *[]L2Transaction) *FullL2Block {
	return &FullL2Block{
		BatchNumber:    startL2Block.BatchNumber,
		L2BlockNumber:  startL2Block.L2BlockNumber,
		Timestamp:      startL2Block.Timestamp,
		GlobalExitRoot: startL2Block.GlobalExitRoot,
		Coinbase:       startL2Block.Coinbase,
		ForkId:         startL2Block.ForkId,
		L2Blockhash:    endL2Block.L2Blockhash,
		StateRoot:      endL2Block.StateRoot,
		L2Txs:          *l2Txs,
	}
}
