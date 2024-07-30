package types

import (
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"google.golang.org/protobuf/proto"
)

type L2BlockEndProto struct {
	Number uint64
}

func (b *L2BlockEndProto) Marshal() ([]byte, error) {
	return proto.Marshal(&datastream.L2BlockEnd{
		Number: b.Number,
	})
}

func (b *L2BlockEndProto) Type() EntryType {
	return EntryTypeL2BlockEnd
}

func (b *L2BlockEndProto) GetBlockNumber() uint64 {
	return b.Number
}

func UnmarshalL2BlockEnd(data []byte) (*L2BlockEndProto, error) {
	blockEnd := datastream.L2BlockEnd{}
	if err := proto.Unmarshal(data, &blockEnd); err != nil {
		return nil, err
	}

	return &L2BlockEndProto{
		Number: blockEnd.Number,
	}, nil
}

type L2BlockProto struct {
	*datastream.L2Block
}

type FullL2Block struct {
	BatchNumber     uint64
	L2BlockNumber   uint64
	Timestamp       int64
	DeltaTimestamp  uint32
	L1InfoTreeIndex uint32
	GlobalExitRoot  libcommon.Hash
	Coinbase        libcommon.Address
	ForkId          uint64
	L1BlockHash     libcommon.Hash
	L2Blockhash     libcommon.Hash
	ParentHash      libcommon.Hash
	StateRoot       libcommon.Hash
	BlockGasLimit   uint64
	BlockInfoRoot   libcommon.Hash
	L2Txs           []L2TransactionProto
	Debug           Debug
}

func (b *L2BlockProto) Marshal() ([]byte, error) {
	return proto.Marshal(b.L2Block)
}

func (b *L2BlockProto) Type() EntryType {
	return EntryTypeL2Block
}

func UnmarshalL2Block(data []byte) (*FullL2Block, error) {
	block := datastream.L2Block{}
	err := proto.Unmarshal(data, &block)
	if err != nil {
		return nil, err
	}

	l2Block := &FullL2Block{
		BatchNumber:     block.BatchNumber,
		L2BlockNumber:   block.Number,
		Timestamp:       int64(block.Timestamp),
		DeltaTimestamp:  block.DeltaTimestamp,
		L1InfoTreeIndex: block.L1InfotreeIndex,
		GlobalExitRoot:  libcommon.BytesToHash(block.GlobalExitRoot),
		Coinbase:        libcommon.BytesToAddress(block.Coinbase),
		L1BlockHash:     libcommon.BytesToHash(block.L1Blockhash),
		L2Blockhash:     libcommon.BytesToHash(block.Hash),
		StateRoot:       libcommon.BytesToHash(block.StateRoot),
		BlockGasLimit:   block.BlockGasLimit,
		BlockInfoRoot:   libcommon.BytesToHash(block.BlockInfoRoot),
		Debug:           ProcessDebug(block.Debug),
	}

	return l2Block, nil
}
