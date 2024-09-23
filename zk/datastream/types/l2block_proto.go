package types

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
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

	return ConvertToFullL2Block(&block), nil
}

// ConvertToFullL2Block converts the datastream.L2Block to types.FullL2Block
func ConvertToFullL2Block(block *datastream.L2Block) *FullL2Block {
	return &FullL2Block{
		BatchNumber:     block.GetBatchNumber(),
		L2BlockNumber:   block.GetNumber(),
		Timestamp:       int64(block.GetTimestamp()),
		DeltaTimestamp:  block.GetDeltaTimestamp(),
		L1InfoTreeIndex: block.GetL1InfotreeIndex(),
		GlobalExitRoot:  libcommon.BytesToHash(block.GetGlobalExitRoot()),
		Coinbase:        libcommon.BytesToAddress(block.GetCoinbase()),
		L1BlockHash:     libcommon.BytesToHash(block.GetL1Blockhash()),
		L2Blockhash:     libcommon.BytesToHash(block.GetHash()),
		StateRoot:       libcommon.BytesToHash(block.GetStateRoot()),
		BlockGasLimit:   block.GetBlockGasLimit(),
		BlockInfoRoot:   libcommon.BytesToHash(block.GetBlockInfoRoot()),
		Debug:           ProcessDebug(block.GetDebug()),
	}
}
