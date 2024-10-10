package types

import (
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"google.golang.org/protobuf/proto"
)

type BatchType uint32

var (
	BatchTypeUnspecified BatchType = 0
	BatchTypeRegular     BatchType = 1
	BatchTypeForced      BatchType = 2
	BatchTypeInjected    BatchType = 3
	BatchTypeInvalid     BatchType = 4
)

type BatchStartProto struct {
	*datastream.BatchStart
}

type BatchEndProto struct {
	*datastream.BatchEnd
}

type BatchStart struct {
	Number    uint64
	BatchType BatchType
	ForkId    uint64
	ChainId   uint64
	Debug     Debug
}

func (b *BatchStartProto) Marshal() ([]byte, error) {
	return proto.Marshal(b.BatchStart)
}

func (b *BatchStartProto) Type() EntryType {
	return EntryTypeBatchStart
}

type BatchEnd struct {
	Number        uint64
	LocalExitRoot libcommon.Hash
	StateRoot     libcommon.Hash
	Debug         Debug
}

func (b *BatchEndProto) Marshal() ([]byte, error) {
	return proto.Marshal(b.BatchEnd)
}

func (b *BatchEndProto) Type() EntryType {
	return EntryTypeBatchEnd
}

func UnmarshalBatchStart(data []byte) (*BatchStart, error) {
	batch := &datastream.BatchStart{}
	if err := proto.Unmarshal(data, batch); err != nil {
		return nil, err
	}

	return &BatchStart{
		Number:    batch.Number,
		ForkId:    batch.ForkId,
		ChainId:   batch.ChainId,
		Debug:     ProcessDebug(batch.Debug),
		BatchType: BatchType(batch.Type),
	}, nil
}

func UnmarshalBatchEnd(data []byte) (*BatchEnd, error) {
	batchEnd := &datastream.BatchEnd{}
	if err := proto.Unmarshal(data, batchEnd); err != nil {
		return nil, err
	}

	return &BatchEnd{
		Number:        batchEnd.Number,
		LocalExitRoot: libcommon.BytesToHash(batchEnd.LocalExitRoot),
		StateRoot:     libcommon.BytesToHash(batchEnd.StateRoot),
		Debug:         ProcessDebug(batchEnd.Debug),
	}, nil
}
