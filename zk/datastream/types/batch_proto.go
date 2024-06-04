package types

import (
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"google.golang.org/protobuf/proto"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
)

type BatchType uint32

var (
	BatchTypeUnspecified BatchType = 0
	BatchTypeRegular               = 1
	BatchTypeForced                = 2
	BatchTypeInjected              = 3
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
		Number:  batch.Number,
		ForkId:  batch.ForkId,
		ChainId: batch.ChainId,
		Debug:   ProcessDebug(batch.Debug),
	}, nil
}

func UnmarshalBatchEnd(data []byte) (*BatchEnd, error) {
	batchEnd := &datastream.BatchEnd{}
	if err := proto.Unmarshal(data, batchEnd); err != nil {
		return nil, err
	}

	return &BatchEnd{
		LocalExitRoot: libcommon.BytesToHash(batchEnd.LocalExitRoot),
		StateRoot:     libcommon.BytesToHash(batchEnd.StateRoot),
		Debug:         ProcessDebug(batchEnd.Debug),
	}, nil
}
