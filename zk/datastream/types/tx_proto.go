package types

import (
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"google.golang.org/protobuf/proto"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
)

type TxProto struct {
	*datastream.Transaction
}

type L2TransactionProto struct {
	L2BlockNumber               uint64
	Index                       uint64
	IsValid                     bool
	Encoded                     []byte
	EffectiveGasPricePercentage uint8
	IntermediateStateRoot       libcommon.Hash
	Debug                       Debug
}

func (t *TxProto) Marshal() ([]byte, error) {
	return proto.Marshal(t.Transaction)
}

func (t *TxProto) Type() EntryType {
	return EntryTypeL2Tx
}

func UnmarshalTx(data []byte) (*L2TransactionProto, error) {
	tx := datastream.Transaction{}
	err := proto.Unmarshal(data, &tx)
	if err != nil {
		return nil, err
	}

	l2Tx := &L2TransactionProto{
		L2BlockNumber:               tx.L2BlockNumber,
		Index:                       tx.Index,
		IsValid:                     tx.IsValid,
		Encoded:                     tx.Encoded,
		EffectiveGasPricePercentage: uint8(tx.EffectiveGasPricePercentage),
		IntermediateStateRoot:       libcommon.BytesToHash(tx.ImStateRoot),
		Debug:                       ProcessDebug(tx.Debug),
	}

	return l2Tx, nil
}
