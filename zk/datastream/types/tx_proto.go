package types

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"google.golang.org/protobuf/proto"
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

	return ConvertToL2TransactionProto(&tx), nil
}

// ConvertToL2TransactionProto converts transaction object from datastream.Transaction to types.L2TransactionProto
func ConvertToL2TransactionProto(tx *datastream.Transaction) *L2TransactionProto {
	return &L2TransactionProto{
		L2BlockNumber:               tx.GetL2BlockNumber(),
		Index:                       tx.GetIndex(),
		IsValid:                     tx.GetIsValid(),
		Encoded:                     tx.GetEncoded(),
		EffectiveGasPricePercentage: uint8(tx.GetEffectiveGasPricePercentage()),
		IntermediateStateRoot:       libcommon.BytesToHash(tx.GetImStateRoot()),
		Debug:                       ProcessDebug(tx.GetDebug()),
	}
}
