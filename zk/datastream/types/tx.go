package types

import (
	"encoding/binary"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common"
)

const (
	l2TxMinDataLength = 38
)

// L2Transaction represents a zkEvm transaction
type L2Transaction struct {
	EffectiveGasPricePercentage uint8       // 1 byte
	IsValid                     uint8       // 1 byte
	StateRoot                   common.Hash // 32 bytes
	EncodedLength               uint32      // 4 bytes
	Encoded                     []byte
}

func (t *L2Transaction) EntryType() EntryType {
	return EntryTypeL2Tx
}

func (t *L2Transaction) Bytes(bigEndian bool) []byte {
	if bigEndian {
		return EncodeL2TransactionBigEndian(t)
	}
	return EncodeL2Transaction(t)
}

// DecodeL2Transaction decodes a L2 transaction from a byte array
func DecodeL2Transaction(data []byte) (*L2Transaction, error) {
	dataLen := len(data)
	if dataLen < l2TxMinDataLength {
		return &L2Transaction{}, fmt.Errorf("expected minimum data length: %d, got: %d", l2TxMinDataLength, len(data))
	}

	stateRoot := common.BytesToHash(data[2:34])
	encodedLength := binary.LittleEndian.Uint32(data[34:38])
	encoded := data[38:]
	if encodedLength != uint32(len(encoded)) {
		return &L2Transaction{}, fmt.Errorf("expected encoded length: %d, got: %d", encodedLength, len(encoded))
	}

	return &L2Transaction{
		EffectiveGasPricePercentage: data[0],
		IsValid:                     data[1],
		StateRoot:                   stateRoot,
		EncodedLength:               encodedLength,
		Encoded:                     encoded,
	}, nil
}

// DecodeL2Transaction decodes a L2 transaction from a byte array
func DecodeL2TransactionBigEndian(data []byte) (*L2Transaction, error) {
	dataLen := len(data)
	if dataLen < l2TxMinDataLength {
		return &L2Transaction{}, fmt.Errorf("expected minimum data length: %d, got: %d", l2TxMinDataLength, len(data))
	}

	stateRoot := common.BytesToHash(data[2:34])
	encodedLength := binary.BigEndian.Uint32(data[34:38])
	encoded := data[38:]
	if encodedLength != uint32(len(encoded)) {
		return &L2Transaction{}, fmt.Errorf("expected encoded length: %d, got: %d", encodedLength, len(encoded))
	}

	return &L2Transaction{
		EffectiveGasPricePercentage: data[0],
		IsValid:                     data[1],
		StateRoot:                   stateRoot,
		EncodedLength:               encodedLength,
		Encoded:                     encoded,
	}, nil
}

func EncodeL2Transaction(tx *L2Transaction) []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, []byte{tx.EffectiveGasPricePercentage, tx.IsValid}...)
	bytes = append(bytes, tx.StateRoot[:]...)
	bytes = binary.LittleEndian.AppendUint32(bytes, tx.EncodedLength)
	bytes = append(bytes, tx.Encoded...)
	return bytes
}

func EncodeL2TransactionBigEndian(tx *L2Transaction) []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, []byte{tx.EffectiveGasPricePercentage, tx.IsValid}...)
	bytes = append(bytes, tx.StateRoot[:]...)
	bytes = binary.BigEndian.AppendUint32(bytes, tx.EncodedLength)
	bytes = append(bytes, tx.Encoded...)
	return bytes
}
