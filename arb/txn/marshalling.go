package txn

import (
	"fmt"

	"github.com/erigontech/erigon/execution/types"
)

func init() {
	types.UnmarshalExtTxnFunc = ArbUnmarshalJSON
}

func ArbUnmarshalJSON(txType byte, input []byte) (types.Transaction, error) {
	switch txType {
	case ArbitrumDepositTxType:
		tx := new(ArbitrumDepositTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case ArbitrumInternalTxType:
		tx := new(ArbitrumInternalTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case ArbitrumUnsignedTxType:
		tx := new(ArbitrumUnsignedTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case ArbitrumContractTxType:
		tx := new(ArbitrumContractTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case ArbitrumRetryTxType:
		tx := new(ArbitrumRetryTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case ArbitrumSubmitRetryableTxType:
		tx := new(ArbitrumSubmitRetryableTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case ArbitrumLegacyTxType:
		tx := new(ArbitrumLegacyTxData)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	}
	return nil, nil
}

func (tx *ArbitrumContractTx) UnmarshalJSON(_ []byte) error {
	return fmt.Errorf("JSON unmarshaling not implemented for ArbitrumContractTx (type 0x%x)", ArbitrumContractTxType)
}

func (tx *ArbitrumRetryTx) UnmarshalJSON(_ []byte) error {
	return fmt.Errorf("JSON unmarshaling not implemented for ArbitrumRetryTx (type 0x%x)", ArbitrumRetryTxType)
}

func (tx *ArbitrumSubmitRetryableTx) UnmarshalJSON(_ []byte) error {
	return fmt.Errorf("JSON unmarshaling not implemented for ArbitrumSubmitRetryableTx (type 0x%x)", ArbitrumSubmitRetryableTxType)
}

func (tx *ArbitrumDepositTx) UnmarshalJSON(_ []byte) error {
	return fmt.Errorf("JSON unmarshaling not implemented for ArbitrumDepositTx (type 0x%x)", ArbitrumDepositTxType)
}

func (tx *ArbitrumUnsignedTx) UnmarshalJSON(_ []byte) error {
	return fmt.Errorf("JSON unmarshaling not implemented for ArbitrumUnsignedTx (type 0x%x)", ArbitrumUnsignedTxType)
}

func (tx *ArbitrumInternalTx) UnmarshalJSON(_ []byte) error {
	return fmt.Errorf("JSON unmarshaling not implemented for ArbitrumInternalTx (type 0x%x)", ArbitrumInternalTxType)
}
