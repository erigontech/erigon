package txn

import (
	"encoding/json"

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
	case ArbitrumInternalTxType:
		tx := new(ArbitrumInternalTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case ArbitrumUnsignedTxType:
		tx := new(ArbitrumUnsignedTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case ArbitrumContractTxType:
		tx := new(ArbitrumContractTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case ArbitrumRetryTxType:
		tx := new(ArbitrumRetryTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case ArbitrumSubmitRetryableTxType:
		tx := new(ArbitrumSubmitRetryableTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case ArbitrumLegacyTxType:
		tx := new(ArbitrumLegacyTxData)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (tx *ArbitrumContractTx) UnmarshalJSON(input []byte) error {
	var dec types.TxJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	return nil
}
func (tx *ArbitrumRetryTx) UnmarshalJSON(input []byte) error {
	var dec types.TxJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	return nil
}
func (tx *ArbitrumSubmitRetryableTx) UnmarshalJSON(input []byte) error {
	var dec types.TxJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumDepositTx) UnmarshalJSON(input []byte) error {
	var dec types.TxJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumUnsignedTx) UnmarshalJSON(input []byte) error {
	var dec types.TxJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	return nil
}

func (tx *ArbitrumInternalTx) UnmarshalJSON(input []byte) error {
	var dec types.TxJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	return nil
}
