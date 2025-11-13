package txn

import (
	"encoding/json"

	"github.com/erigontech/erigon/arb/txn_types"
	"github.com/erigontech/erigon/execution/types"
)

func init() {
	types.UnmarshalExtTxnFunc = ArbUnmarshalJSON
}

func ArbUnmarshalJSON(txType byte, input []byte) (types.Transaction, error) {
	switch txType {
	case txn_types.ArbitrumDepositTxType:
		tx := new(ArbitrumDepositTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case txn_types.ArbitrumInternalTxType:
		tx := new(ArbitrumInternalTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case txn_types.ArbitrumUnsignedTxType:
		tx := new(ArbitrumUnsignedTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case txn_types.ArbitrumContractTxType:
		tx := new(ArbitrumContractTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case txn_types.ArbitrumRetryTxType:
		tx := new(ArbitrumRetryTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case txn_types.ArbitrumSubmitRetryableTxType:
		tx := new(ArbitrumSubmitRetryableTx)
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
	case txn_types.ArbitrumLegacyTxType:
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
