package types

import (
	"encoding/json"
	"errors"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

// txJSON is the JSON representation of transactions.
type txJSON struct {
	Type hexutil.Uint64 `json:"type"`

	// Common transaction fields:
	Nonce    *hexutil.Uint64 `json:"nonce"`
	GasPrice *hexutil.Big    `json:"gasPrice"`
	Gas      *hexutil.Uint64 `json:"gas"`
	Value    *hexutil.Big    `json:"value"`
	Data     *hexutil.Bytes  `json:"input"`
	V        *hexutil.Big    `json:"v"`
	R        *hexutil.Big    `json:"r"`
	S        *hexutil.Big    `json:"s"`
	To       *common.Address `json:"to"`

	// Access list transaction fields:
	ChainID    *hexutil.Big `json:"chainId,omitempty"`
	AccessList *AccessList  `json:"accessList,omitempty"`

	// Only used for encoding:
	Hash common.Hash `json:"hash"`
}

// MarshalJSON marshals as JSON with a hash.
func (t *Transaction) MarshalJSON() ([]byte, error) {
	var enc txJSON
	// These are set for all tx types.
	enc.Hash = t.Hash()
	enc.Type = hexutil.Uint64(t.Type())

	// Other fields are set conditionally depending on tx type.
	switch tx := t.inner.(type) {
	case *LegacyTx:
		enc.Nonce = (*hexutil.Uint64)(&tx.Nonce)
		enc.Gas = (*hexutil.Uint64)(&tx.Gas)
		enc.GasPrice = (*hexutil.Big)(tx.GasPrice.ToBig())
		enc.Value = (*hexutil.Big)(tx.Value.ToBig())
		enc.Data = (*hexutil.Bytes)(&tx.Data)
		enc.To = t.To()
		enc.V = (*hexutil.Big)(tx.V.ToBig())
		enc.R = (*hexutil.Big)(tx.R.ToBig())
		enc.S = (*hexutil.Big)(tx.S.ToBig())
	case *AccessListTx:
		enc.ChainID = (*hexutil.Big)(tx.ChainID.ToBig())
		enc.AccessList = &tx.AccessList
		enc.Nonce = (*hexutil.Uint64)(&tx.Nonce)
		enc.Gas = (*hexutil.Uint64)(&tx.Gas)
		enc.GasPrice = (*hexutil.Big)(tx.GasPrice.ToBig())
		enc.Value = (*hexutil.Big)(tx.Value.ToBig())
		enc.Data = (*hexutil.Bytes)(&tx.Data)
		enc.To = t.To()
		enc.V = (*hexutil.Big)(tx.V.ToBig())
		enc.R = (*hexutil.Big)(tx.R.ToBig())
		enc.S = (*hexutil.Big)(tx.S.ToBig())
	}
	return json.Marshal(&enc)
}

// UnmarshalJSON unmarshals from JSON.
func (t *Transaction) UnmarshalJSON(input []byte) error {
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}

	// Decode / verify fields according to transaction type.
	var inner TxData
	switch dec.Type {
	case LegacyTxType:
		var itx LegacyTx
		inner = &itx
		if dec.To != nil {
			itx.To = dec.To
		}
		if dec.Nonce == nil {
			return errors.New("missing required field 'nonce' in transaction")
		}
		itx.Nonce = uint64(*dec.Nonce)
		if dec.GasPrice == nil {
			return errors.New("missing required field 'gasPrice' in transaction")
		}
		var overflow bool
		itx.GasPrice, overflow = uint256.FromBig(dec.GasPrice.ToInt())
		if overflow {
			return errors.New("'gasPrice' in transaction does not fit in 256 bits")
		}
		if dec.Gas == nil {
			return errors.New("missing required field 'gas' in transaction")
		}
		itx.Gas = uint64(*dec.Gas)
		if dec.Value == nil {
			return errors.New("missing required field 'value' in transaction")
		}
		itx.Value, overflow = uint256.FromBig(dec.Value.ToInt())
		if overflow {
			return errors.New("'value' in transaction does not fit in 256 bits")
		}
		if dec.Data == nil {
			return errors.New("missing required field 'input' in transaction")
		}
		itx.Data = *dec.Data
		if dec.V == nil {
			return errors.New("missing required field 'v' in transaction")
		}
		itx.V, overflow = uint256.FromBig(dec.V.ToInt())
		if overflow {
			return errors.New("'v' in transaction does not fit in 256 bits")
		}
		if dec.R == nil {
			return errors.New("missing required field 'r' in transaction")
		}
		itx.R, overflow = uint256.FromBig(dec.R.ToInt())
		if overflow {
			return errors.New("'r' in transaction does not fit in 256 bits")
		}
		if dec.S == nil {
			return errors.New("missing required field 's' in transaction")
		}
		itx.S, overflow = uint256.FromBig(dec.S.ToInt())
		if overflow {
			return errors.New("'s' in transaction does not fit in 256 bits")
		}
		withSignature := itx.V.Sign() != 0 || itx.R.Sign() != 0 || itx.S.Sign() != 0
		if withSignature {
			if err := sanityCheckSignature(itx.V, itx.R, itx.S, true); err != nil {
				return err
			}
		}

	case AccessListTxType:
		var itx AccessListTx
		inner = &itx
		// Access list is optional for now.
		if dec.AccessList != nil {
			itx.AccessList = *dec.AccessList
		}
		if dec.ChainID == nil {
			return errors.New("missing required field 'chainId' in transaction")
		}
		var overflow bool
		itx.ChainID, overflow = uint256.FromBig(dec.ChainID.ToInt())
		if overflow {
			return errors.New("'chainId' in transaction does not fit in 256 bits")
		}
		if dec.To != nil {
			itx.To = dec.To
		}
		if dec.Nonce == nil {
			return errors.New("missing required field 'nonce' in transaction")
		}
		itx.Nonce = uint64(*dec.Nonce)
		if dec.GasPrice == nil {
			return errors.New("missing required field 'gasPrice' in transaction")
		}
		itx.GasPrice, overflow = uint256.FromBig(dec.GasPrice.ToInt())
		if overflow {
			return errors.New("'gasPrice' in transaction does not fit in 256 bits")
		}
		if dec.Gas == nil {
			return errors.New("missing required field 'gas' in transaction")
		}
		itx.Gas = uint64(*dec.Gas)
		if dec.Value == nil {
			return errors.New("missing required field 'value' in transaction")
		}
		itx.Value, overflow = uint256.FromBig(dec.Value.ToInt())
		if overflow {
			return errors.New("'value' in transaction does not fit in 256 bits")
		}
		if dec.Data == nil {
			return errors.New("missing required field 'input' in transaction")
		}
		itx.Data = *dec.Data
		if dec.V == nil {
			return errors.New("missing required field 'v' in transaction")
		}
		itx.V, overflow = uint256.FromBig(dec.V.ToInt())
		if overflow {
			return errors.New("'v' in transaction does not fit in 256 bits")
		}
		if dec.R == nil {
			return errors.New("missing required field 'r' in transaction")
		}
		itx.R, overflow = uint256.FromBig(dec.R.ToInt())
		if overflow {
			return errors.New("'r' in transaction does not fit in 256 bits")
		}
		if dec.S == nil {
			return errors.New("missing required field 's' in transaction")
		}
		itx.S, overflow = uint256.FromBig(dec.S.ToInt())
		if overflow {
			return errors.New("'s' in transaction does not fit in 256 bits")
		}
		withSignature := itx.V.Sign() != 0 || itx.R.Sign() != 0 || itx.S.Sign() != 0
		if withSignature {
			if err := sanityCheckSignature(itx.V, itx.R, itx.S, false); err != nil {
				return err
			}
		}

	default:
		return ErrTxTypeNotSupported
	}

	// Now set the inner transaction.
	t.setDecoded(inner, 0)

	// TODO: check hash here?
	return nil
}
