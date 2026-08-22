// Copyright 2021 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	"github.com/valyala/fastjson"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// txJSON is the JSON representation of transactions.
type txJSON struct {
	Type hexutil.Uint64 `json:"type"`

	// Common transaction fields:
	Nonce                *hexutil.Uint64 `json:"nonce"`
	GasPrice             *hexutil.U256   `json:"gasPrice"`
	MaxFeePerGas         *hexutil.U256   `json:"maxFeePerGas"`
	MaxPriorityFeePerGas *hexutil.U256   `json:"maxPriorityFeePerGas"`
	Gas                  *hexutil.Uint64 `json:"gas"`
	Value                *hexutil.U256   `json:"value"`
	Data                 *hexutil.Bytes  `json:"input"`
	V                    *hexutil.U256   `json:"v"`
	R                    *hexutil.U256   `json:"r"`
	S                    *hexutil.U256   `json:"s"`
	To                   *common.Address `json:"to"`

	// Access list transaction fields:
	ChainID        *hexutil.U256        `json:"chainId,omitempty"`
	AccessList     *AccessList          `json:"accessList,omitempty"`
	Authorizations *[]JsonAuthorization `json:"authorizationList,omitempty"`

	// Blob transaction fields:
	MaxFeePerBlobGas    *hexutil.U256 `json:"maxFeePerBlobGas,omitempty"`
	BlobVersionedHashes []common.Hash `json:"blobVersionedHashes,omitempty"`
	// Blob wrapper fields:
	Blobs       Blobs     `json:"blobs,omitempty"`
	Commitments BlobKzgs  `json:"commitments,omitempty"`
	Proofs      KZGProofs `json:"proofs,omitempty"`

	// Only used for encoding:
	Hash common.Hash `json:"hash"`
}

type JsonAuthorization struct {
	ChainID hexutil.U256   `json:"chainId"`
	Address common.Address `json:"address"`
	Nonce   hexutil.Uint64 `json:"nonce"`
	YParity hexutil.Uint64 `json:"yParity"`
	R       hexutil.U256   `json:"r"`
	S       hexutil.U256   `json:"s"`
}

func (a JsonAuthorization) FromAuthorization(authorization Authorization) JsonAuthorization {
	a.ChainID = hexutil.U256(authorization.ChainID)
	a.Address = authorization.Address
	a.Nonce = (hexutil.Uint64)(authorization.Nonce)

	a.YParity = (hexutil.Uint64)(authorization.YParity)
	a.R = hexutil.U256(authorization.R)
	a.S = hexutil.U256(authorization.S)
	return a
}

func (a JsonAuthorization) ToAuthorization() (Authorization, error) {
	auth := Authorization{
		Address: a.Address,
		Nonce:   a.Nonce.Uint64(),
		ChainID: uint256.Int(a.ChainID),
		R:       uint256.Int(a.R),
		S:       uint256.Int(a.S),
	}
	yParity := a.YParity.Uint64()
	if yParity >= 1<<8 {
		return auth, errors.New("y parity in authorization does not fit in 8 bits")
	}
	auth.YParity = uint8(yParity)
	return auth, nil
}

func (tx *LegacyTx) MarshalJSON() ([]byte, error) {
	var enc txJSON
	// These are set for all txn types.
	enc.Hash = tx.Hash()
	enc.Type = hexutil.Uint64(tx.Type())
	enc.Nonce = (*hexutil.Uint64)(&tx.Nonce)
	enc.Gas = (*hexutil.Uint64)(&tx.GasLimit)
	enc.GasPrice = (*hexutil.U256)(&tx.GasPrice)
	enc.Value = (*hexutil.U256)(&tx.Value)
	enc.Data = (*hexutil.Bytes)(&tx.Data)
	enc.To = tx.To
	enc.V = (*hexutil.U256)(&tx.V)
	enc.R = (*hexutil.U256)(&tx.R)
	enc.S = (*hexutil.U256)(&tx.S)
	if tx.Protected() {
		enc.ChainID = (*hexutil.U256)(tx.GetChainID())
	}
	return json.Marshal(&enc)
}

func (tx *AccessListTx) MarshalJSON() ([]byte, error) {
	var enc txJSON
	// These are set for all txn types.
	enc.Hash = tx.Hash()
	enc.Type = hexutil.Uint64(tx.Type())
	enc.ChainID = (*hexutil.U256)(&tx.ChainID)
	enc.AccessList = &tx.AccessList
	enc.Nonce = (*hexutil.Uint64)(&tx.Nonce)
	enc.Gas = (*hexutil.Uint64)(&tx.GasLimit)
	enc.GasPrice = (*hexutil.U256)(&tx.GasPrice)
	enc.Value = (*hexutil.U256)(&tx.Value)
	enc.Data = (*hexutil.Bytes)(&tx.Data)
	enc.To = tx.To
	enc.V = (*hexutil.U256)(&tx.V)
	enc.R = (*hexutil.U256)(&tx.R)
	enc.S = (*hexutil.U256)(&tx.S)
	return json.Marshal(&enc)
}

func (tx *DynamicFeeTransaction) MarshalJSON() ([]byte, error) {
	var enc txJSON
	// These are set for all txn types.
	enc.Hash = tx.Hash()
	enc.Type = hexutil.Uint64(tx.Type())
	enc.ChainID = (*hexutil.U256)(&tx.ChainID)
	enc.AccessList = &tx.AccessList
	enc.Nonce = (*hexutil.Uint64)(&tx.Nonce)
	enc.Gas = (*hexutil.Uint64)(&tx.GasLimit)
	enc.MaxFeePerGas = (*hexutil.U256)(&tx.FeeCap)
	enc.MaxPriorityFeePerGas = (*hexutil.U256)(&tx.TipCap)
	enc.Value = (*hexutil.U256)(&tx.Value)
	enc.Data = (*hexutil.Bytes)(&tx.Data)
	enc.To = tx.To
	enc.V = (*hexutil.U256)(&tx.V)
	enc.R = (*hexutil.U256)(&tx.R)
	enc.S = (*hexutil.U256)(&tx.S)
	return json.Marshal(&enc)
}

func toBlobTxJSON(tx *BlobTx) *txJSON {
	var enc txJSON
	// These are set for all txn types.
	enc.Hash = tx.Hash()
	enc.Type = hexutil.Uint64(tx.Type())
	enc.ChainID = (*hexutil.U256)(&tx.ChainID)
	enc.AccessList = &tx.AccessList
	enc.Nonce = (*hexutil.Uint64)(&tx.Nonce)
	enc.Gas = (*hexutil.Uint64)(&tx.GasLimit)
	enc.MaxFeePerGas = (*hexutil.U256)(&tx.FeeCap)
	enc.MaxPriorityFeePerGas = (*hexutil.U256)(&tx.TipCap)
	enc.Value = (*hexutil.U256)(&tx.Value)
	enc.Data = (*hexutil.Bytes)(&tx.Data)
	enc.To = tx.To
	enc.V = (*hexutil.U256)(&tx.V)
	enc.R = (*hexutil.U256)(&tx.R)
	enc.S = (*hexutil.U256)(&tx.S)
	enc.MaxFeePerBlobGas = (*hexutil.U256)(&tx.MaxFeePerBlobGas)
	enc.BlobVersionedHashes = tx.GetBlobHashes()
	return &enc
}

func (tx *BlobTx) MarshalJSON() ([]byte, error) {
	return json.Marshal(toBlobTxJSON(tx))
}

func (tx *BlobTxWrapper) MarshalJSON() ([]byte, error) {
	enc := toBlobTxJSON(&tx.Tx)

	enc.Blobs = tx.Blobs
	enc.Commitments = tx.Commitments
	enc.Proofs = tx.Proofs

	return json.Marshal(enc)
}

func UnmarshalTransactionFromJSON(input []byte) (Transaction, error) {
	var p fastjson.Parser
	v, err := p.ParseBytes(input)
	if err != nil {
		return nil, fmt.Errorf("parse transaction json: %w", err)
	}
	// check the type
	txTypeHex := v.GetStringBytes("type")
	var txType hexutil.Uint64 = LegacyTxType
	if txTypeHex != nil {
		if err := txType.UnmarshalText(txTypeHex); err != nil {
			return nil, err
		}
	}
	if txType > 0xff {
		return nil, fmt.Errorf("unknown transaction type: %v", txType)
	}
	switch byte(txType) {
	case LegacyTxType:
		tx := &LegacyTx{}
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case AccessListTxType:
		tx := &AccessListTx{}
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case DynamicFeeTxType:
		tx := &DynamicFeeTransaction{}
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	case BlobTxType:
		tx, err := UnmarshalBlobTxJSON(input)
		if err != nil {
			return nil, err
		}
		return tx, nil
	case SetCodeTxType:
		tx := &SetCodeTransaction{}
		if err := tx.UnmarshalJSON(input); err != nil {
			return nil, err
		}
		return tx, nil
	default:
		if spec, ok := registeredTxType(byte(txType)); ok && uint64(txType) < 0x80 && spec.UnmarshalJSON != nil {
			return spec.UnmarshalJSON(input)
		}
		return nil, fmt.Errorf("unknown transaction type: %v", txType)
	}
}

func (tx *LegacyTx) UnmarshalJSON(input []byte) error {
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.To != nil {
		tx.To = dec.To
	}
	if dec.Nonce == nil {
		return errors.New("missing required field 'nonce' in transaction")
	}
	tx.Nonce = uint64(*dec.Nonce)
	if dec.GasPrice == nil {
		return errors.New("missing required field 'gasPrice' in transaction")
	}
	tx.GasPrice = uint256.Int(*dec.GasPrice)
	if dec.Gas == nil {
		return errors.New("missing required field 'gas' in transaction")
	}
	tx.GasLimit = uint64(*dec.Gas)
	if dec.Value == nil {
		return errors.New("missing required field 'value' in transaction")
	}
	tx.Value = uint256.Int(*dec.Value)
	if dec.Data == nil {
		return errors.New("missing required field 'input' in transaction")
	}
	tx.Data = *dec.Data
	if dec.V == nil {
		return errors.New("missing required field 'v' in transaction")
	}
	tx.V = uint256.Int(*dec.V)
	if dec.R == nil {
		return errors.New("missing required field 'r' in transaction")
	}
	tx.R = uint256.Int(*dec.R)
	if dec.S == nil {
		return errors.New("missing required field 's' in transaction")
	}
	tx.S = uint256.Int(*dec.S)
	withSignature := !tx.V.IsZero() || !tx.R.IsZero() || !tx.S.IsZero()
	if withSignature {
		if err := sanityCheckSignature(&tx.V, &tx.R, &tx.S, true); err != nil {
			return err
		}
	}
	return nil
}

func (tx *AccessListTx) UnmarshalJSON(input []byte) error {
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	// Access list is optional for now.
	if dec.AccessList != nil {
		tx.AccessList = *dec.AccessList
	}
	if dec.ChainID == nil {
		return errors.New("missing required field 'chainId' in transaction")
	}
	tx.ChainID = uint256.Int(*dec.ChainID)
	if dec.To != nil {
		tx.To = dec.To
	}
	if dec.Nonce == nil {
		return errors.New("missing required field 'nonce' in transaction")
	}
	tx.Nonce = uint64(*dec.Nonce)
	if dec.GasPrice == nil {
		return errors.New("missing required field 'gasPrice' in transaction")
	}
	tx.GasPrice = uint256.Int(*dec.GasPrice)
	if dec.Gas == nil {
		return errors.New("missing required field 'gas' in transaction")
	}
	tx.GasLimit = uint64(*dec.Gas)
	if dec.Value == nil {
		return errors.New("missing required field 'value' in transaction")
	}
	tx.Value = uint256.Int(*dec.Value)
	if dec.Data == nil {
		return errors.New("missing required field 'input' in transaction")
	}
	tx.Data = *dec.Data
	if dec.V == nil {
		return errors.New("missing required field 'v' in transaction")
	}
	tx.V = uint256.Int(*dec.V)
	if dec.R == nil {
		return errors.New("missing required field 'r' in transaction")
	}
	tx.R = uint256.Int(*dec.R)
	if dec.S == nil {
		return errors.New("missing required field 's' in transaction")
	}
	tx.S = uint256.Int(*dec.S)
	withSignature := !tx.V.IsZero() || !tx.R.IsZero() || !tx.S.IsZero()
	if withSignature {
		if err := sanityCheckSignature(&tx.V, &tx.R, &tx.S, false); err != nil {
			return err
		}
	}
	return nil
}

func (tx *DynamicFeeTransaction) unmarshalJson(dec txJSON) error {
	// Access list is optional for now.
	if dec.AccessList != nil {
		tx.AccessList = *dec.AccessList
	}
	if dec.ChainID == nil {
		return errors.New("missing required field 'chainId' in transaction")
	}
	tx.ChainID = uint256.Int(*dec.ChainID)
	if dec.To != nil {
		tx.To = dec.To
	}
	if dec.Nonce == nil {
		return errors.New("missing required field 'nonce' in transaction")
	}
	tx.Nonce = uint64(*dec.Nonce)
	if dec.MaxPriorityFeePerGas == nil {
		return errors.New("missing required field 'maxPriorityFeePerGas' in transaction")
	}
	tx.TipCap = uint256.Int(*dec.MaxPriorityFeePerGas)
	if dec.MaxFeePerGas == nil {
		return errors.New("missing required field 'maxFeePerGas' in transaction")
	}
	tx.FeeCap = uint256.Int(*dec.MaxFeePerGas)
	if dec.Gas == nil {
		return errors.New("missing required field 'gas' in transaction")
	}
	tx.GasLimit = uint64(*dec.Gas)
	if dec.Value == nil {
		return errors.New("missing required field 'value' in transaction")
	}
	tx.Value = uint256.Int(*dec.Value)
	if dec.Data == nil {
		return errors.New("missing required field 'input' in transaction")
	}
	tx.Data = *dec.Data
	if dec.V == nil {
		return errors.New("missing required field 'v' in transaction")
	}
	tx.V = uint256.Int(*dec.V)
	if dec.R == nil {
		return errors.New("missing required field 'r' in transaction")
	}
	tx.R = uint256.Int(*dec.R)
	if dec.S == nil {
		return errors.New("missing required field 's' in transaction")
	}
	tx.S = uint256.Int(*dec.S)
	withSignature := !tx.V.IsZero() || !tx.R.IsZero() || !tx.S.IsZero()
	if withSignature {
		if err := sanityCheckSignature(&tx.V, &tx.R, &tx.S, false); err != nil {
			return err
		}
	}
	return nil
}

func (tx *DynamicFeeTransaction) UnmarshalJSON(input []byte) error {
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}

	return tx.unmarshalJson(dec)
}

func (tx *SetCodeTransaction) UnmarshalJSON(input []byte) error {
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}

	if err := tx.unmarshalJson(dec); err != nil {
		return err
	}
	if dec.Authorizations == nil {
		return errors.New("missing required field 'authorizationList' in transaction")
	}
	auths := *dec.Authorizations
	tx.Authorizations = make([]Authorization, len(auths))
	for i := range auths {
		var err error
		tx.Authorizations[i], err = auths[i].ToAuthorization()
		if err != nil {
			return err
		}
	}
	return nil
}

func UnmarshalBlobTxJSON(input []byte) (Transaction, error) {
	var dec txJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return nil, err
	}
	tx := BlobTx{}
	if dec.AccessList != nil {
		tx.AccessList = *dec.AccessList
	} else {
		tx.AccessList = []AccessTuple{}
	}
	if dec.ChainID == nil {
		return nil, errors.New("missing required field 'chainId' in transaction")
	}
	tx.ChainID = uint256.Int(*dec.ChainID)
	if dec.To != nil {
		tx.To = dec.To
	}
	if dec.Nonce == nil {
		return nil, errors.New("missing required field 'nonce' in transaction")
	}
	tx.Nonce = uint64(*dec.Nonce)
	if dec.MaxPriorityFeePerGas == nil {
		return nil, errors.New("missing required field 'maxPriorityFeePerGas' in transaction")
	}
	tx.TipCap = uint256.Int(*dec.MaxPriorityFeePerGas)
	if dec.MaxFeePerGas == nil {
		return nil, errors.New("missing required field 'maxFeePerGas' in transaction")
	}
	tx.FeeCap = uint256.Int(*dec.MaxFeePerGas)
	if dec.Gas == nil {
		return nil, errors.New("missing required field 'gas' in transaction")
	}
	tx.GasLimit = uint64(*dec.Gas)
	if dec.Value == nil {
		return nil, errors.New("missing required field 'value' in transaction")
	}
	tx.Value = uint256.Int(*dec.Value)
	if dec.Data == nil {
		return nil, errors.New("missing required field 'input' in transaction")
	}
	tx.Data = *dec.Data

	if dec.MaxFeePerBlobGas == nil {
		return nil, errors.New("missing required field 'maxFeePerBlobGas' in transaction")
	}

	tx.MaxFeePerBlobGas = uint256.Int(*dec.MaxFeePerBlobGas)

	if dec.BlobVersionedHashes != nil {
		tx.BlobVersionedHashes = dec.BlobVersionedHashes
	} else {
		tx.BlobVersionedHashes = []common.Hash{}
	}

	if dec.V == nil {
		return nil, errors.New("missing required field 'v' in transaction")
	}
	tx.V = uint256.Int(*dec.V)
	if dec.R == nil {
		return nil, errors.New("missing required field 'r' in transaction")
	}
	tx.R = uint256.Int(*dec.R)
	if dec.S == nil {
		return nil, errors.New("missing required field 's' in transaction")
	}
	tx.S = uint256.Int(*dec.S)

	withSignature := !tx.V.IsZero() || !tx.R.IsZero() || !tx.S.IsZero()
	if withSignature {
		if err := sanityCheckSignature(&tx.V, &tx.R, &tx.S, false); err != nil {
			return nil, err
		}
	}

	if len(dec.Blobs) == 0 {
		// if no blobs are specified in the json we assume it is an unwrapped blob tx
		return &tx, nil
	}

	btx := BlobTxWrapper{
		Tx:          tx.copyData(),
		Commitments: dec.Commitments,
		Blobs:       dec.Blobs,
		Proofs:      dec.Proofs,
	}
	err := btx.ValidateBlobTransactionWrapper()
	if err != nil {
		return nil, err
	}
	return &btx, nil
}
