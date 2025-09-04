package types

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/holiman/uint256"
)

type ArbitrumLegacyTxData struct {
	LegacyTx
	HashOverride      common.Hash // Hash cannot be locally computed from other fields
	EffectiveGasPrice uint64
	L1BlockNumber     uint64
	OverrideSender    *common.Address `rlp:"optional,nil"` // only used in unsigned Txs
}

func NewArbitrumLegacyTx(origTx Transaction, hashOverride common.Hash, effectiveGas uint64, l1Block uint64, senderOverride *common.Address) (Transaction, error) {
	if origTx.Type() != LegacyTxType {
		return nil, errors.New("attempt to arbitrum-wrap non-legacy transaction")
	}
	legacyPtr := origTx.(*LegacyTx)
	inner := ArbitrumLegacyTxData{
		LegacyTx:          *legacyPtr,
		HashOverride:      hashOverride,
		EffectiveGasPrice: effectiveGas,
		L1BlockNumber:     l1Block,
		OverrideSender:    senderOverride,
	}
	return NewArbTx(&inner), nil
}

// func (tx *ArbitrumLegacyTxData) copy() *ArbitrumLegacyTxData {
// 	legacyCopy := tx.LegacyTx.copy()
// 	var sender *common.Address
// 	if tx.Sender != nil {
// 		sender = new(common.Address)
// 		*sender = *tx.Sender()
// 	}
// 	return &ArbitrumLegacyTxData{
// 		LegacyTx:          *legacyCopy,
// 		HashOverride:      tx.HashOverride,
// 		EffectiveGasPrice: tx.EffectiveGasPrice,
// 		L1BlockNumber:     tx.L1BlockNumber,
// 		OverrideSender:    sender,
// 	}
// }

func (tx *ArbitrumLegacyTxData) Type() byte { return ArbitrumLegacyTxType }

func (tx *ArbitrumLegacyTxData) EncodeRLP(w io.Writer) error {
	// Create a clean struct with only the fields that should be RLP encoded
	// This avoids encoding the TransactionMisc cache fields
	legacyData := struct {
		Nonce    uint64
		GasPrice *uint256.Int
		GasLimit uint64
		To       *common.Address `rlp:"nil"`
		Value    *uint256.Int
		Data     []byte
		V, R, S  uint256.Int
	}{
		Nonce:    tx.Nonce,
		GasPrice: tx.GasPrice,
		GasLimit: tx.GasLimit,
		To:       tx.To,
		Value:    tx.Value,
		Data:     tx.Data,
		V:        tx.V,
		R:        tx.R,
		S:        tx.S,
	}
	
	// Encode the legacy transaction fields
	legacyBytes, err := rlp.EncodeToBytes(legacyData)
	if err != nil {
		return err
	}
	
	// Then encode the entire structure with LegacyTx as an encapsulated byte slice
	return rlp.Encode(w, struct {
		LegacyTxBytes     []byte          // Encapsulated RLP-encoded LegacyTx fields
		HashOverride      common.Hash
		EffectiveGasPrice uint64
		L1BlockNumber     uint64
		OverrideSender    *common.Address `rlp:"nil"`
	}{
		LegacyTxBytes:     legacyBytes,
		HashOverride:      tx.HashOverride,
		EffectiveGasPrice: tx.EffectiveGasPrice,
		L1BlockNumber:     tx.L1BlockNumber,
		OverrideSender:    tx.OverrideSender,
	})
}

func (tx *ArbitrumLegacyTxData) DecodeRLP(s *rlp.Stream) error {
	// Decode into a temporary struct with LegacyTx as bytes
	var temp struct {
		LegacyTxBytes     []byte
		HashOverride      common.Hash
		EffectiveGasPrice uint64
		L1BlockNumber     uint64
		OverrideSender    *common.Address `rlp:"nil"`
	}
	
	if err := s.Decode(&temp); err != nil {
		return err
	}
	
	// Decode the legacy transaction fields from bytes
	var legacyData struct {
		Nonce    uint64
		GasPrice *uint256.Int
		GasLimit uint64
		To       *common.Address `rlp:"nil"`
		Value    *uint256.Int
		Data     []byte
		V, R, S  uint256.Int
	}
	
	if err := rlp.DecodeBytes(temp.LegacyTxBytes, &legacyData); err != nil {
		return err
	}
	
	// Copy the decoded legacy fields to the transaction
	tx.Nonce = legacyData.Nonce
	tx.GasPrice = legacyData.GasPrice
	tx.GasLimit = legacyData.GasLimit
	tx.To = legacyData.To
	tx.Value = legacyData.Value
	tx.Data = legacyData.Data
	tx.V = legacyData.V
	tx.R = legacyData.R
	tx.S = legacyData.S
	
	// Copy Arbitrum-specific fields
	tx.HashOverride = temp.HashOverride
	tx.EffectiveGasPrice = temp.EffectiveGasPrice
	tx.L1BlockNumber = temp.L1BlockNumber
	tx.OverrideSender = temp.OverrideSender
	
	return nil
}

// arbitrumLegacyTxJSON represents the JSON structure for ArbitrumLegacyTxData
type arbitrumLegacyTxJSON struct {
	Type              hexutil.Uint64  `json:"type"`
	Hash              common.Hash     `json:"hash"`
	Nonce             *hexutil.Uint64 `json:"nonce"`
	GasPrice          *hexutil.Big    `json:"gasPrice"`
	Gas               *hexutil.Uint64 `json:"gas"`
	To                *common.Address `json:"to"`
	Value             *hexutil.Big    `json:"value"`
	Data              *hexutil.Bytes  `json:"input"`
	V                 *hexutil.Big    `json:"v"`
	R                 *hexutil.Big    `json:"r"`
	S                 *hexutil.Big    `json:"s"`
	HashOverride      common.Hash     `json:"hashOverride"`
	EffectiveGasPrice *hexutil.Uint64 `json:"effectiveGasPrice"`
	L1BlockNumber     *hexutil.Uint64 `json:"l1BlockNumber"`
	OverrideSender    *common.Address `json:"overrideSender,omitempty"`
}

func (tx *ArbitrumLegacyTxData) MarshalJSON() ([]byte, error) {
	var enc arbitrumLegacyTxJSON

	// These are set for all txn types.
	enc.Type = hexutil.Uint64(tx.Type())
	enc.Hash = tx.HashOverride // For ArbitrumLegacyTxData, hash comes from HashOverride
	enc.Nonce = (*hexutil.Uint64)(&tx.Nonce)
	enc.Gas = (*hexutil.Uint64)(&tx.GasLimit)
	enc.GasPrice = (*hexutil.Big)(tx.GasPrice.ToBig())
	enc.Value = (*hexutil.Big)(tx.Value.ToBig())
	enc.Data = (*hexutil.Bytes)(&tx.Data)
	enc.To = tx.To
	enc.V = (*hexutil.Big)(tx.V.ToBig())
	enc.R = (*hexutil.Big)(tx.R.ToBig())
	enc.S = (*hexutil.Big)(tx.S.ToBig())

	// Arbitrum-specific fields
	enc.HashOverride = tx.HashOverride
	enc.EffectiveGasPrice = (*hexutil.Uint64)(&tx.EffectiveGasPrice)
	enc.L1BlockNumber = (*hexutil.Uint64)(&tx.L1BlockNumber)
	enc.OverrideSender = tx.OverrideSender

	return json.Marshal(&enc)
}

func (tx *ArbitrumLegacyTxData) UnmarshalJSON(input []byte) error {
	var dec arbitrumLegacyTxJSON
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}

	// Validate and set common fields
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
	var overflow bool
	tx.GasPrice, overflow = uint256.FromBig(dec.GasPrice.ToInt())
	if overflow {
		return errors.New("'gasPrice' in transaction does not fit in 256 bits")
	}

	if dec.Gas == nil {
		return errors.New("missing required field 'gas' in transaction")
	}
	tx.GasLimit = uint64(*dec.Gas)

	if dec.Value == nil {
		return errors.New("missing required field 'value' in transaction")
	}
	tx.Value, overflow = uint256.FromBig(dec.Value.ToInt())
	if overflow {
		return errors.New("'value' in transaction does not fit in 256 bits")
	}

	if dec.Data == nil {
		return errors.New("missing required field 'input' in transaction")
	}
	tx.Data = *dec.Data

	// Decode signature fields
	if dec.V == nil {
		return errors.New("missing required field 'v' in transaction")
	}
	overflow = tx.V.SetFromBig(dec.V.ToInt())
	if overflow {
		return errors.New("dec.V higher than 2^256-1")
	}

	if dec.R == nil {
		return errors.New("missing required field 'r' in transaction")
	}
	overflow = tx.R.SetFromBig(dec.R.ToInt())
	if overflow {
		return errors.New("dec.R higher than 2^256-1")
	}

	if dec.S == nil {
		return errors.New("missing required field 's' in transaction")
	}
	overflow = tx.S.SetFromBig(dec.S.ToInt())
	if overflow {
		return errors.New("dec.S higher than 2^256-1")
	}

	// Validate signature if present
	withSignature := !tx.V.IsZero() || !tx.R.IsZero() || !tx.S.IsZero()
	if withSignature {
		if err := sanityCheckSignature(&tx.V, &tx.R, &tx.S, true); err != nil {
			return err
		}
	}

	// Arbitrum-specific fields
	tx.HashOverride = dec.HashOverride

	if dec.EffectiveGasPrice != nil {
		tx.EffectiveGasPrice = uint64(*dec.EffectiveGasPrice)
	}

	if dec.L1BlockNumber != nil {
		tx.L1BlockNumber = uint64(*dec.L1BlockNumber)
	}

	tx.OverrideSender = dec.OverrideSender

	return nil
}
