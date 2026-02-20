package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/holiman/uint256"
)

type ArbitrumLegacyTxData struct {
	*LegacyTx
	HashOverride      common.Hash // Hash cannot be locally computed from other fields
	EffectiveGasPrice uint64
	L1BlockNumber     uint64
	OverrideSender    *common.Address `rlp:"optional,nil"` // only used in unsigned Txs
}

func NewArbitrumLegacyTx(origTx Transaction, hashOverride common.Hash, effectiveGas uint64, l1Block uint64, senderOverride *common.Address) (Transaction, error) {
	if origTx.Type() != LegacyTxType {
		return nil, errors.New("attempt to arbitrum-wrap non-legacy transaction")
	}
	inner := ArbitrumLegacyTxData{
		LegacyTx:          origTx.(*LegacyTx),
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

func (tx *ArbitrumLegacyTxData) Unwrap() Transaction {
	return tx
}

func (tx *ArbitrumLegacyTxData) Hash() common.Hash {
	if tx.HashOverride != (common.Hash{}) {
		return tx.HashOverride
	}
	return tx.LegacyTx.Hash()
}

func (tx *ArbitrumLegacyTxData) EncodeRLP(w io.Writer) error {
	if _, err := w.Write([]byte{ArbitrumLegacyTxType}); err != nil {
		return err
	}

	legacy := bytes.NewBuffer(nil)
	if err := tx.LegacyTx.EncodeRLP(legacy); err != nil {
		return err
	}
	legacyBytes := legacy.Bytes()

	payloadSize := rlp.StringLen(legacyBytes)                        // embedded LegacyTx RLP
	payloadSize += 1 + 32                                            // HashOverride (1 byte length + 32 bytes hash)
	payloadSize += 1 + rlp.IntLenExcludingHead(tx.EffectiveGasPrice) // EffectiveGasPrice
	payloadSize += 1 + rlp.IntLenExcludingHead(tx.L1BlockNumber)     // L1BlockNumber

	if tx.OverrideSender == nil {
		payloadSize += 1 // empty OverrideSender
	} else {
		payloadSize += 1 + 20 // OverrideSender (1 byte length + 20 bytes address)
	}

	b := make([]byte, 10)
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeString(legacyBytes, w, b); err != nil {
		return err
	}

	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(tx.HashOverride[:]); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.EffectiveGasPrice, w, b); err != nil {
		return err
	}

	if err := rlp.EncodeInt(tx.L1BlockNumber, w, b); err != nil {
		return err
	}

	if tx.OverrideSender == nil {
		b[0] = 0x80
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + 20
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(tx.OverrideSender[:]); err != nil {
			return err
		}
	}

	return nil
}

func (tx *ArbitrumLegacyTxData) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}

	legacyBytes, err := s.Bytes()
	if err != nil {
		return err
	}

	legacyTx := &LegacyTx{}
	str := rlp.NewStream(bytes.NewReader(legacyBytes), uint64(len(legacyBytes)))
	if err := legacyTx.DecodeRLP(str); err != nil {
		return err
	}
	tx.LegacyTx = legacyTx

	var hash common.Hash
	if err := s.Decode(&hash); err != nil {
		return err
	}
	tx.HashOverride = hash

	var effectiveGasPrice uint64
	if err := s.Decode(&effectiveGasPrice); err != nil {
		return err
	}
	tx.EffectiveGasPrice = effectiveGasPrice

	var l1BlockNumber uint64
	if err := s.Decode(&l1BlockNumber); err != nil {
		return err
	}
	tx.L1BlockNumber = l1BlockNumber

	var sender common.Address
	if err := s.Decode(&sender); err != nil {
		if err.Error() == "rlp: input string too short for common.Address" {
			tx.OverrideSender = nil
		} else {
			return err
		}
	} else if sender != (common.Address{}) {
		tx.OverrideSender = &sender
	}

	return s.ListEnd()
}

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
		if err := SanityCheckSignature(&tx.V, &tx.R, &tx.S, true); err != nil {
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
