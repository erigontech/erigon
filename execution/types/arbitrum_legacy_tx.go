package types

import (
	"bytes"
	"errors"

	"github.com/erigontech/erigon-lib/common"
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

func (tx *ArbitrumLegacyTxData) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, tx)
}

func (tx *ArbitrumLegacyTxData) decode(input []byte) error {
	return rlp.DecodeBytes(input, tx)
}

func (tx *ArbitrumLegacyTxData) EncodeOnlyLegacyInto(w *bytes.Buffer) {
	rlp.Encode(w, tx.LegacyTx)
}

func (tx *ArbitrumLegacyTxData) EncodeRLP(w io.Writer) error {
	// For ArbitrumLegacyTxData, we use the same pattern as other transactions
	// but we only encode the complete structure when needed for storage/transmission.
	// The hash comes from HashOverride, not from this RLP encoding.
	return rlp.Encode(w, []interface{}{
		tx.Nonce,
		tx.GasPrice,
		tx.GasLimit,
		tx.To,
		tx.Value,
		tx.Data,
		tx.V,
		tx.R,
		tx.S,
		tx.HashOverride,
		tx.EffectiveGasPrice,
		tx.L1BlockNumber,
		tx.OverrideSender,
	})
}

func (tx *ArbitrumLegacyTxData) DecodeRLP(s *rlp.Stream) error {
	type arbitrumLegacyTx struct {
		Nonce             uint64
		GasPrice          *uint256.Int
		GasLimit          uint64
		To                *common.Address `rlp:"nil"`
		Value             *uint256.Int
		Data              []byte
		V, R, S           uint256.Int
		HashOverride      common.Hash
		EffectiveGasPrice uint64
		L1BlockNumber     uint64
		OverrideSender    *common.Address `rlp:"nil"`
	}
	
	var dec arbitrumLegacyTx
	if err := s.Decode(&dec); err != nil {
		return err
	}
	
	tx.Nonce = dec.Nonce
	tx.GasPrice = dec.GasPrice
	tx.GasLimit = dec.GasLimit
	tx.To = dec.To
	tx.Value = dec.Value
	tx.Data = dec.Data
	tx.V = dec.V
	tx.R = dec.R
	tx.S = dec.S
	tx.HashOverride = dec.HashOverride
	tx.EffectiveGasPrice = dec.EffectiveGasPrice
	tx.L1BlockNumber = dec.L1BlockNumber
	tx.OverrideSender = dec.OverrideSender
	
	return nil
}
