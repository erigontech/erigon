package types

import (
	"bytes"
	"errors"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
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
