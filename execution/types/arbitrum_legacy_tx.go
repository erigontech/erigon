package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"

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

func (tx *ArbitrumLegacyTxData) payloadSize() (payloadSize int, nonceLen, gasLen, gasPriceLen int) {
	// Nonce (uint64)
	payloadSize++
	nonceLen = rlp.IntLenExcludingHead(tx.Nonce)
	payloadSize += nonceLen

	// GasPrice (*uint256.Int)
	payloadSize++
	gasPriceLen = rlp.Uint256LenExcludingHead(tx.GasPrice)
	payloadSize += gasPriceLen

	// Gas (uint64)
	payloadSize++
	gasLen = rlp.IntLenExcludingHead(tx.GasLimit)
	payloadSize += gasLen

	// To (*common.Address, 20 bytes if non-nil)
	payloadSize++
	if tx.To != nil {
		payloadSize += 20
	}

	// Value (*uint256.Int)
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.Value)

	// Data ([]byte)
	payloadSize += rlp.StringLen(tx.Data)

	// V, R, S (*uint256.Int each)
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.V)
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.R)
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.S)

	// HashOverride (common.Hash, 32 bytes)
	payloadSize++
	payloadSize += 32

	// EffectiveGasPrice (uint64)
	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.EffectiveGasPrice)

	// L1BlockNumber (uint64)
	payloadSize++
	payloadSize += rlp.IntLenExcludingHead(tx.L1BlockNumber)

	// OverrideSender (*common.Address, 20 bytes if non-nil)
	payloadSize++
	if tx.OverrideSender != nil {
		payloadSize += 20
	}

	return payloadSize, nonceLen, gasLen, gasPriceLen
}

func (tx *ArbitrumLegacyTxData) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, gasPriceLen int) error {
	// Write the RLP list prefix
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}

	// Encode Nonce (uint64)
	if err := rlp.EncodeInt(tx.Nonce, w, b); err != nil {
		return err
	}

	// Encode GasPrice (*uint256.Int)
	if err := rlp.EncodeUint256(tx.GasPrice, w, b); err != nil {
		return err
	}

	// Encode Gas (uint64)
	if err := rlp.EncodeInt(tx.GasLimit, w, b); err != nil {
		return err
	}

	// Encode To (*common.Address, 20 bytes if non-nil)
	if tx.To == nil {
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + 20
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write((*tx.To)[:]); err != nil {
			return err
		}
	}

	// Encode Value (*uint256.Int)
	if err := rlp.EncodeUint256(tx.Value, w, b); err != nil {
		return err
	}

	// Encode Data ([]byte)
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}

	// Encode V (*uint256.Int)
	if err := rlp.EncodeUint256(&tx.V, w, b); err != nil {
		return err
	}

	// Encode R (*uint256.Int)
	if err := rlp.EncodeUint256(&tx.R, w, b); err != nil {
		return err
	}

	// Encode S (*uint256.Int)
	if err := rlp.EncodeUint256(&tx.S, w, b); err != nil {
		return err
	}

	// Encode HashOverride (common.Hash, 32 bytes)
	b[0] = 128 + 32
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(tx.HashOverride[:]); err != nil {
		return err
	}

	// Encode EffectiveGasPrice (uint64)
	if err := rlp.EncodeInt(tx.EffectiveGasPrice, w, b); err != nil {
		return err
	}

	// Encode L1BlockNumber (uint64)
	if err := rlp.EncodeInt(tx.L1BlockNumber, w, b); err != nil {
		return err
	}

	// Encode OverrideSender (*common.Address, 20 bytes if non-nil)
	if tx.OverrideSender == nil {
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		b[0] = 128 + 20
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write((*tx.OverrideSender)[:]); err != nil {
			return err
		}
	}

	return nil
}

func (tx *ArbitrumLegacyTxData) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, gasPriceLen := tx.payloadSize()
	
	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}

	// encode TxType
	b[0] = ArbitrumLegacyTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, gasPriceLen); err != nil {
		return err
	}
	
	return nil
}

func (tx *ArbitrumLegacyTxData) DecodeRLP(s *rlp.Stream) error {
	// Begin decoding the RLP list.
	if _, err := s.List(); err != nil {
		return err
	}

	var b []byte
	var err error

	// Decode Nonce (uint64)
	if tx.Nonce, err = s.Uint(); err != nil {
		return fmt.Errorf("read Nonce: %w", err)
	}

	// Decode GasPrice (*uint256.Int)
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read GasPrice: %w", err)
	}
	tx.GasPrice = new(uint256.Int).SetBytes(b)

	// Decode GasLimit (uint64)
	if tx.GasLimit, err = s.Uint(); err != nil {
		return fmt.Errorf("read GasLimit: %w", err)
	}

	// Decode To (*common.Address, 20 bytes if non-nil)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read To: %w", err)
	}
	if len(b) > 0 {
		if len(b) != 20 {
			return fmt.Errorf("wrong size for To: %d", len(b))
		}
		tx.To = new(common.Address)
		copy(tx.To[:], b)
	} else {
		tx.To = nil
	}

	// Decode Value (*uint256.Int)
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	tx.Value = new(uint256.Int).SetBytes(b)

	// Decode Data ([]byte)
	if tx.Data, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Data: %w", err)
	}

	// Decode V (*uint256.Int)
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read V: %w", err)
	}
	tx.V.SetBytes(b)

	// Decode R (*uint256.Int)
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read R: %w", err)
	}
	tx.R.SetBytes(b)

	// Decode S (*uint256.Int)
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read S: %w", err)
	}
	tx.S.SetBytes(b)

	// Decode HashOverride (common.Hash, 32 bytes)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read HashOverride: %w", err)
	}
	if len(b) != 32 {
		return fmt.Errorf("wrong size for HashOverride: %d", len(b))
	}
	copy(tx.HashOverride[:], b)

	// Decode EffectiveGasPrice (uint64)
	if tx.EffectiveGasPrice, err = s.Uint(); err != nil {
		return fmt.Errorf("read EffectiveGasPrice: %w", err)
	}

	// Decode L1BlockNumber (uint64)
	if tx.L1BlockNumber, err = s.Uint(); err != nil {
		return fmt.Errorf("read L1BlockNumber: %w", err)
	}

	// Decode OverrideSender (*common.Address, 20 bytes if non-nil)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read OverrideSender: %w", err)
	}
	if len(b) > 0 {
		if len(b) != 20 {
			return fmt.Errorf("wrong size for OverrideSender: %d", len(b))
		}
		tx.OverrideSender = new(common.Address)
		copy(tx.OverrideSender[:], b)
	} else {
		tx.OverrideSender = nil
	}

	// End the RLP list.
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close ArbitrumLegacyTxData: %w", err)
	}

	return nil
}
