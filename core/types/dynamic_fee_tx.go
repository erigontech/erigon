// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"math/bits"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type DynamicFeeTransaction struct {
	CommonTx
	ChainID    *uint256.Int
	Tip        *uint256.Int
	FeeCap     *uint256.Int
	AccessList AccessList
}

func (tx DynamicFeeTransaction) GetPrice() *uint256.Int {
	return tx.Tip
}

func (tx DynamicFeeTransaction) Cost() *uint256.Int {
	total := new(uint256.Int).SetUint64(tx.Gas)
	total.Mul(total, tx.Tip)
	total.Add(total, tx.Value)
	return total
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx DynamicFeeTransaction) copy() *DynamicFeeTransaction {
	cpy := &DynamicFeeTransaction{
		CommonTx: CommonTx{
			TransactionMisc: TransactionMisc{
				time: tx.time,
			},
			Nonce: tx.Nonce,
			To:    tx.To, // TODO: copy pointed-to address
			Data:  common.CopyBytes(tx.Data),
			Gas:   tx.Gas,
			// These are copied below.
			Value: new(uint256.Int),
			V:     new(uint256.Int),
			R:     new(uint256.Int),
			S:     new(uint256.Int),
		},
		AccessList: make(AccessList, len(tx.AccessList)),
		ChainID:    new(uint256.Int),
		Tip:        new(uint256.Int),
		FeeCap:     new(uint256.Int),
	}
	copy(cpy.AccessList, tx.AccessList)
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	if tx.ChainID != nil {
		cpy.ChainID.Set(tx.ChainID)
	}
	if tx.Tip != nil {
		cpy.Tip.Set(tx.Tip)
	}
	if tx.FeeCap != nil {
		cpy.FeeCap.Set(tx.FeeCap)
	}
	if tx.V != nil {
		cpy.V.Set(tx.V)
	}
	if tx.R != nil {
		cpy.R.Set(tx.R)
	}
	if tx.S != nil {
		cpy.S.Set(tx.S)
	}
	return cpy
}

func (tx DynamicFeeTransaction) GetAccessList() AccessList {
	return tx.AccessList
}

func (tx *DynamicFeeTransaction) Size() common.StorageSize {
	if size := tx.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := tx.EncodingSize()
	tx.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

func (tx DynamicFeeTransaction) Protected() bool {
	return true
}

func (tx DynamicFeeTransaction) EncodingSize() int {
	encodingSize := 0
	// size of ChainID
	encodingSize++
	var chainIdLen int
	if tx.ChainID.BitLen() >= 8 {
		chainIdLen = (tx.ChainID.BitLen() + 7) / 8
	}
	encodingSize += chainIdLen
	// size of Nonce
	encodingSize++
	var nonceLen int
	if tx.Nonce >= 128 {
		nonceLen = (bits.Len64(tx.Nonce) + 7) / 8
	}
	encodingSize += nonceLen
	// size of Tip
	encodingSize++
	var tipLen int
	if tx.Tip.BitLen() >= 8 {
		tipLen = (tx.Tip.BitLen() + 7) / 8
	}
	encodingSize += tipLen
	// size of FeeCap
	encodingSize++
	var feeCapLen int
	if tx.FeeCap.BitLen() >= 8 {
		feeCapLen = (tx.FeeCap.BitLen() + 7) / 8
	}
	encodingSize += feeCapLen
	// size of Gas
	encodingSize++
	var gasLen int
	if tx.Gas >= 128 {
		gasLen = (bits.Len64(tx.Gas) + 7) / 8
	}
	encodingSize += gasLen
	// size of To
	encodingSize++
	if tx.To != nil {
		encodingSize += 20
	}
	// size of Value
	encodingSize++
	var valueLen int
	if tx.Value.BitLen() >= 8 {
		valueLen = (tx.Value.BitLen() + 7) / 8
	}
	encodingSize += valueLen
	// size of Data
	encodingSize++
	switch len(tx.Data) {
	case 0:
	case 1:
		if tx.Data[0] >= 128 {
			encodingSize++
		}
	default:
		if len(tx.Data) >= 56 {
			encodingSize += (bits.Len(uint(len(tx.Data))) + 7) / 8
		}
		encodingSize += len(tx.Data)
	}
	// size of AccessList
	encodingSize++
	accessListLen := accessListSize(tx.AccessList)
	if accessListLen >= 56 {
		encodingSize += (bits.Len(uint(accessListLen)) + 7) / 8
	}
	encodingSize += accessListLen
	// size of V
	encodingSize++
	var vLen int
	if tx.V.BitLen() >= 8 {
		vLen = (tx.V.BitLen() + 7) / 8
	}
	encodingSize += vLen
	// size of R
	encodingSize++
	var rLen int
	if tx.R.BitLen() >= 8 {
		rLen = (tx.R.BitLen() + 7) / 8
	}
	encodingSize += rLen
	// size of S
	encodingSize++
	var sLen int
	if tx.S.BitLen() >= 8 {
		sLen = (tx.S.BitLen() + 7) / 8
	}
	encodingSize += sLen
	// Add envelope size and type size
	if encodingSize >= 56 {
		encodingSize += (bits.Len(uint(encodingSize)) + 7) / 8
	}
	encodingSize += 2
	return encodingSize
}

func (tx *DynamicFeeTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := tx.copy()
	var err error
	cpy.R, cpy.S, cpy.V, err = signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy.ChainID = signer.ChainID()
	return cpy, nil
}

func (tx DynamicFeeTransaction) EncodeRLP(w io.Writer) error {
	encodingSize := 0
	// size of ChainID
	encodingSize++
	var chainIdLen int
	if tx.ChainID.BitLen() >= 8 {
		chainIdLen = (tx.ChainID.BitLen() + 7) / 8
	}
	encodingSize += chainIdLen
	// size of Nonce
	encodingSize++
	var nonceLen int
	if tx.Nonce >= 128 {
		nonceLen = (bits.Len64(tx.Nonce) + 7) / 8
	}
	encodingSize += nonceLen
	// size of Tip
	encodingSize++
	var tipLen int
	if tx.Tip.BitLen() >= 8 {
		tipLen = (tx.Tip.BitLen() + 7) / 8
	}
	encodingSize += tipLen
	// size of FeeCap
	encodingSize++
	var feeCapLen int
	if tx.FeeCap.BitLen() >= 8 {
		feeCapLen = (tx.FeeCap.BitLen() + 7) / 8
	}
	encodingSize += feeCapLen
	// size of Gas
	encodingSize++
	var gasLen int
	if tx.Gas >= 128 {
		gasLen = (bits.Len64(tx.Gas) + 7) / 8
	}
	encodingSize += gasLen
	// size of To
	encodingSize++
	if tx.To != nil {
		encodingSize += 20
	}
	// size of Value
	encodingSize++
	var valueLen int
	if tx.Value.BitLen() >= 8 {
		valueLen = (tx.Value.BitLen() + 7) / 8
	}
	encodingSize += valueLen
	// size of Data
	encodingSize++
	switch len(tx.Data) {
	case 0:
	case 1:
		if tx.Data[0] >= 128 {
			encodingSize++
		}
	default:
		if len(tx.Data) >= 56 {
			encodingSize += (bits.Len(uint(len(tx.Data))) + 7) / 8
		}
		encodingSize += len(tx.Data)
	}
	// size of AccessList
	encodingSize++
	var accessListLen int = accessListSize(tx.AccessList)
	if accessListLen >= 56 {
		encodingSize += (bits.Len(uint(accessListLen)) + 7) / 8
	}
	encodingSize += accessListLen
	// size of V
	encodingSize++
	var vLen int
	if tx.V.BitLen() >= 8 {
		vLen = (tx.V.BitLen() + 7) / 8
	}
	encodingSize += vLen
	// size of R
	encodingSize++
	var rLen int
	if tx.R.BitLen() >= 8 {
		rLen = (tx.R.BitLen() + 7) / 8
	}
	encodingSize += rLen
	// size of S
	encodingSize++
	var sLen int
	if tx.S.BitLen() >= 8 {
		sLen = (tx.S.BitLen() + 7) / 8
	}
	encodingSize += sLen
	envelopeSize := encodingSize
	if encodingSize >= 56 {
		envelopeSize += (bits.Len(uint(encodingSize)) + 7) / 8
	}
	// size of struct prefix and TxType
	envelopeSize += 2
	var b [33]byte
	// envelope
	if err := EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = AccessListTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(encodingSize, w, b[:]); err != nil {
		return err
	}
	// encode ChainID
	if err := tx.ChainID.EncodeRLP(w); err != nil {
		return err
	}
	// encode Nonce
	if tx.Nonce > 0 && tx.Nonce < 128 {
		b[0] = byte(tx.Nonce)
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		binary.BigEndian.PutUint64(b[1:], tx.Nonce)
		b[8-nonceLen] = 128 + byte(nonceLen)
		if _, err := w.Write(b[8-nonceLen : 9]); err != nil {
			return err
		}
	}
	// encode Tip
	if err := tx.Tip.EncodeRLP(w); err != nil {
		return err
	}
	// encode FeeCap
	if err := tx.FeeCap.EncodeRLP(w); err != nil {
		return err
	}
	// encode Gas
	if tx.Gas > 0 && tx.Gas < 128 {
		b[0] = byte(tx.Gas)
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		binary.BigEndian.PutUint64(b[1:], tx.Gas)
		b[8-gasLen] = 128 + byte(gasLen)
		if _, err := w.Write(b[8-gasLen : 9]); err != nil {
			return err
		}
	}
	// encode To
	if tx.To == nil {
		b[0] = 128
	} else {
		b[0] = 128 + 20
	}
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if tx.To != nil {
		if _, err := w.Write(tx.To.Bytes()); err != nil {
			return err
		}
	}
	// encode Value
	if err := tx.Value.EncodeRLP(w); err != nil {
		return err
	}
	// encode Data
	if err := EncodeString(tx.Data, w, b[:]); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(accessListLen, w, b[:]); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(tx.AccessList, w, b[:]); err != nil {
		return err
	}
	// encode V
	if err := tx.V.EncodeRLP(w); err != nil {
		return err
	}
	// encode R
	if err := tx.R.EncodeRLP(w); err != nil {
		return err
	}
	// encode S
	if err := tx.S.EncodeRLP(w); err != nil {
		return err
	}
	return nil
}

func (tx *DynamicFeeTransaction) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for ChainID: %d", len(b))
	}
	tx.ChainID = new(uint256.Int).SetBytes(b)
	if tx.Nonce, err = s.Uint(); err != nil {
		return err
	}
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for Tip: %d", len(b))
	}
	tx.Tip = new(uint256.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for FeeCap: %d", len(b))
	}
	tx.FeeCap = new(uint256.Int).SetBytes(b)
	if tx.Gas, err = s.Uint(); err != nil {
		return err
	}
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 0 && len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	if len(b) > 0 {
		tx.To = &common.Address{}
		copy((*tx.To)[:], b)
	}
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for Value: %d", len(b))
	}
	tx.Value = new(uint256.Int).SetBytes(b)
	if tx.Data, err = s.Bytes(); err != nil {
		return err
	}
	// decode AccessList
	tx.AccessList = AccessList{}
	if err = decodeAccessList(&tx.AccessList, s); err != nil {
		return err
	}
	// decode V
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for V: %d", len(b))
	}
	tx.V = new(uint256.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for R: %d", len(b))
	}
	tx.R = new(uint256.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for S: %d", len(b))
	}
	tx.S = new(uint256.Int).SetBytes(b)
	return s.ListEnd()

}

// AsMessage returns the transaction as a core.Message.
func (tx DynamicFeeTransaction) AsMessage(s Signer) (Message, error) {
	msg := Message{
		nonce:      tx.Nonce,
		gasLimit:   tx.Gas,
		gasPrice:   *tx.Tip,
		tip:        *tx.Tip,
		feeCap:     *tx.FeeCap,
		to:         tx.To,
		amount:     *tx.Value,
		data:       tx.Data,
		accessList: tx.AccessList,
		checkNonce: true,
	}

	var err error
	msg.from, err = Sender(s, &tx)
	return msg, err
}

// Hash computes the hash (but not for signatures!)
func (tx DynamicFeeTransaction) Hash() common.Hash {
	return rlpHash([]interface{}{
		tx.ChainID,
		tx.Nonce,
		tx.Tip,
		tx.FeeCap,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
		tx.AccessList,
		tx.V, tx.R, tx.S,
	})
}

func (tx DynamicFeeTransaction) SigningHash(chainID *big.Int) common.Hash {
	return prefixedRlpHash(
		DynamicFeeTxType,
		[]interface{}{
			chainID,
			tx.Nonce,
			tx.Tip,
			tx.FeeCap,
			tx.Gas,
			tx.To,
			tx.Value,
			tx.Data,
			tx.AccessList,
		})
}

// accessors for innerTx.
func (tx DynamicFeeTransaction) Type() byte { return DynamicFeeTxType }

func (tx DynamicFeeTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return tx.V, tx.R, tx.S
}
