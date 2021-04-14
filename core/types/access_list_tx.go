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
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/bits"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// go:generate gencodec -type AccessTuple -out gen_access_tuple.go

// AccessList is an EIP-2930 access list.
type AccessList []AccessTuple

// AccessTuple is the element type of an access list.
type AccessTuple struct {
	Address     common.Address `json:"address"        gencodec:"required"`
	StorageKeys []common.Hash  `json:"storageKeys"    gencodec:"required"`
}

// StorageKeys returns the total number of storage keys in the access list.
func (al AccessList) StorageKeys() int {
	sum := 0
	for _, tuple := range al {
		sum += len(tuple.StorageKeys)
	}
	return sum
}

// AccessListTx is the data of EIP-2930 access list transactions.
type AccessListTx struct {
	LegacyTx
	ChainID    *uint256.Int
	AccessList AccessList // EIP-2930 access list
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx AccessListTx) copy() *AccessListTx {
	cpy := &AccessListTx{
		LegacyTx: LegacyTx{
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
			GasPrice: new(uint256.Int),
		},
		ChainID:    new(uint256.Int),
		AccessList: make(AccessList, len(tx.AccessList)),
	}
	copy(cpy.AccessList, tx.AccessList)
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	if tx.ChainID != nil {
		cpy.ChainID.Set(tx.ChainID)
	}
	if tx.GasPrice != nil {
		cpy.GasPrice.Set(tx.GasPrice)
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

func (tx AccessListTx) GetAccessList() AccessList {
	return tx.AccessList
}

func (tx *AccessListTx) Size() common.StorageSize {
	if size := tx.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := tx.EncodingSize()
	tx.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

func (tx AccessListTx) Protected() bool {
	return true
}

func (tx AccessListTx) EncodingSize() int {
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
	// size of GasPrice
	encodingSize++
	var gasPriceLen int
	if tx.GasPrice.BitLen() >= 8 {
		gasPriceLen = (tx.GasPrice.BitLen() + 7) / 8
	}
	encodingSize += gasPriceLen
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

func accessListSize(al AccessList) int {
	var accessListLen int
	for _, tuple := range al {
		tupleLen := 21 // For the address
		// size of StorageKeys
		tupleLen++
		// Each storage key takes 33 bytes
		storageLen := 33 * len(tuple.StorageKeys)
		if storageLen >= 56 {
			tupleLen += (bits.Len(uint(storageLen)) + 7) / 8 // BE encoding of the length of the storage keys
		}
		tupleLen += storageLen
		accessListLen++
		if tupleLen >= 56 {
			accessListLen += (bits.Len(uint(tupleLen)) + 7) / 8 // BE encoding of the length of the storage keys
		}
		accessListLen += tupleLen
	}
	return accessListLen
}

func encodeAccessList(al AccessList, w io.Writer, b []byte) error {
	for _, tuple := range al {
		tupleLen := 21
		tupleLen++
		// Each storage key takes 33 bytes
		storageLen := 33 * len(tuple.StorageKeys)
		if storageLen >= 56 {
			tupleLen += (bits.Len(uint(storageLen)) + 7) / 8 // BE encoding of the length of the storage keys
		}
		tupleLen += storageLen
		if err := EncodeStructSizePrefix(tupleLen, w, b); err != nil {
			return err
		}
		b[0] = 128 + 20
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
		if _, err := w.Write(tuple.Address.Bytes()); err != nil {
			return err
		}
		if err := EncodeStructSizePrefix(storageLen, w, b); err != nil {
			return err
		}
		b[0] = 128 + 32
		for _, storageKey := range tuple.StorageKeys {
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
			if _, err := w.Write(storageKey.Bytes()); err != nil {
				return err
			}
		}
	}
	return nil
}

func EncodeStructSizePrefix(size int, w io.Writer, b []byte) error {
	if size >= 56 {
		beSize := (bits.Len(uint(size)) + 7) / 8
		binary.BigEndian.PutUint64(b[1:], uint64(size))
		b[8-beSize] = byte(beSize) + 247
		if _, err := w.Write(b[8-beSize : 9]); err != nil {
			return err
		}
	} else {
		b[0] = byte(size) + 192
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	}
	return nil
}

func (tx AccessListTx) EncodeRLP(w io.Writer) error {
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
	// size of GasPrice
	encodingSize++
	var gasPriceLen int
	if tx.GasPrice.BitLen() >= 8 {
		gasPriceLen = (tx.GasPrice.BitLen() + 7) / 8
	}
	encodingSize += gasPriceLen
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
	envelopeSize := encodingSize + 1
	if encodingSize >= 56 {
		envelopeSize += (bits.Len(uint(encodingSize)) + 7) / 8
	}
	// size of TxType
	envelopeSize++
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
	// encode GasPrice
	if err := tx.GasPrice.EncodeRLP(w); err != nil {
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

func decodeAccessList(al *AccessList, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open accessList: %w", err)
	}
	var b []byte
	i := 0
	for _, err = s.List(); err == nil; _, err = s.List() {
		// decode tuple
		*al = append(*al, AccessTuple{})
		tuple := &(*al)[len(*al)-1]
		if b, err = s.Bytes(); err != nil {
			return fmt.Errorf("read Address: %w", err)
		}
		if len(b) != 20 {
			return fmt.Errorf("wrong size for AccessTuple address: %d", len(b))
		}
		copy(tuple.Address[:], b)
		if _, err = s.List(); err != nil {
			return fmt.Errorf("open StorageKeys: %w", err)
		}
		for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
			tuple.StorageKeys = append(tuple.StorageKeys, common.Hash{})
			if len(b) != 32 {
				return fmt.Errorf("wrong size for StorageKey: %d", len(b))
			}
			copy(tuple.StorageKeys[len(tuple.StorageKeys)-1][:], b)
		}
		if !errors.Is(err, rlp.EOL) {
			return fmt.Errorf("read StorageKey: %w", err)
		}
		// end of StorageKeys list
		if err = s.ListEnd(); err != nil {
			return fmt.Errorf("close StorageKeys: %w", err)
		}
		// end of tuple
		if err = s.ListEnd(); err != nil {
			return fmt.Errorf("close AccessTuple: %w", err)
		}
		i++
	}
	if !errors.Is(err, rlp.EOL) {
		return fmt.Errorf("open accessTuple: %d %w", i, err)
	}
	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close accessList: %w", err)
	}
	return nil
}

func (tx *AccessListTx) DecodeRLP(s *rlp.Stream) error {
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
		return fmt.Errorf("wrong size for GasPrice: %d", len(b))
	}
	tx.GasPrice = new(uint256.Int).SetBytes(b)
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
		return fmt.Errorf("read AccessList: %w", err)
	}
	// decode V
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read V: %w", err)
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for V: %d", len(b))
	}
	tx.V = new(uint256.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read R: %w", err)
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for R: %d", len(b))
	}
	tx.R = new(uint256.Int).SetBytes(b)
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read S: %w", err)
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for S: %d", len(b))
	}
	tx.S = new(uint256.Int).SetBytes(b)
	return s.ListEnd()
}

// AsMessage returns the transaction as a core.Message.
func (tx AccessListTx) AsMessage(s Signer) (Message, error) {
	msg := Message{
		nonce:      tx.Nonce,
		gasLimit:   tx.Gas,
		gasPrice:   *tx.GasPrice,
		tip:        *tx.GasPrice,
		feeCap:     *tx.GasPrice,
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

func (tx *AccessListTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := tx.copy()
	var err error
	cpy.R, cpy.S, cpy.V, err = signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy.ChainID = signer.ChainID()
	return cpy, nil
}

// Hash computes the hash (but not for signatures!)
func (tx AccessListTx) Hash() common.Hash {
	return rlpHash([]interface{}{
		tx.ChainID,
		tx.Nonce,
		tx.GasPrice,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
		tx.AccessList,
		tx.V, tx.R, tx.S,
	})
}

func (tx AccessListTx) SigningHash(chainID *big.Int) common.Hash {
	return prefixedRlpHash(
		AccessListTxType,
		[]interface{}{
			chainID,
			tx.Nonce,
			tx.GasPrice,
			tx.Gas,
			tx.To,
			tx.Value,
			tx.Data,
			tx.AccessList,
		})
}

func (tx AccessListTx) Type() byte { return AccessListTxType }
