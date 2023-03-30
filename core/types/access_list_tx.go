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
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/rlp"
)

// AccessListTx is the data of EIP-2930 access list transactions.
type AccessListTx struct {
	LegacyTx
	ChainID    *uint256.Int
	AccessList types2.AccessList // EIP-2930 access list
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
			},
			GasPrice: new(uint256.Int),
		},
		ChainID:    new(uint256.Int),
		AccessList: make(types2.AccessList, len(tx.AccessList)),
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
	cpy.V.Set(&tx.V)
	cpy.R.Set(&tx.R)
	cpy.S.Set(&tx.S)
	return cpy
}

func (tx AccessListTx) GetAccessList() types2.AccessList {
	return tx.AccessList
}

func (tx AccessListTx) Protected() bool {
	return true
}

// EncodingSize returns the RLP encoding size of the whole transaction envelope
func (tx AccessListTx) EncodingSize() int {
	payloadSize, _, _, _ := tx.payloadSize()
	envelopeSize := payloadSize
	// Add envelope size and type size
	if payloadSize >= 56 {
		envelopeSize += (bits.Len(uint(payloadSize)) + 7) / 8
	}
	envelopeSize += 2
	return envelopeSize
}

// payloadSize calculates the RLP encoding size of transaction, without TxType and envelope
func (tx AccessListTx) payloadSize() (payloadSize int, nonceLen, gasLen, accessListLen int) {
	// size of ChainID
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.ChainID)
	// size of Nonce
	payloadSize++
	nonceLen = rlp.IntLenExcludingHead(tx.Nonce)
	payloadSize += nonceLen
	// size of GasPrice
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.GasPrice)
	// size of Gas
	payloadSize++
	gasLen = rlp.IntLenExcludingHead(tx.Gas)
	payloadSize += gasLen
	// size of To
	payloadSize++
	if tx.To != nil {
		payloadSize += 20
	}
	// size of Value
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.Value)
	// size of Data
	payloadSize++
	switch len(tx.Data) {
	case 0:
	case 1:
		if tx.Data[0] >= 128 {
			payloadSize++
		}
	default:
		if len(tx.Data) >= 56 {
			payloadSize += (bits.Len(uint(len(tx.Data))) + 7) / 8
		}
		payloadSize += len(tx.Data)
	}
	// size of AccessList
	payloadSize++
	accessListLen = accessListSize(tx.AccessList)
	if accessListLen >= 56 {
		payloadSize += (bits.Len(uint(accessListLen)) + 7) / 8
	}
	payloadSize += accessListLen
	// size of V
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.V)
	// size of R
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.R)
	// size of S
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.S)
	return payloadSize, nonceLen, gasLen, accessListLen
}

func accessListSize(al types2.AccessList) int {
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

func encodeAccessList(al types2.AccessList, w io.Writer, b []byte) error {
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

// MarshalBinary returns the canonical encoding of the transaction.
// For legacy transactions, it returns the RLP encoding. For EIP-2718 typed
// transactions, it returns the type and payload.
func (tx AccessListTx) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen := tx.payloadSize()
	var b [33]byte
	// encode TxType
	b[0] = AccessListTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen); err != nil {
		return err
	}
	return nil
}

func (tx AccessListTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, accessListLen int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := tx.ChainID.EncodeRLP(w); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeInt(tx.Nonce, w, b); err != nil {
		return err
	}
	// encode GasPrice
	if err := tx.GasPrice.EncodeRLP(w); err != nil {
		return err
	}
	// encode Gas
	if err := rlp.EncodeInt(tx.Gas, w, b); err != nil {
		return err
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
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(tx.AccessList, w, b); err != nil {
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

// EncodeRLP implements rlp.Encoder
func (tx AccessListTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen := tx.payloadSize()
	envelopeSize := payloadSize
	if payloadSize >= 56 {
		envelopeSize += (bits.Len(uint(payloadSize)) + 7) / 8
	}
	// size of struct prefix and TxType
	envelopeSize += 2
	var b [33]byte
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = AccessListTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen); err != nil {
		return err
	}
	return nil
}

func decodeAccessList(al *types2.AccessList, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open accessList: %w", err)
	}
	var b []byte
	i := 0
	for _, err = s.List(); err == nil; _, err = s.List() {
		// decode tuple
		*al = append(*al, types2.AccessTuple{StorageKeys: []libcommon.Hash{}})
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
			tuple.StorageKeys = append(tuple.StorageKeys, libcommon.Hash{})
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
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read ChainID: %w", err)
	}
	tx.ChainID = new(uint256.Int).SetBytes(b)
	if tx.Nonce, err = s.Uint(); err != nil {
		return fmt.Errorf("read Nonce: %w", err)
	}
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read GasPrice: %w", err)
	}
	tx.GasPrice = new(uint256.Int).SetBytes(b)
	if tx.Gas, err = s.Uint(); err != nil {
		return fmt.Errorf("read Gas: %w", err)
	}
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read To: %w", err)
	}
	if len(b) > 0 && len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	if len(b) > 0 {
		tx.To = &libcommon.Address{}
		copy((*tx.To)[:], b)
	}
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	tx.Value = new(uint256.Int).SetBytes(b)
	if tx.Data, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Data: %w", err)
	}
	// decode AccessList
	tx.AccessList = types2.AccessList{}
	if err = decodeAccessList(&tx.AccessList, s); err != nil {
		return fmt.Errorf("read AccessList: %w", err)
	}
	// decode V
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read V: %w", err)
	}
	tx.V.SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read R: %w", err)
	}
	tx.R.SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read S: %w", err)
	}
	tx.S.SetBytes(b)
	if err := s.ListEnd(); err != nil {
		return fmt.Errorf("close AccessListTx: %w", err)
	}
	return nil
}

// AsMessage returns the transaction as a core.Message.
func (tx AccessListTx) AsMessage(s Signer, _ *big.Int, rules *chain.Rules) (Message, error) {
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

	if !rules.IsBerlin {
		return msg, errors.New("eip-2930 transactions require Berlin")
	}

	var err error
	msg.from, err = tx.Sender(s)
	return msg, err
}

func (tx *AccessListTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := tx.copy()
	r, s, v, err := signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy.R.Set(r)
	cpy.S.Set(s)
	cpy.V.Set(v)
	cpy.ChainID = signer.ChainID()
	return cpy, nil
}
func (tx *AccessListTx) FakeSign(address libcommon.Address) (Transaction, error) {
	cpy := tx.copy()
	cpy.R.Set(u256.Num1)
	cpy.S.Set(u256.Num1)
	cpy.V.Set(u256.Num4)
	cpy.from.Store(address)
	return cpy, nil
}

// Hash computes the hash (but not for signatures!)
func (tx *AccessListTx) Hash() libcommon.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash.(*libcommon.Hash)
	}
	hash := prefixedRlpHash(AccessListTxType, []interface{}{
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
	tx.hash.Store(&hash)
	return hash
}

func (tx AccessListTx) SigningHash(chainID *big.Int) libcommon.Hash {
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

func (tx AccessListTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return &tx.V, &tx.R, &tx.S
}

func (tx AccessListTx) GetChainID() *uint256.Int {
	return tx.ChainID
}

func (tx *AccessListTx) Sender(signer Signer) (libcommon.Address, error) {
	if sc := tx.from.Load(); sc != nil {
		return sc.(libcommon.Address), nil
	}
	addr, err := signer.Sender(tx)
	if err != nil {
		return libcommon.Address{}, err
	}
	tx.from.Store(addr)
	return addr, nil
}
