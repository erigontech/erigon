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
	"github.com/ledgerwatch/turbo-geth/common/u256"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type CommonTx struct {
	TransactionMisc
	Nonce   uint64          // nonce of sender account
	Gas     uint64          // gas limit
	To      *common.Address `rlp:"nil"` // nil means contract creation
	Value   *uint256.Int    // wei amount
	Data    []byte          // contract invocation input data
	V, R, S *uint256.Int    // signature values
}

func (ct CommonTx) GetNonce() uint64 {
	return ct.Nonce
}

func (ct CommonTx) GetTo() *common.Address {
	return ct.To
}

func (ct CommonTx) GetGas() uint64 {
	return ct.Gas
}

func (ct CommonTx) GetValue() *uint256.Int {
	return ct.Value
}

func (ct CommonTx) GetData() []byte {
	return ct.Data
}

// LegacyTx is the transaction data of regular Ethereum transactions.
type LegacyTx struct {
	CommonTx
	GasPrice *uint256.Int // wei per gas
}

func (tx LegacyTx) GetPrice() *uint256.Int {
	return tx.GasPrice
}

func (tx LegacyTx) Cost() *uint256.Int {
	total := new(uint256.Int).SetUint64(tx.Gas)
	total.Mul(total, tx.GasPrice)
	total.Add(total, tx.Value)
	return total
}

func (tx LegacyTx) GetAccessList() AccessList {
	return AccessList{}
}

func (tx LegacyTx) Protected() bool {
	return tx.V != nil && isProtectedV(tx.V)
}

// NewTransaction creates an unsigned legacy transaction.
// Deprecated: use NewTx instead.
func NewTransaction(nonce uint64, to common.Address, amount *uint256.Int, gasLimit uint64, gasPrice *uint256.Int, data []byte) *LegacyTx {
	return &LegacyTx{
		CommonTx: CommonTx{
			Nonce: nonce,
			To:    &to,
			Value: amount,
			Gas:   gasLimit,
			Data:  data,
		},
		GasPrice: gasPrice,
	}
}

// NewContractCreation creates an unsigned legacy transaction.
// Deprecated: use NewTx instead.
func NewContractCreation(nonce uint64, amount *uint256.Int, gasLimit uint64, gasPrice *uint256.Int, data []byte) *LegacyTx {
	return &LegacyTx{
		CommonTx: CommonTx{
			Nonce: nonce,
			Value: amount,
			Gas:   gasLimit,
			Data:  data,
		},
		GasPrice: gasPrice,
	}
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx LegacyTx) copy() *LegacyTx {
	cpy := &LegacyTx{
		CommonTx: CommonTx{
			TransactionMisc: TransactionMisc{
				time: tx.time,
			},
			Nonce: tx.Nonce,
			To:    tx.To, // TODO: copy pointed-to address
			Data:  common.CopyBytes(tx.Data),
			Gas:   tx.Gas,
			// These are initialized below.
			Value: new(uint256.Int),
			V:     new(uint256.Int),
			R:     new(uint256.Int),
			S:     new(uint256.Int),
		},
		GasPrice: new(uint256.Int),
	}
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
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

func (tx *LegacyTx) Size() common.StorageSize {
	if size := tx.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := tx.EncodingSize()
	tx.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

func (tx LegacyTx) EncodingSize() int {
	payloadSize, _, _ := tx.payloadSize()
	return payloadSize
}

func (tx LegacyTx) payloadSize() (payloadSize int, nonceLen, gasLen int) {
	payloadSize++
	if tx.Nonce >= 128 {
		nonceLen = (bits.Len64(tx.Nonce) + 7) / 8
	}
	payloadSize += nonceLen
	payloadSize++
	var gasPriceLen int
	if tx.GasPrice.BitLen() >= 8 {
		gasPriceLen = (tx.GasPrice.BitLen() + 7) / 8
	}
	payloadSize += gasPriceLen
	payloadSize++
	if tx.Gas >= 128 {
		gasLen = (bits.Len64(tx.Gas) + 7) / 8
	}
	payloadSize += gasLen
	payloadSize++
	if tx.To != nil {
		payloadSize += 20
	}
	payloadSize++
	var valueLen int
	if tx.Value.BitLen() >= 8 {
		valueLen = (tx.Value.BitLen() + 7) / 8
	}
	payloadSize += valueLen
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
	// size of V
	payloadSize++
	var vLen int
	if tx.V != nil && tx.V.BitLen() >= 8 {
		vLen = (tx.V.BitLen() + 7) / 8
	}
	payloadSize += vLen
	payloadSize++
	var rLen int
	if tx.R != nil && tx.R.BitLen() >= 8 {
		rLen = (tx.R.BitLen() + 7) / 8
	}
	payloadSize += rLen
	payloadSize++
	var sLen int
	if tx.S != nil && tx.S.BitLen() >= 8 {
		sLen = (tx.S.BitLen() + 7) / 8
	}
	payloadSize += sLen
	return payloadSize, nonceLen, gasLen
}

func EncodeString(s []byte, w io.Writer, b []byte) error {
	switch len(s) {
	case 0:
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	case 1:
		if s[0] >= 128 {
			b[0] = 129
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
		}
		if _, err := w.Write(s); err != nil {
			return err
		}
	default:
		if err := EncodeStringSizePrefix(len(s), w, b); err != nil {
			return err
		}
		if _, err := w.Write(s); err != nil {
			return err
		}
	}
	return nil
}

func EncodeStringSizePrefix(size int, w io.Writer, b []byte) error {
	if size >= 56 {
		beSize := (bits.Len(uint(size)) + 7) / 8
		binary.BigEndian.PutUint64(b[1:], uint64(size))
		b[8-beSize] = byte(beSize) + 183
		if _, err := w.Write(b[8-beSize : 9]); err != nil {
			return err
		}
	} else {
		b[0] = byte(size) + 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	}
	return nil
}

func (tx LegacyTx) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen := tx.payloadSize()
	var b [33]byte
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen); err != nil {
		return err
	}
	return nil
}

func (tx LegacyTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b[:]); err != nil {
		return err
	}
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
	if err := tx.GasPrice.EncodeRLP(w); err != nil {
		return err
	}
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
	if err := tx.Value.EncodeRLP(w); err != nil {
		return err
	}
	if err := EncodeString(tx.Data, w, b[:]); err != nil {
		return err
	}
	if tx.V == nil {
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		if err := tx.V.EncodeRLP(w); err != nil {
			return err
		}
	}
	if tx.R == nil {
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		if err := tx.R.EncodeRLP(w); err != nil {
			return err
		}
	}
	if tx.S == nil {
		b[0] = 128
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	} else {
		if err := tx.S.EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil

}

func (tx LegacyTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen := tx.payloadSize()
	var b [33]byte
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen); err != nil {
		return err
	}
	return nil
}

// DecodeRLP decodes LegacyTx but with the list token already consumed and encodingSize being presented
func (tx *LegacyTx) DecodeRLP(s *rlp.Stream, encodingSize uint64) error {
	var err error
	s.NewList(uint64(encodingSize))
	if tx.Nonce, err = s.Uint(); err != nil {
		return fmt.Errorf("read Nonce: %w", err)
	}
	var b []byte
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read GasPrice: %w", err)
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for GasPrice: %d", len(b))
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
		tx.To = &common.Address{}
		copy((*tx.To)[:], b)
	}
	if b, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Value: %w", err)
	}
	if len(b) > 32 {
		return fmt.Errorf("wrong size for Value: %d", len(b))
	}
	tx.Value = new(uint256.Int).SetBytes(b)
	if tx.Data, err = s.Bytes(); err != nil {
		return fmt.Errorf("read Data: %w", err)
	}
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
	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close tx struct: %w", err)
	}
	return nil
}

// AsMessage returns the transaction as a core.Message.
func (tx LegacyTx) AsMessage(s Signer) (Message, error) {
	msg := Message{
		nonce:      tx.Nonce,
		gasLimit:   tx.Gas,
		gasPrice:   *tx.GasPrice,
		tip:        *tx.GasPrice,
		feeCap:     *tx.GasPrice,
		to:         tx.To,
		amount:     *tx.Value,
		data:       tx.Data,
		accessList: nil,
		checkNonce: true,
	}

	var err error
	msg.from, err = Sender(s, &tx)
	return msg, err
}

func (tx *LegacyTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := tx.copy()
	var err error
	cpy.R, cpy.S, cpy.V, err = signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	return cpy, nil
}

// Hash computes the hash (but not for signatures!)
func (tx LegacyTx) Hash() common.Hash {
	return rlpHash([]interface{}{
		tx.Nonce,
		tx.GasPrice,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
		tx.V, tx.R, tx.S,
	})
}

func (tx LegacyTx) SigningHash(chainID *big.Int) common.Hash {
	if chainID != nil && chainID.Sign() != 0 {
		return rlpHash([]interface{}{
			tx.Nonce,
			tx.GasPrice,
			tx.Gas,
			tx.To,
			tx.Value,
			tx.Data,
			chainID, uint(0), uint(0),
		})
	}
	return rlpHash([]interface{}{
		tx.Nonce,
		tx.GasPrice,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
	})
}

func (tx LegacyTx) Type() byte { return LegacyTxType }

func (tx LegacyTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return tx.V, tx.R, tx.S
}

func (tx LegacyTx) GetChainID() *uint256.Int {
	return u256.Num0
}
