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
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/rlp"
)

type CommonTx struct {
	TransactionMisc

	ChainID *uint256.Int
	Nonce   uint64             // nonce of sender account
	Gas     uint64             // gas limit
	To      *libcommon.Address `rlp:"nil"` // nil means contract creation
	Value   *uint256.Int       // wei amount
	Data    []byte             // contract invocation input data
	V, R, S uint256.Int        // signature values
}

func (ct CommonTx) GetChainID() *uint256.Int {
	return ct.ChainID
}

func (ct CommonTx) GetNonce() uint64 {
	return ct.Nonce
}

func (ct CommonTx) GetTo() *libcommon.Address {
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

func (ct CommonTx) GetSender() (libcommon.Address, bool) {
	if sc := ct.from.Load(); sc != nil {
		return sc.(libcommon.Address), true
	}
	return libcommon.Address{}, false
}

func (ct *CommonTx) SetSender(addr libcommon.Address) {
	ct.from.Store(addr)
}

func (ct CommonTx) Protected() bool {
	return true
}

func (ct CommonTx) IsContractDeploy() bool {
	return ct.GetTo() == nil
}

func (ct *CommonTx) GetDataHashes() []libcommon.Hash {
	// Only blob txs have data hashes
	return []libcommon.Hash{}
}

// LegacyTx is the transaction data of regular Ethereum transactions.
type LegacyTx struct {
	CommonTx
	GasPrice *uint256.Int // wei per gas
}

func (tx LegacyTx) GetPrice() *uint256.Int  { return tx.GasPrice }
func (tx LegacyTx) GetTip() *uint256.Int    { return tx.GasPrice }
func (tx LegacyTx) GetFeeCap() *uint256.Int { return tx.GasPrice }
func (tx LegacyTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetTip()
	}
	gasFeeCap := tx.GetFeeCap()
	// return 0 because effectiveFee cant be < 0
	if gasFeeCap.Lt(baseFee) {
		return uint256.NewInt(0)
	}
	effectiveFee := new(uint256.Int).Sub(gasFeeCap, baseFee)
	if tx.GetTip().Lt(effectiveFee) {
		return tx.GetTip()
	} else {
		return effectiveFee
	}
}

func (tx LegacyTx) Cost() *uint256.Int {
	total := new(uint256.Int).SetUint64(tx.Gas)
	total.Mul(total, tx.GasPrice)
	total.Add(total, tx.Value)
	return total
}

func (tx LegacyTx) GetAccessList() types2.AccessList {
	return types2.AccessList{}
}

func (tx LegacyTx) Protected() bool {
	return isProtectedV(&tx.V)
}

// NewTransaction creates an unsigned legacy transaction.
// Deprecated: use NewTx instead.
func NewTransaction(nonce uint64, to libcommon.Address, amount *uint256.Int, gasLimit uint64, gasPrice *uint256.Int, data []byte) *LegacyTx {
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
		},
		GasPrice: new(uint256.Int),
	}
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	if tx.GasPrice != nil {
		cpy.GasPrice.Set(tx.GasPrice)
	}
	cpy.V.Set(&tx.V)
	cpy.R.Set(&tx.R)
	cpy.S.Set(&tx.S)
	return cpy
}

func (tx LegacyTx) EncodingSize() int {
	payloadSize, _, _ := tx.payloadSize()
	return payloadSize
}

func (tx LegacyTx) payloadSize() (payloadSize int, nonceLen, gasLen int) {
	payloadSize++
	nonceLen = rlp.IntLenExcludingHead(tx.Nonce)
	payloadSize += nonceLen
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.GasPrice)
	payloadSize++
	gasLen = rlp.IntLenExcludingHead(tx.Gas)
	payloadSize += gasLen
	payloadSize++
	if tx.To != nil {
		payloadSize += 20
	}
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
	// size of V
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.V)
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.R)
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.S)
	return payloadSize, nonceLen, gasLen
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
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
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
	if err := rlp.EncodeInt(tx.Gas, w, b); err != nil {
		return err
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
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}
	if err := tx.V.EncodeRLP(w); err != nil {
		return err
	}
	if err := tx.R.EncodeRLP(w); err != nil {
		return err
	}
	if err := tx.S.EncodeRLP(w); err != nil {
		return err
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
	s.NewList(encodingSize)
	if tx.Nonce, err = s.Uint(); err != nil {
		return fmt.Errorf("read Nonce: %w", err)
	}
	var b []byte
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
	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close tx struct: %w", err)
	}
	return nil
}

// AsMessage returns the transaction as a core.Message.
func (tx LegacyTx) AsMessage(s Signer, _ *big.Int, _ *chain.Rules) (Message, error) {
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
	msg.from, err = tx.Sender(s)
	return msg, err
}

func (tx *LegacyTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := tx.copy()
	r, s, v, err := signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy.R.Set(r)
	cpy.S.Set(s)
	cpy.V.Set(v)
	return cpy, nil
}

func (tx *LegacyTx) FakeSign(address libcommon.Address) (Transaction, error) {
	cpy := tx.copy()
	cpy.R.Set(u256.Num1)
	cpy.S.Set(u256.Num1)
	cpy.V.Set(u256.Num4)
	cpy.from.Store(address)
	return cpy, nil
}

// Hash computes the hash (but not for signatures!)
func (tx *LegacyTx) Hash() libcommon.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash.(*libcommon.Hash)
	}
	hash := rlpHash([]interface{}{
		tx.Nonce,
		tx.GasPrice,
		tx.Gas,
		tx.To,
		tx.Value,
		tx.Data,
		tx.V, tx.R, tx.S,
	})
	tx.hash.Store(&hash)
	return hash
}

func (tx LegacyTx) SigningHash(chainID *big.Int) libcommon.Hash {
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
	return &tx.V, &tx.R, &tx.S
}

func (tx LegacyTx) GetChainID() *uint256.Int {
	return DeriveChainId(&tx.V)
}

func (tx *LegacyTx) Sender(signer Signer) (libcommon.Address, error) {
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
