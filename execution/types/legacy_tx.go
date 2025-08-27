// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
)

type CommonTx struct {
	TransactionMisc

	Nonce    uint64          // nonce of sender account
	GasLimit uint64          // gas limit
	To       *common.Address `rlp:"nil"` // nil means contract creation
	Value    *uint256.Int    // wei amount
	Data     []byte          // contract invocation input data
	V, R, S  uint256.Int     // signature values
}

func (ct *CommonTx) GetNonce() uint64 {
	return ct.Nonce
}

func (ct *CommonTx) GetTo() *common.Address {
	return ct.To
}

func (ct *CommonTx) GetBlobGas() uint64 {
	return 0
}

func (ct *CommonTx) GetGasLimit() uint64 {
	return ct.GasLimit
}

func (ct *CommonTx) GetValue() *uint256.Int {
	return ct.Value
}

func (ct *CommonTx) GetData() []byte {
	return ct.Data
}

func (ct *CommonTx) GetSender() (common.Address, bool) {
	if sc := ct.from.Load(); sc != nil {
		return *sc, true
	}
	return common.Address{}, false
}

func (ct *CommonTx) SetSender(addr common.Address) {
	ct.from.Store(&addr)
}

func (ct *CommonTx) Protected() bool {
	return true
}

func (ct *CommonTx) IsContractDeploy() bool {
	return ct.GetTo() == nil
}

func (ct *CommonTx) GetBlobHashes() []common.Hash {
	// Only blob txs have blob hashes
	return []common.Hash{}
}

// LegacyTx is the transaction data of regular Ethereum transactions.
type LegacyTx struct {
	CommonTx
	GasPrice *uint256.Int // wei per gas
}

func (tx *LegacyTx) GetTipCap() *uint256.Int { return tx.GasPrice }
func (tx *LegacyTx) GetFeeCap() *uint256.Int { return tx.GasPrice }
func (tx *LegacyTx) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetTipCap()
	}
	gasFeeCap := tx.GetFeeCap()
	// return 0 because effectiveFee cant be < 0
	if gasFeeCap.Lt(baseFee) {
		return uint256.NewInt(0)
	}
	effectiveFee := new(uint256.Int).Sub(gasFeeCap, baseFee)
	if tx.GetTipCap().Lt(effectiveFee) {
		return tx.GetTipCap()
	} else {
		return effectiveFee
	}
}

func (tx *LegacyTx) GetAccessList() AccessList {
	return AccessList{}
}

func (tx *LegacyTx) GetAuthorizations() []Authorization {
	return nil
}

func (tx *LegacyTx) Protected() bool {
	return isProtectedV(&tx.V)
}

func (tx *LegacyTx) Unwrap() Transaction {
	return tx
}

// NewTransaction creates an unsigned legacy transaction.
// Deprecated: use NewTx instead.
func NewTransaction(nonce uint64, to common.Address, amount *uint256.Int, gasLimit uint64, gasPrice *uint256.Int, data []byte) *LegacyTx {
	return &LegacyTx{
		CommonTx: CommonTx{
			Nonce:    nonce,
			To:       &to,
			Value:    amount,
			GasLimit: gasLimit,
			Data:     data,
		},
		GasPrice: gasPrice,
	}
}

// NewContractCreation creates an unsigned legacy transaction.
// Deprecated: use NewTx instead.
func NewContractCreation(nonce uint64, amount *uint256.Int, gasLimit uint64, gasPrice *uint256.Int, data []byte) *LegacyTx {
	return &LegacyTx{
		CommonTx: CommonTx{
			Nonce:    nonce,
			Value:    amount,
			GasLimit: gasLimit,
			Data:     data,
		},
		GasPrice: gasPrice,
	}
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx *LegacyTx) copy() *LegacyTx {
	cpy := &LegacyTx{
		CommonTx: CommonTx{
			TransactionMisc: TransactionMisc{},
			Nonce:           tx.Nonce,
			To:              tx.To, // TODO: copy pointed-to address
			Data:            common.CopyBytes(tx.Data),
			GasLimit:        tx.GasLimit,
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

func (tx *LegacyTx) EncodingSize() int {
	payloadSize, _, _ := tx.payloadSize()
	return payloadSize
}

func (tx *LegacyTx) payloadSize() (payloadSize int, nonceLen, gasLen int) {
	payloadSize++
	nonceLen = rlp.IntLenExcludingHead(tx.Nonce)
	payloadSize += nonceLen
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.GasPrice)
	payloadSize++
	gasLen = rlp.IntLenExcludingHead(tx.GasLimit)
	payloadSize += gasLen
	payloadSize++
	if tx.To != nil {
		payloadSize += 20
	}
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.Value)
	// size of Data
	payloadSize += rlp.StringLen(tx.Data)
	// size of V
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.V)
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.R)
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(&tx.S)
	return payloadSize, nonceLen, gasLen
}

func (tx *LegacyTx) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen := tx.payloadSize()
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen); err != nil {
		return err
	}
	return nil
}

func (tx *LegacyTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen int) error {
	// prefix
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
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
	if err := rlp.EncodeUint256(tx.GasPrice, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeInt(tx.GasLimit, w, b); err != nil {
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
		if _, err := w.Write(tx.To[:]); err != nil {
			return err
		}
	}
	if err := rlp.EncodeUint256(tx.Value, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(&tx.V, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(&tx.R, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(&tx.S, w, b); err != nil {
		return err
	}
	return nil

}

func (tx *LegacyTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen := tx.payloadSize()
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen); err != nil {
		return err
	}
	return nil
}

func (tx *LegacyTx) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("legacy txn must be a list: %w", err)
	}
	if tx.Nonce, err = s.Uint(); err != nil {
		return fmt.Errorf("read Nonce: %w", err)
	}
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return fmt.Errorf("read GasPrice: %w", err)
	}
	tx.GasPrice = new(uint256.Int).SetBytes(b)
	if tx.GasLimit, err = s.Uint(); err != nil {
		return fmt.Errorf("read GasLimit: %w", err)
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
		return fmt.Errorf("close txn struct: %w", err)
	}
	return nil
}

// AsMessage returns the transaction as a core.Message.
func (tx *LegacyTx) AsMessage(s Signer, _ *big.Int, _ *chain.Rules) (*Message, error) {
	msg := Message{
		nonce:      tx.Nonce,
		gasLimit:   tx.GasLimit,
		gasPrice:   *tx.GasPrice,
		tipCap:     *tx.GasPrice,
		feeCap:     *tx.GasPrice,
		to:         tx.To,
		amount:     *tx.Value,
		data:       tx.Data,
		accessList: nil,
		checkNonce: true,
	}

	var err error
	msg.from, err = tx.Sender(s)
	return &msg, err
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

// Hash computes the hash (but not for signatures!)
func (tx *LegacyTx) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}
	hash := rlpHash([]interface{}{
		tx.Nonce,
		tx.GasPrice,
		tx.GasLimit,
		tx.To,
		tx.Value,
		tx.Data,
		tx.V, tx.R, tx.S,
	})
	tx.hash.Store(&hash)
	return hash
}

func (tx *LegacyTx) SigningHash(chainID *big.Int) common.Hash {
	if chainID != nil && chainID.Sign() != 0 {
		return rlpHash([]interface{}{
			tx.Nonce,
			tx.GasPrice,
			tx.GasLimit,
			tx.To,
			tx.Value,
			tx.Data,
			chainID, uint(0), uint(0),
		})
	}
	return rlpHash([]interface{}{
		tx.Nonce,
		tx.GasPrice,
		tx.GasLimit,
		tx.To,
		tx.Value,
		tx.Data,
	})
}

func (tx *LegacyTx) Type() byte { return LegacyTxType }

func (tx *LegacyTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return &tx.V, &tx.R, &tx.S
}

func (tx *LegacyTx) GetChainID() *uint256.Int {
	return DeriveChainId(&tx.V)
}

func (tx *LegacyTx) cachedSender() (sender common.Address, ok bool) {
	s := tx.from.Load()
	if s == nil {
		return sender, false
	}
	return *s, true
}
func (tx *LegacyTx) Sender(signer Signer) (common.Address, error) {
	if from := tx.from.Load(); from != nil {
		if *from != zeroAddr { // Sender address can never be zero in a transaction with a valid signer
			return *from, nil
		}
	}

	addr, err := signer.Sender(tx)
	if err != nil {
		return common.Address{}, err
	}
	tx.from.Store(&addr)
	return addr, nil
}
