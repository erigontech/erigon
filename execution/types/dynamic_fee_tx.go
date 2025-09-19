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
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
)

type DynamicFeeTransaction struct {
	CommonTx
	ChainID    *uint256.Int
	TipCap     *uint256.Int
	FeeCap     *uint256.Int
	AccessList AccessList
}

func (tx *DynamicFeeTransaction) GetFeeCap() *uint256.Int { return tx.FeeCap }
func (tx *DynamicFeeTransaction) GetTipCap() *uint256.Int { return tx.TipCap }
func (tx *DynamicFeeTransaction) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetTipCap()
	}
	gasFeeCap := tx.GetFeeCap()
	// return 0 because effectiveFee cant be < 0
	// transaction max fee is below base fee
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

func (tx *DynamicFeeTransaction) Unwrap() Transaction {
	return tx
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx *DynamicFeeTransaction) copy() *DynamicFeeTransaction {
	cpy := &DynamicFeeTransaction{
		CommonTx: CommonTx{
			TransactionMisc: TransactionMisc{},
			Nonce:           tx.Nonce,
			To:              tx.To, // TODO: copy pointed-to address
			Data:            common.CopyBytes(tx.Data),
			GasLimit:        tx.GasLimit,
			// These are copied below.
			Value: new(uint256.Int),
		},
		ChainID:    new(uint256.Int),
		AccessList: make(AccessList, len(tx.AccessList)),
		TipCap:     new(uint256.Int),
		FeeCap:     new(uint256.Int),
	}
	copy(cpy.AccessList, tx.AccessList)
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	if tx.ChainID != nil {
		cpy.ChainID.Set(tx.ChainID)
	}
	if tx.TipCap != nil {
		cpy.TipCap.Set(tx.TipCap)
	}
	if tx.FeeCap != nil {
		cpy.FeeCap.Set(tx.FeeCap)
	}
	cpy.V.Set(&tx.V)
	cpy.R.Set(&tx.R)
	cpy.S.Set(&tx.S)
	return cpy
}

func (tx *DynamicFeeTransaction) GetAccessList() AccessList {
	return tx.AccessList
}

func (tx *DynamicFeeTransaction) GetAuthorizations() []Authorization {
	return nil
}

func (tx *DynamicFeeTransaction) EncodingSize() int {
	payloadSize, _, _, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *DynamicFeeTransaction) payloadSize() (payloadSize int, nonceLen, gasLen, accessListLen int) {
	// size of ChainID
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.ChainID)
	// size of Nonce
	payloadSize++
	nonceLen = rlp.IntLenExcludingHead(tx.Nonce)
	payloadSize += nonceLen
	// size of MaxPriorityFeePerGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.TipCap)
	// size of MaxFeePerGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(tx.FeeCap)
	// size of GasLimit
	payloadSize++
	gasLen = rlp.IntLenExcludingHead(tx.GasLimit)
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
	payloadSize += rlp.StringLen(tx.Data)
	// size of AccessList
	accessListLen = accessListSize(tx.AccessList)
	payloadSize += rlp.ListPrefixLen(accessListLen) + accessListLen
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

func (tx *DynamicFeeTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
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

// MarshalBinary returns the canonical encoding of the transaction.
// For legacy transactions, it returns the RLP encoding. For EIP-2718 typed
// transactions, it returns the type and payload.
func (tx *DynamicFeeTransaction) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen := tx.payloadSize()
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// encode TxType
	b[0] = DynamicFeeTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen); err != nil {
		return err
	}
	return nil
}

func (tx *DynamicFeeTransaction) encodePayload(w io.Writer, b []byte, payloadSize, _, _, accessListLen int) error {
	// prefix
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := rlp.EncodeUint256(tx.ChainID, w, b); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeInt(tx.Nonce, w, b); err != nil {
		return err
	}
	// encode MaxPriorityFeePerGas
	if err := rlp.EncodeUint256(tx.TipCap, w, b); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := rlp.EncodeUint256(tx.FeeCap, w, b); err != nil {
		return err
	}
	// encode GasLimit
	if err := rlp.EncodeInt(tx.GasLimit, w, b); err != nil {
		return err
	}
	// encode To
	if err := rlp.EncodeOptionalAddress(tx.To, w, b); err != nil {
		return err
	}
	// encode Value
	if err := rlp.EncodeUint256(tx.Value, w, b); err != nil {
		return err
	}
	// encode Data
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}
	// prefix
	if err := rlp.EncodeStructSizePrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(tx.AccessList, w, b); err != nil {
		return err
	}
	// encode V
	if err := rlp.EncodeUint256(&tx.V, w, b); err != nil {
		return err
	}
	// encode R
	if err := rlp.EncodeUint256(&tx.R, w, b); err != nil {
		return err
	}
	// encode S
	if err := rlp.EncodeUint256(&tx.S, w, b); err != nil {
		return err
	}
	return nil
}

func (tx *DynamicFeeTransaction) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen := tx.payloadSize()
	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = DynamicFeeTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen); err != nil {
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
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.ChainID = new(uint256.Int).SetBytes(b)
	if tx.Nonce, err = s.Uint(); err != nil {
		return err
	}
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.TipCap = new(uint256.Int).SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.FeeCap = new(uint256.Int).SetBytes(b)
	if tx.GasLimit, err = s.Uint(); err != nil {
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
	if b, err = s.Uint256Bytes(); err != nil {
		return err
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
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.V.SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.R.SetBytes(b)
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	tx.S.SetBytes(b)
	return s.ListEnd()
}

// AsMessage returns the transaction as a core.Message.
func (tx *DynamicFeeTransaction) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	msg := Message{
		nonce:      tx.Nonce,
		gasLimit:   tx.GasLimit,
		gasPrice:   *tx.FeeCap,
		tipCap:     *tx.TipCap,
		feeCap:     *tx.FeeCap,
		to:         tx.To,
		amount:     *tx.Value,
		data:       tx.Data,
		accessList: tx.AccessList,
		checkNonce: true,
	}
	if !rules.IsLondon {
		return nil, errors.New("eip-1559 transactions require London")
	}
	if baseFee != nil {
		overflow := msg.gasPrice.SetFromBig(baseFee)
		if overflow {
			return nil, errors.New("gasPrice higher than 2^256-1")
		}
	}
	msg.gasPrice.Add(&msg.gasPrice, tx.TipCap)
	if msg.gasPrice.Gt(tx.FeeCap) {
		msg.gasPrice.Set(tx.FeeCap)
	}

	var err error
	msg.from, err = tx.Sender(s)
	return &msg, err
}

// Hash computes the hash (but not for signatures!)
func (tx *DynamicFeeTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}
	hash := prefixedRlpHash(DynamicFeeTxType, []interface{}{
		tx.ChainID,
		tx.Nonce,
		tx.TipCap,
		tx.FeeCap,
		tx.GasLimit,
		tx.To,
		tx.Value,
		tx.Data,
		tx.AccessList,
		tx.V, tx.R, tx.S,
	})
	tx.hash.Store(&hash)
	return hash
}

func (tx *DynamicFeeTransaction) SigningHash(chainID *big.Int) common.Hash {
	return prefixedRlpHash(
		DynamicFeeTxType,
		[]interface{}{
			chainID,
			tx.Nonce,
			tx.TipCap,
			tx.FeeCap,
			tx.GasLimit,
			tx.To,
			tx.Value,
			tx.Data,
			tx.AccessList,
		})
}

// accessors for innerTx.
func (tx *DynamicFeeTransaction) Type() byte { return DynamicFeeTxType }

func (tx *DynamicFeeTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return &tx.V, &tx.R, &tx.S
}

func (tx *DynamicFeeTransaction) GetChainID() *uint256.Int {
	return tx.ChainID
}

func (tx *DynamicFeeTransaction) cachedSender() (sender common.Address, ok bool) {
	s := tx.from.Load()
	if s == nil {
		return sender, false
	}
	return *s, true
}
func (tx *DynamicFeeTransaction) Sender(signer Signer) (common.Address, error) {
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

// NewEIP1559Transaction creates an unsigned eip1559 transaction.
func NewEIP1559Transaction(chainID uint256.Int, nonce uint64, to common.Address, amount *uint256.Int, gasLimit uint64, gasPrice *uint256.Int, gasTip *uint256.Int, gasFeeCap *uint256.Int, data []byte) *DynamicFeeTransaction {
	return &DynamicFeeTransaction{
		CommonTx: CommonTx{
			Nonce:    nonce,
			To:       &to,
			Value:    amount,
			GasLimit: gasLimit,
			Data:     data,
		},
		ChainID: &chainID,
		TipCap:  gasTip,
		FeeCap:  gasFeeCap,
	}
}
