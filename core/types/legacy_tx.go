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
	"io"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
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

// LegacyTx is the transaction data of regular Ethereum transactions.
type LegacyTx struct {
	CommonTx
	GasPrice *uint256.Int // wei per gas
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
func (tx *LegacyTx) copy() *LegacyTx {
	cpy := &LegacyTx{
		CommonTx: CommonTx{
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

func (tx *LegacyTx) EncodeRLP(w io.Writer) error {
	return nil
}

func (tx *LegacyTx) DecodeRLP(s *rlp.Stream) error {
	return nil
}

// Protected says whether the transaction is replay-protected.
func (tx *LegacyTx) Protected() bool {
	return tx.V != nil && isProtectedV(tx.V)
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

// accessors for innerTx.

func (tx *LegacyTx) Type() byte             { return LegacyTxType }
func (tx *LegacyTx) chainID() *uint256.Int  { return deriveChainId(tx.V) }
func (tx *LegacyTx) accessList() AccessList { return nil }
func (tx *LegacyTx) data() []byte           { return tx.Data }
func (tx *LegacyTx) gas() uint64            { return tx.Gas }
func (tx *LegacyTx) gasPrice() *uint256.Int { return tx.GasPrice }
func (tx *LegacyTx) value() *uint256.Int    { return tx.Value }
func (tx *LegacyTx) to() *common.Address    { return tx.To }

func (tx *LegacyTx) rawSignatureValues() (v, r, s *uint256.Int) {
	return tx.V, tx.R, tx.S
}

func (tx *LegacyTx) setSignatureValues(chainID, v, r, s *uint256.Int) {
	tx.V, tx.R, tx.S = v, r, s
}
