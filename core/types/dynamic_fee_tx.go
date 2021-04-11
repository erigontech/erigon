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
	c := tx.encodingSize()
	tx.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

func (tx DynamicFeeTransaction) encodingSize() int {
	return 0
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
	return nil
}

func (tx *DynamicFeeTransaction) DecodeRLP(s *rlp.Stream) error {
	return nil
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

// accessors for innerTx.
func (tx DynamicFeeTransaction) Type() byte { return DynamicFeeTxType }
