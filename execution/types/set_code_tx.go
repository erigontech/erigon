// Copyright 2021 The Erigon Authors
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
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const DelegateDesignationCodeSize = 23

type SetCodeTransaction struct {
	DynamicFeeTransaction
	Authorizations []Authorization
}

func (tx *SetCodeTransaction) Unwrap() Transaction {
	return tx
}

func (tx *SetCodeTransaction) Type() byte {
	return SetCodeTxType
}

func (tx *SetCodeTransaction) GetBlobHashes() []common.Hash {
	return []common.Hash{}
}

func (tx *SetCodeTransaction) GetAuthorizations() []Authorization {
	return tx.Authorizations
}

func (tx *SetCodeTransaction) copy() *SetCodeTransaction {
	cpy := &SetCodeTransaction{}
	cpy.DynamicFeeTransaction = *tx.DynamicFeeTransaction.copy()

	cpy.Authorizations = make([]Authorization, len(tx.Authorizations))

	for i, ath := range tx.Authorizations {
		cpy.Authorizations[i] = *ath.copy()
	}

	return cpy
}

func (tx *SetCodeTransaction) EncodingSize() int {
	payloadSize, _, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *SetCodeTransaction) payloadSize() (payloadSize, accessListLen, authorizationsLen int) {
	payloadSize, accessListLen = tx.DynamicFeeTransaction.payloadSize()
	// size of Authorizations
	authorizationsLen = authorizationsSize(tx.Authorizations)
	payloadSize += rlp.ListPrefixLen(authorizationsLen) + authorizationsLen

	return
}

func (tx *SetCodeTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
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

func (tx *SetCodeTransaction) MarshalBinary(w io.Writer) error {
	if tx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, accessListLen, authorizationsLen := tx.payloadSize()
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// encode TxType
	b[0] = SetCodeTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, accessListLen, authorizationsLen); err != nil {
		return err
	}
	return nil
}

func (tx *SetCodeTransaction) AsMessage(s Signer, baseFee *uint256.Int, rules *chain.Rules) (*Message, error) {
	var to accounts.Address
	if tx.To == nil {
		to = accounts.NilAddress
	} else {
		to = accounts.InternAddress(*tx.To)
	}
	msg := Message{
		nonce:            tx.Nonce,
		gasLimit:         tx.GasLimit,
		gasPrice:         *tx.FeeCap,
		tipCap:           *tx.TipCap,
		feeCap:           *tx.FeeCap,
		to:               to,
		amount:           *tx.Value,
		data:             tx.Data,
		accessList:       tx.AccessList,
		checkNonce:       true,
		checkTransaction: true,
		checkGas:         true,
	}
	if !rules.IsPrague {
		return nil, errors.New("SetCodeTransaction is only supported in Prague")
	}
	if baseFee != nil {
		msg.gasPrice.Set(baseFee)
	}
	msg.gasPrice.Add(&msg.gasPrice, tx.TipCap)
	if msg.gasPrice.Gt(tx.FeeCap) {
		msg.gasPrice.Set(tx.FeeCap)
	}

	if len(tx.Authorizations) == 0 {
		return nil, errors.New("SetCodeTransaction without authorizations is invalid")
	}
	msg.authorizations = tx.Authorizations

	var err error
	if msg.from, err = tx.Sender(s); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (tx *SetCodeTransaction) Sender(signer Signer) (accounts.Address, error) {
	if from := tx.from; !from.IsNil() && !from.IsZero() {
		// Sender address can never be zero in a transaction with a valid signer
		return from, nil
	}
	addr, err := signer.Sender(tx)
	if err != nil {
		return accounts.ZeroAddress, err
	}
	tx.from = addr
	return addr, nil
}

func (tx *SetCodeTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}
	hash := prefixedRlpHash(SetCodeTxType, []any{
		tx.ChainID,
		tx.Nonce,
		tx.TipCap,
		tx.FeeCap,
		tx.GasLimit,
		tx.To,
		tx.Value,
		tx.Data,
		tx.AccessList,
		tx.Authorizations,
		tx.V, tx.R, tx.S,
	})
	tx.hash.Store(&hash)
	return hash
}

type setCodeTxSigHash struct {
	ChainID    *big.Int
	Nonce      uint64
	GasTipCap  *uint256.Int
	GasFeeCap  *uint256.Int
	Gas        uint64
	To         *common.Address
	Value      *uint256.Int
	Data       []byte
	AccessList AccessList
	AuthList   []Authorization
}

func (tx *SetCodeTransaction) SigningHash(chainID *big.Int) common.Hash {
	return prefixedRlpHash(
		SetCodeTxType,
		&setCodeTxSigHash{
			ChainID:    chainID,
			Nonce:      tx.Nonce,
			GasTipCap:  tx.TipCap,
			GasFeeCap:  tx.FeeCap,
			Gas:        tx.GasLimit,
			To:         tx.To,
			Value:      tx.Value,
			Data:       tx.Data,
			AccessList: tx.AccessList,
			AuthList:   tx.Authorizations,
		})
}

func (tx *SetCodeTransaction) EncodeRLP(w io.Writer) error {
	if tx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, accessListLen, authorizationsLen := tx.payloadSize()
	envelopSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// encode envelope size
	if err := rlp.EncodeStringSizePrefix(envelopSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = SetCodeTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}

	return tx.encodePayload(w, b[:], payloadSize, accessListLen, authorizationsLen)
}

func (tx *SetCodeTransaction) DecodeRLP(s *rlp.Stream) error {
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
	if len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	tx.To = &common.Address{}
	copy((*tx.To)[:], b)
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

	// decode authorizations
	tx.Authorizations = make([]Authorization, 0)
	if err = decodeAuthorizations(&tx.Authorizations, s); err != nil {
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

func (tx *SetCodeTransaction) encodePayload(w io.Writer, b []byte, payloadSize, accessListLen, authorizationsLen int) error {
	// prefix
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := rlp.EncodeUint256(*tx.ChainID, w, b); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeInt(tx.Nonce, w, b); err != nil {
		return err
	}
	// encode MaxPriorityFeePerGas
	if err := rlp.EncodeUint256(*tx.TipCap, w, b); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := rlp.EncodeUint256(*tx.FeeCap, w, b); err != nil {
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
	if err := rlp.EncodeUint256(*tx.Value, w, b); err != nil {
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
	// prefix
	if err := rlp.EncodeStructSizePrefix(authorizationsLen, w, b); err != nil {
		return err
	}
	// encode Authorizations
	if err := encodeAuthorizations(tx.Authorizations, w, b); err != nil {
		return err
	}
	// encode V
	if err := rlp.EncodeUint256(tx.V, w, b); err != nil {
		return err
	}
	// encode R
	if err := rlp.EncodeUint256(tx.R, w, b); err != nil {
		return err
	}
	// encode S
	if err := rlp.EncodeUint256(tx.S, w, b); err != nil {
		return err
	}
	return nil

}

// ParseDelegation tries to parse the address from a delegation slice.
func ParseDelegation(code []byte) (accounts.Address, bool) {
	if len(code) != DelegateDesignationCodeSize || !bytes.HasPrefix(code, params.DelegatedDesignationPrefix) {
		return accounts.NilAddress, false
	}
	var addr common.Address
	copy(addr[:], code[len(params.DelegatedDesignationPrefix):])
	return accounts.InternAddress(addr), true
}

// AddressToDelegation adds the delegation prefix to the specified address.
func AddressToDelegation(addr accounts.Address) []byte {
	addrVal := addr.Value()
	return append(params.DelegatedDesignationPrefix, addrVal[:]...)
}
