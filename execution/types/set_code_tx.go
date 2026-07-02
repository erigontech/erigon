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
	"io"

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
	if err := applySignature(signer, tx, sig, &cpy.CommonTx, &cpy.ChainID); err != nil {
		return nil, err
	}
	return cpy, nil
}

func (tx *SetCodeTransaction) MarshalBinary(w io.Writer) error {
	if tx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, accessListLen, authorizationsLen := tx.payloadSize()
	b := rlp.NewEncodingBuf()
	defer b.Release()
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
		gasPrice:         tx.FeeCap,
		tipCap:           tx.TipCap,
		feeCap:           tx.FeeCap,
		to:               to,
		amount:           tx.Value,
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
	msg.gasPrice.Add(&msg.gasPrice, &tx.TipCap)
	if msg.gasPrice.Gt(&tx.FeeCap) {
		msg.gasPrice.Set(&tx.FeeCap)
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
	return recoverSender(tx, &tx.TransactionMisc, signer)
}

func (tx *SetCodeTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}
	payloadSize, accessListLen, authorizationsLen := tx.payloadSize()
	hash := prefixedPayloadHash(SetCodeTxType, func(w io.Writer, b []byte) error {
		return tx.encodePayload(w, b, payloadSize, accessListLen, authorizationsLen)
	})
	tx.hash.Store(&hash)
	return hash
}

type setCodeTxSigHash struct {
	ChainID    *uint256.Int
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

func (tx *SetCodeTransaction) SigningHash(chainID *uint256.Int) common.Hash {
	return prefixedRlpHash(
		SetCodeTxType,
		&setCodeTxSigHash{
			ChainID:    chainID,
			Nonce:      tx.Nonce,
			GasTipCap:  &tx.TipCap,
			GasFeeCap:  &tx.FeeCap,
			Gas:        tx.GasLimit,
			To:         tx.To,
			Value:      &tx.Value,
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
	b := rlp.NewEncodingBuf()
	defer b.Release()
	// encode envelope size
	if err := rlp.EncodeStringPrefix(envelopSize, w, b[:]); err != nil {
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
	if err := tx.decode1559Prefix(s, true); err != nil {
		return err
	}
	tx.Authorizations = make([]Authorization, 0)
	if err := decodeAuthorizations(&tx.Authorizations, s); err != nil {
		return err
	}
	if err := tx.decodeVRS(s); err != nil {
		return err
	}
	return s.ListEnd()
}

func (tx *SetCodeTransaction) encodePayload(w io.Writer, b []byte, payloadSize, accessListLen, authorizationsLen int) error {
	if err := tx.encode1559Prefix(w, b, payloadSize, accessListLen); err != nil {
		return err
	}
	if err := rlp.EncodeListPrefix(authorizationsLen, w, b); err != nil {
		return err
	}
	if err := encodeAuthorizations(tx.Authorizations, w, b); err != nil {
		return err
	}
	return tx.encodeVRS(w, b)
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
