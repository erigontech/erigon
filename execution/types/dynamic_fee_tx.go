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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type DynamicFeeTransaction struct {
	CommonTx
	ChainID    uint256.Int
	TipCap     uint256.Int
	FeeCap     uint256.Int
	AccessList AccessList
}

func (tx *DynamicFeeTransaction) GetFeeCap() *uint256.Int { return &tx.FeeCap }
func (tx *DynamicFeeTransaction) GetTipCap() *uint256.Int { return &tx.TipCap }
func (tx *DynamicFeeTransaction) GetEffectiveGasTip(baseFee *uint256.Int) uint256.Int {
	return CalcEffectiveGasTip(baseFee, tx.GetTipCap, tx.GetFeeCap)
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
			Data:            common.Copy(tx.Data),
			GasLimit:        tx.GasLimit,
			Value:           tx.Value,
			V:               tx.V,
			R:               tx.R,
			S:               tx.S,
		},
		ChainID:    tx.ChainID,
		AccessList: make(AccessList, len(tx.AccessList)),
		TipCap:     tx.TipCap,
		FeeCap:     tx.FeeCap,
	}
	copy(cpy.AccessList, tx.AccessList)
	return cpy
}

func (tx *DynamicFeeTransaction) GetAccessList() AccessList {
	return tx.AccessList
}

func (tx *DynamicFeeTransaction) GetAuthorizations() []Authorization {
	return nil
}

func (tx *DynamicFeeTransaction) EncodingSize() int {
	payloadSize, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (tx *DynamicFeeTransaction) payloadSize() (payloadSize int, accessListLen int) {
	payloadSize += rlp.Uint256Len(tx.ChainID)
	payloadSize += rlp.U64Len(tx.Nonce)
	payloadSize += rlp.Uint256Len(tx.TipCap)
	payloadSize += rlp.Uint256Len(tx.FeeCap)
	payloadSize += rlp.U64Len(tx.GasLimit)

	// size of To
	payloadSize++
	if tx.To != nil {
		payloadSize += 20
	}

	payloadSize += rlp.Uint256Len(tx.Value)
	payloadSize += rlp.StringLen(tx.Data)

	// size of AccessList
	accessListLen = accessListSize(tx.AccessList)
	payloadSize += rlp.ListPrefixLen(accessListLen) + accessListLen

	payloadSize += rlp.Uint256Len(tx.V)
	payloadSize += rlp.Uint256Len(tx.R)
	payloadSize += rlp.Uint256Len(tx.S)
	return payloadSize, accessListLen
}

func (tx *DynamicFeeTransaction) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := tx.copy()
	if err := applySignature(signer, tx, sig, &cpy.CommonTx, &cpy.ChainID); err != nil {
		return nil, err
	}
	return cpy, nil
}

// MarshalBinary returns the canonical encoding of the transaction.
// For legacy transactions, it returns the RLP encoding. For EIP-2718 typed
// transactions, it returns the type and payload.
func (tx *DynamicFeeTransaction) MarshalBinary(w io.Writer) error {
	payloadSize, accessListLen := tx.payloadSize()
	return marshalTyped(w, DynamicFeeTxType, func(w io.Writer, b []byte) error {
		return tx.encodePayload(w, b, payloadSize, accessListLen)
	})
}

// encode1559Prefix writes the payload list prefix and the leading fields shared
// by all EIP-1559-style payloads: ChainID, Nonce, TipCap, FeeCap, GasLimit, To,
// Value, Data and AccessList.
func (tx *DynamicFeeTransaction) encode1559Prefix(w io.Writer, b []byte, payloadSize, accessListLen int) error {
	if err := rlp.EncodeListPrefix(payloadSize, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(tx.ChainID, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeU64(tx.Nonce, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(tx.TipCap, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(tx.FeeCap, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeU64(tx.GasLimit, w, b); err != nil {
		return err
	}
	if err := EncodeOptionalAddress(tx.To, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(tx.Value, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeString(tx.Data, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeListPrefix(accessListLen, w, b); err != nil {
		return err
	}
	return encodeAccessList(tx.AccessList, w, b)
}

func (tx *DynamicFeeTransaction) encodePayload(w io.Writer, b []byte, payloadSize, accessListLen int) error {
	if err := tx.encode1559Prefix(w, b, payloadSize, accessListLen); err != nil {
		return err
	}
	return tx.encodeVRS(w, b)
}

func (tx *DynamicFeeTransaction) EncodeRLP(w io.Writer) error {
	payloadSize, accessListLen := tx.payloadSize()
	return encodeRLPTyped(w, DynamicFeeTxType, payloadSize, func(w io.Writer, b []byte) error {
		return tx.encodePayload(w, b, payloadSize, accessListLen)
	})
}

// decode1559Prefix reads the payload fields shared by all EIP-1559-style
// payloads, mirroring encode1559Prefix. With strictTo the To field must be a
// 20-byte address (no contract creation).
func (tx *DynamicFeeTransaction) decode1559Prefix(s *rlp.Stream, strictTo bool) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	if err = s.ReadUint256(&tx.ChainID); err != nil {
		return err
	}
	if tx.Nonce, err = s.Uint64(); err != nil {
		return err
	}
	if err = s.ReadUint256(&tx.TipCap); err != nil {
		return err
	}
	if err = s.ReadUint256(&tx.FeeCap); err != nil {
		return err
	}
	if tx.GasLimit, err = s.Uint64(); err != nil {
		return err
	}
	if strictTo {
		tx.To = &common.Address{}
		if kind, size, err := s.Kind(); err != nil {
			return err
		} else if kind == rlp.Byte {
			return errors.New("wrong size for To: 1")
		} else if size != 20 {
			return fmt.Errorf("wrong size for To: %d", size)
		}
		if err = s.ReadBytes(tx.To[:]); err != nil {
			return err
		}
	} else if err = DecodeOptionalAddress(&tx.To, s); err != nil {
		return err
	}
	if err = s.ReadUint256(&tx.Value); err != nil {
		return err
	}
	if tx.Data, err = s.Bytes(); err != nil {
		return err
	}
	tx.AccessList = AccessList{}
	return decodeAccessList(&tx.AccessList, s)
}

func (tx *DynamicFeeTransaction) DecodeRLP(s *rlp.Stream) error {
	if err := tx.decode1559Prefix(s, false); err != nil {
		return err
	}
	if err := tx.decodeVRS(s); err != nil {
		return err
	}
	return s.ListEnd()
}

// AsMessage returns the transaction as a core.Message.
func (tx *DynamicFeeTransaction) AsMessage(s Signer, baseFee *uint256.Int, rules *chain.Rules) (*Message, error) {
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
	if !rules.IsLondon {
		return nil, errors.New("eip-1559 transactions require London")
	}
	if baseFee != nil {
		msg.gasPrice.Set(baseFee)
	}
	msg.gasPrice.Add(&msg.gasPrice, &tx.TipCap)
	if msg.gasPrice.Gt(&tx.FeeCap) {
		msg.gasPrice.Set(&tx.FeeCap)
	}

	var err error
	if msg.from, err = tx.Sender(s); err != nil {
		return nil, err
	}
	return &msg, nil
}

// Hash computes the hash (but not for signatures!)
func (tx *DynamicFeeTransaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}
	payloadSize, accessListLen := tx.payloadSize()
	hash := prefixedPayloadHash(DynamicFeeTxType, func(w io.Writer, b []byte) error {
		return tx.encodePayload(w, b, payloadSize, accessListLen)
	})
	tx.hash.Store(&hash)
	return hash
}

type dynamicFeeTxSigHash struct {
	ChainID    *uint256.Int
	Nonce      uint64
	GasTipCap  *uint256.Int
	GasFeeCap  *uint256.Int
	Gas        uint64
	To         *common.Address `rlp:"nil"`
	Value      *uint256.Int
	Data       []byte
	AccessList AccessList
}

func (tx *DynamicFeeTransaction) SigningHash(chainID *uint256.Int) common.Hash {
	return prefixedRlpHash(
		DynamicFeeTxType,
		&dynamicFeeTxSigHash{
			ChainID:    chainID,
			Nonce:      tx.Nonce,
			GasTipCap:  &tx.TipCap,
			GasFeeCap:  &tx.FeeCap,
			Gas:        tx.GasLimit,
			To:         tx.To,
			Value:      &tx.Value,
			Data:       tx.Data,
			AccessList: tx.AccessList,
		})
}

// accessors for innerTx.
func (tx *DynamicFeeTransaction) Type() byte { return DynamicFeeTxType }

func (tx *DynamicFeeTransaction) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return &tx.V, &tx.R, &tx.S
}

func (tx *DynamicFeeTransaction) GetChainID() *uint256.Int {
	return &tx.ChainID
}

func (tx *DynamicFeeTransaction) Sender(signer Signer) (accounts.Address, error) {
	return recoverSender(tx, &tx.TransactionMisc, signer)
}

// NewEIP1559Transaction creates an unsigned eip1559 transaction.
func NewEIP1559Transaction(chainID uint256.Int, nonce uint64, to common.Address, amount *uint256.Int, gasLimit uint64, gasPrice *uint256.Int, gasTip *uint256.Int, gasFeeCap *uint256.Int, data []byte) *DynamicFeeTransaction {
	return &DynamicFeeTransaction{
		CommonTx: CommonTx{
			Nonce:    nonce,
			To:       &to,
			Value:    *amount,
			GasLimit: gasLimit,
			Data:     data,
		},
		ChainID: chainID,
		TipCap:  *gasTip,
		FeeCap:  *gasFeeCap,
	}
}
