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

// AccessTuple is the element type of an access list.
type AccessTuple struct {
	Address     common.Address `json:"address"`
	StorageKeys []common.Hash  `json:"storageKeys"`
}

// AccessList is an EIP-2930 access list.
type AccessList []AccessTuple

// StorageKeys returns the total number of storage keys in the access list.
func (al AccessList) StorageKeys() int {
	sum := 0
	for _, tuple := range al {
		sum += len(tuple.StorageKeys)
	}
	return sum
}

// AccessListTx is the data of EIP-2930 access list transactions.
type AccessListTx struct {
	LegacyTx
	ChainID    *uint256.Int
	AccessList AccessList // EIP-2930 access list
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx *AccessListTx) copy() *AccessListTx {
	cpy := &AccessListTx{
		LegacyTx: LegacyTx{
			CommonTx: CommonTx{
				TransactionMisc: TransactionMisc{},
				Nonce:           tx.Nonce,
				To:              tx.To, // TODO: copy pointed-to address
				Data:            common.CopyBytes(tx.Data),
				GasLimit:        tx.GasLimit,
				// These are copied below.
				Value: new(uint256.Int),
			},
			GasPrice: new(uint256.Int),
		},
		ChainID:    new(uint256.Int),
		AccessList: make(AccessList, len(tx.AccessList)),
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

func (tx *AccessListTx) GetAccessList() AccessList {
	return tx.AccessList
}

func (tx *AccessListTx) GetAuthorizations() []Authorization {
	return nil
}

func (tx *AccessListTx) Protected() bool {
	return true
}

func (tx *AccessListTx) Unwrap() Transaction {
	return tx
}

// EncodingSize returns the RLP encoding size of the whole transaction envelope
func (tx *AccessListTx) EncodingSize() int {
	payloadSize, _, _, _ := tx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

// payloadSize calculates the RLP encoding size of transaction, without TxType and envelope
func (tx *AccessListTx) payloadSize() (payloadSize int, nonceLen, gasLen, accessListLen int) {
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

func accessListSize(al AccessList) int {
	var accessListLen int
	for _, tuple := range al {
		tupleLen := 21 // For the address
		// size of StorageKeys
		// Each storage key takes 33 bytes
		storageLen := 33 * len(tuple.StorageKeys)
		tupleLen += rlp.ListPrefixLen(storageLen) + storageLen
		accessListLen += rlp.ListPrefixLen(tupleLen) + tupleLen
	}
	return accessListLen
}

func encodeAccessList(al AccessList, w io.Writer, b []byte) error {
	for i := 0; i < len(al); i++ {
		tupleLen := 21
		// Each storage key takes 33 bytes
		storageLen := 33 * len(al[i].StorageKeys)
		tupleLen += rlp.ListPrefixLen(storageLen) + storageLen
		if err := rlp.EncodeStructSizePrefix(tupleLen, w, b); err != nil {
			return err
		}
		if err := rlp.EncodeOptionalAddress(&al[i].Address, w, b); err != nil { // TODO(racytech): change addr to []byte?
			return err
		}
		if err := rlp.EncodeStructSizePrefix(storageLen, w, b); err != nil {
			return err
		}
		b[0] = 128 + 32
		for idx := 0; idx < len(al[i].StorageKeys); idx++ {
			if _, err := w.Write(b[:1]); err != nil {
				return err
			}
			if _, err := w.Write(al[i].StorageKeys[idx][:]); err != nil {
				return err
			}
		}
	}
	return nil
}

// MarshalBinary returns the canonical encoding of the transaction.
// For legacy transactions, it returns the RLP encoding. For EIP-2718 typed
// transactions, it returns the type and payload.
func (tx *AccessListTx) MarshalBinary(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen := tx.payloadSize()
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
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

func (tx *AccessListTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, accessListLen int) error {
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
	// encode GasPrice
	if err := rlp.EncodeUint256(tx.GasPrice, w, b); err != nil {
		return err
	}
	// encode GasLimit
	if err := rlp.EncodeInt(tx.GasLimit, w, b); err != nil {
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
		if _, err := w.Write(tx.To[:]); err != nil {
			return err
		}
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

// EncodeRLP implements rlp.Encoder
func (tx *AccessListTx) EncodeRLP(w io.Writer) error {
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
	b[0] = AccessListTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := tx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen); err != nil {
		return err
	}
	return nil
}

func decodeAccessList(al *AccessList, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open accessList: %w", err)
	}
	var b []byte
	i := 0
	for _, err = s.List(); err == nil; _, err = s.List() {
		// decode tuple
		*al = append(*al, AccessTuple{StorageKeys: []common.Hash{}})
		tuple := &(*al)[len(*al)-1]
		if err = s.ReadBytes(tuple.Address[:]); err != nil {
			return fmt.Errorf("read Address: %w", err)
		}
		if _, err = s.List(); err != nil {
			return fmt.Errorf("open StorageKeys: %w", err)
		}
		for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
			tuple.StorageKeys = append(tuple.StorageKeys, common.Hash{})
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
	// decode AccessList
	tx.AccessList = AccessList{}
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
func (tx *AccessListTx) AsMessage(s Signer, _ *big.Int, rules *chain.Rules) (*Message, error) {
	msg := Message{
		nonce:      tx.Nonce,
		gasLimit:   tx.GasLimit,
		gasPrice:   *tx.GasPrice,
		tipCap:     *tx.GasPrice,
		feeCap:     *tx.GasPrice,
		to:         tx.To,
		amount:     *tx.Value,
		data:       tx.Data,
		accessList: tx.AccessList,
		checkNonce: true,
	}

	if !rules.IsBerlin {
		return nil, errors.New("eip-2930 transactions require Berlin")
	}

	var err error
	msg.from, err = tx.Sender(s)
	return &msg, err
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

// Hash computes the hash (but not for signatures!)
func (tx *AccessListTx) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash
	}
	hash := prefixedRlpHash(AccessListTxType, []interface{}{
		tx.ChainID,
		tx.Nonce,
		tx.GasPrice,
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

func (tx *AccessListTx) SigningHash(chainID *big.Int) common.Hash {
	return prefixedRlpHash(
		AccessListTxType,
		[]interface{}{
			chainID,
			tx.Nonce,
			tx.GasPrice,
			tx.GasLimit,
			tx.To,
			tx.Value,
			tx.Data,
			tx.AccessList,
		})
}

func (tx *AccessListTx) Type() byte { return AccessListTxType }

func (tx *AccessListTx) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return &tx.V, &tx.R, &tx.S
}

func (tx *AccessListTx) GetChainID() *uint256.Int {
	return tx.ChainID
}

func (tx *AccessListTx) cachedSender() (sender common.Address, ok bool) {
	s := tx.from.Load()
	if s == nil {
		return sender, false
	}
	return *s, true
}

var zeroAddr = common.Address{}

func (tx *AccessListTx) Sender(signer Signer) (common.Address, error) {
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
