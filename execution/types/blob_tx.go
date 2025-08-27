// Copyright 2024 The Erigon Authors
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
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/rlp"
)

var ErrNilToFieldTx = errors.New("txn: field 'To' can not be 'nil'")

type BlobTx struct {
	DynamicFeeTransaction
	MaxFeePerBlobGas    *uint256.Int
	BlobVersionedHashes []common.Hash
}

func (stx *BlobTx) Type() byte { return BlobTxType }

func (stx *BlobTx) GetBlobHashes() []common.Hash {
	return stx.BlobVersionedHashes
}

func (stx *BlobTx) GetBlobGas() uint64 {
	return params.GasPerBlob * uint64(len(stx.BlobVersionedHashes))
}

func (stx *BlobTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	msg := Message{
		nonce:      stx.Nonce,
		gasLimit:   stx.GasLimit,
		gasPrice:   *stx.FeeCap,
		tipCap:     *stx.TipCap,
		feeCap:     *stx.FeeCap,
		to:         stx.To,
		amount:     *stx.Value,
		data:       stx.Data,
		accessList: stx.AccessList,
		checkNonce: true,
	}
	if !rules.IsCancun {
		return nil, errors.New("BlobTx transactions require Cancun")
	}
	if baseFee != nil {
		overflow := msg.gasPrice.SetFromBig(baseFee)
		if overflow {
			return nil, errors.New("gasPrice higher than 2^256-1")
		}
	}
	msg.gasPrice.Add(&msg.gasPrice, stx.TipCap)
	if msg.gasPrice.Gt(stx.FeeCap) {
		msg.gasPrice.Set(stx.FeeCap)
	}
	var err error
	msg.from, err = stx.Sender(s)
	msg.maxFeePerBlobGas = *stx.MaxFeePerBlobGas
	msg.blobHashes = stx.BlobVersionedHashes
	return &msg, err
}

func (stx *BlobTx) cachedSender() (sender common.Address, ok bool) {
	s := stx.from.Load()
	if s == nil {
		return sender, false
	}
	return *s, true
}

func (stx *BlobTx) Sender(signer Signer) (common.Address, error) {
	if from := stx.from.Load(); from != nil {
		if *from != zeroAddr { // Sender address can never be zero in a transaction with a valid signer
			return *from, nil
		}
	}
	addr, err := signer.Sender(stx)
	if err != nil {
		return common.Address{}, err
	}
	stx.from.Store(&addr)
	return addr, nil
}

func (stx *BlobTx) Hash() common.Hash {
	if hash := stx.hash.Load(); hash != nil {
		return *hash
	}
	hash := prefixedRlpHash(BlobTxType, []interface{}{
		stx.ChainID,
		stx.Nonce,
		stx.TipCap,
		stx.FeeCap,
		stx.GasLimit,
		stx.To,
		stx.Value,
		stx.Data,
		stx.AccessList,
		stx.MaxFeePerBlobGas,
		stx.BlobVersionedHashes,
		stx.V, stx.R, stx.S,
	})
	stx.hash.Store(&hash)
	return hash
}

func (stx *BlobTx) SigningHash(chainID *big.Int) common.Hash {
	return prefixedRlpHash(
		BlobTxType,
		[]interface{}{
			chainID,
			stx.Nonce,
			stx.TipCap,
			stx.FeeCap,
			stx.GasLimit,
			stx.To,
			stx.Value,
			stx.Data,
			stx.AccessList,
			stx.MaxFeePerBlobGas,
			stx.BlobVersionedHashes,
		})
}

func (stx *BlobTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := stx.copy()
	r, s, v, err := signer.SignatureValues(stx, sig)
	if err != nil {
		return nil, err
	}
	cpy.R.Set(r)
	cpy.S.Set(s)
	cpy.V.Set(v)
	cpy.ChainID = signer.ChainID()
	return cpy, nil
}

func (stx *BlobTx) copy() *BlobTx {
	cpy := &BlobTx{
		DynamicFeeTransaction: *stx.DynamicFeeTransaction.copy(),
		MaxFeePerBlobGas:      new(uint256.Int).Set(stx.MaxFeePerBlobGas),
		BlobVersionedHashes:   make([]common.Hash, len(stx.BlobVersionedHashes)),
	}
	copy(cpy.BlobVersionedHashes, stx.BlobVersionedHashes)
	return cpy
}

func (stx *BlobTx) EncodingSize() int {
	payloadSize, _, _, _, _ := stx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (stx *BlobTx) payloadSize() (payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) {
	payloadSize, nonceLen, gasLen, accessListLen = stx.DynamicFeeTransaction.payloadSize()
	// size of MaxFeePerBlobGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.MaxFeePerBlobGas)
	// size of BlobVersionedHashes
	blobHashesLen = blobVersionedHashesSize(stx.BlobVersionedHashes)
	payloadSize += rlp.ListPrefixLen(blobHashesLen) + blobHashesLen
	return
}

func blobVersionedHashesSize(hashes []common.Hash) int {
	return 33 * len(hashes)
}

func encodeBlobVersionedHashes(hashes []common.Hash, w io.Writer, b []byte) error {
	for i := 0; i < len(hashes); i++ {
		if err := rlp.EncodeString(hashes[i][:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (stx *BlobTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) error {
	// prefix
	if err := rlp.EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := rlp.EncodeUint256(stx.ChainID, w, b); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeInt(stx.Nonce, w, b); err != nil {
		return err
	}
	// encode MaxPriorityFeePerGas
	if err := rlp.EncodeUint256(stx.TipCap, w, b); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := rlp.EncodeUint256(stx.FeeCap, w, b); err != nil {
		return err
	}
	// encode GasLimit
	if err := rlp.EncodeInt(stx.GasLimit, w, b); err != nil {
		return err
	}
	// encode To
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(stx.To[:]); err != nil {
		return err
	}
	// encode Value
	if err := rlp.EncodeUint256(stx.Value, w, b); err != nil {
		return err
	}
	// encode Data
	if err := rlp.EncodeString(stx.Data, w, b); err != nil {
		return err
	}
	// prefix
	if err := rlp.EncodeStructSizePrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(stx.AccessList, w, b); err != nil {
		return err
	}
	// encode MaxFeePerBlobGas
	if err := rlp.EncodeUint256(stx.MaxFeePerBlobGas, w, b); err != nil {
		return err
	}
	// prefix
	if err := rlp.EncodeStructSizePrefix(blobHashesLen, w, b); err != nil {
		return err
	}
	// encode BlobVersionedHashes
	if err := encodeBlobVersionedHashes(stx.BlobVersionedHashes, w, b); err != nil {
		return err
	}
	// encode V
	if err := rlp.EncodeUint256(&stx.V, w, b); err != nil {
		return err
	}
	// encode R
	if err := rlp.EncodeUint256(&stx.R, w, b); err != nil {
		return err
	}
	// encode S
	if err := rlp.EncodeUint256(&stx.S, w, b); err != nil {
		return err
	}
	return nil
}

func (stx *BlobTx) EncodeRLP(w io.Writer) error {
	if stx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// envelope
	if err := rlp.EncodeStringSizePrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := stx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (stx *BlobTx) MarshalBinary(w io.Writer) error {
	if stx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := stx.encodePayload(w, b[:], payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (stx *BlobTx) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	var b []byte
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.ChainID = new(uint256.Int).SetBytes(b)

	if stx.Nonce, err = s.Uint(); err != nil {
		return err
	}

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.TipCap = new(uint256.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.FeeCap = new(uint256.Int).SetBytes(b)

	if stx.GasLimit, err = s.Uint(); err != nil {
		return err
	}

	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	stx.To = &common.Address{}
	copy((*stx.To)[:], b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.Value = new(uint256.Int).SetBytes(b)

	if stx.Data, err = s.Bytes(); err != nil {
		return err
	}
	// decode AccessList
	stx.AccessList = AccessList{}
	if err = decodeAccessList(&stx.AccessList, s); err != nil {
		return err
	}
	// decode MaxFeePerBlobGas
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.MaxFeePerBlobGas = new(uint256.Int).SetBytes(b)
	// decode BlobVersionedHashes
	stx.BlobVersionedHashes = []common.Hash{}
	if err = decodeBlobVersionedHashes(&stx.BlobVersionedHashes, s); err != nil {
		return err
	}
	if len(stx.BlobVersionedHashes) == 0 {
		return errors.New("a blob stx must contain at least one blob")
	}
	// decode V
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.V.SetBytes(b)

	// decode R
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.R.SetBytes(b)

	// decode S
	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.S.SetBytes(b)
	return s.ListEnd()
}

func decodeBlobVersionedHashes(hashes *[]common.Hash, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobVersionedHashes: %w", err)
	}
	var b []byte
	_hash := common.Hash{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == 32 {
			copy((_hash)[:], b)
			*hashes = append(*hashes, _hash)
		} else {
			return fmt.Errorf("wrong size for blobVersionedHashes: %d", len(b))
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobVersionedHashes: %w", err)
	}

	return nil
}
