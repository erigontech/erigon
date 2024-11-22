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

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/fixedgas"
	rlp2 "github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/rlp"
)

type BlobTx struct {
	DynamicFeeTransaction
	MaxFeePerBlobGas    *uint256.Int
	BlobVersionedHashes []libcommon.Hash
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (stx *BlobTx) copy() *BlobTx {
	cpy := &BlobTx{
		DynamicFeeTransaction: *stx.DynamicFeeTransaction.copy(),
		MaxFeePerBlobGas:      new(uint256.Int),
		BlobVersionedHashes:   make([]libcommon.Hash, len(stx.BlobVersionedHashes)),
	}
	copy(cpy.BlobVersionedHashes, stx.BlobVersionedHashes)
	if stx.MaxFeePerBlobGas != nil {
		cpy.MaxFeePerBlobGas.Set(stx.MaxFeePerBlobGas)
	}
	return cpy
}

func (stx *BlobTx) Type() byte { return BlobTxType }

func (stx *BlobTx) GetBlobHashes() []libcommon.Hash {
	return stx.BlobVersionedHashes
}

func (stx *BlobTx) GetBlobGas() uint64 {
	return fixedgas.BlobGasPerBlob * uint64(len(stx.BlobVersionedHashes))
}

func (stx *BlobTx) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	msg := Message{
		nonce:      stx.Nonce,
		gasLimit:   stx.Gas,
		gasPrice:   *stx.FeeCap,
		tip:        *stx.Tip,
		feeCap:     *stx.FeeCap,
		to:         stx.To,
		amount:     *stx.Value,
		data:       stx.Data,
		accessList: stx.AccessList,
		checkNonce: true,
	}
	if !rules.IsCancun {
		return msg, errors.New("BlobTx transactions require Cancun")
	}
	if baseFee != nil {
		overflow := msg.gasPrice.SetFromBig(baseFee)
		if overflow {
			return msg, errors.New("gasPrice higher than 2^256-1")
		}
	}
	msg.gasPrice.Add(&msg.gasPrice, stx.Tip)
	if msg.gasPrice.Gt(stx.FeeCap) {
		msg.gasPrice.Set(stx.FeeCap)
	}
	var err error
	msg.from, err = stx.Sender(s)
	msg.maxFeePerBlobGas = *stx.MaxFeePerBlobGas
	msg.blobHashes = stx.BlobVersionedHashes
	return msg, err
}

func (stx *BlobTx) cachedSender() (sender libcommon.Address, ok bool) {
	s := stx.from.Load()
	if s == nil {
		return sender, false
	}
	return *s, true
}

func (stx *BlobTx) Sender(signer Signer) (libcommon.Address, error) {
	if from := stx.from.Load(); from != nil {
		if *from != zeroAddr { // Sender address can never be zero in a transaction with a valid signer
			return *from, nil
		}
	}
	addr, err := signer.Sender(stx)
	if err != nil {
		return libcommon.Address{}, err
	}
	stx.from.Store(&addr)
	return addr, nil
}

func (stx *BlobTx) Hash() libcommon.Hash {
	if hash := stx.hash.Load(); hash != nil {
		return *hash
	}
	hash := prefixedRlpHash(BlobTxType, []interface{}{
		stx.ChainID,
		stx.Nonce,
		stx.Tip,
		stx.FeeCap,
		stx.Gas,
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

func (stx *BlobTx) SigningHash(chainID *big.Int) libcommon.Hash {
	return prefixedRlpHash(
		BlobTxType,
		[]interface{}{
			chainID,
			stx.Nonce,
			stx.Tip,
			stx.FeeCap,
			stx.Gas,
			stx.To,
			stx.Value,
			stx.Data,
			stx.AccessList,
			stx.MaxFeePerBlobGas,
			stx.BlobVersionedHashes,
		})
}

func (stx *BlobTx) EncodingSize() int {
	payloadSize, _, _, _, _ := stx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp2.ListPrefixLen(payloadSize) + payloadSize
}

func (stx *BlobTx) payloadSize() (payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) {
	payloadSize, nonceLen, gasLen, accessListLen = stx.DynamicFeeTransaction.payloadSize()
	// size of MaxFeePerBlobGas
	payloadSize++
	payloadSize += rlp.Uint256LenExcludingHead(stx.MaxFeePerBlobGas)
	// size of BlobVersionedHashes
	blobHashesLen = blobVersionedHashesSize(stx.BlobVersionedHashes)
	payloadSize += rlp2.ListPrefixLen(blobHashesLen) + blobHashesLen
	return
}

func blobVersionedHashesSize(hashes []libcommon.Hash) int {
	return 33 * len(hashes)
}

func encodeBlobVersionedHashes(hashes []libcommon.Hash, w io.Writer, b []byte) error {
	for _, h := range hashes {
		if err := rlp.EncodeString(h[:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (stx *BlobTx) encodePayload(w io.Writer, b []byte, payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen int) error {
	// prefix
	if err := EncodeStructSizePrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := stx.ChainID.EncodeRLP(w); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeInt(stx.Nonce, w, b); err != nil {
		return err
	}
	// encode MaxPriorityFeePerGas
	if err := stx.Tip.EncodeRLP(w); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := stx.FeeCap.EncodeRLP(w); err != nil {
		return err
	}
	// encode Gas
	if err := rlp.EncodeInt(stx.Gas, w, b); err != nil {
		return err
	}
	// encode To
	b[0] = 128 + 20
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if _, err := w.Write(stx.To.Bytes()); err != nil {
		return err
	}
	// encode Value
	if err := stx.Value.EncodeRLP(w); err != nil {
		return err
	}
	// encode Data
	if err := rlp.EncodeString(stx.Data, w, b); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(stx.AccessList, w, b); err != nil {
		return err
	}
	// encode MaxFeePerBlobGas
	if err := stx.MaxFeePerBlobGas.EncodeRLP(w); err != nil {
		return err
	}
	// prefix
	if err := EncodeStructSizePrefix(blobHashesLen, w, b); err != nil {
		return err
	}
	// encode BlobVersionedHashes
	if err := encodeBlobVersionedHashes(stx.BlobVersionedHashes, w, b); err != nil {
		return err
	}
	// encode V
	if err := stx.V.EncodeRLP(w); err != nil {
		return err
	}
	// encode R
	if err := stx.R.EncodeRLP(w); err != nil {
		return err
	}
	// encode S
	if err := stx.S.EncodeRLP(w); err != nil {
		return err
	}
	return nil
}

func (stx *BlobTx) EncodeRLP(w io.Writer) error {
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
	// size of struct prefix and TxType
	envelopeSize := 1 + rlp2.ListPrefixLen(payloadSize) + payloadSize
	var b [33]byte
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
	payloadSize, nonceLen, gasLen, accessListLen, blobHashesLen := stx.payloadSize()
	var b [33]byte
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
	stx.Tip = new(uint256.Int).SetBytes(b)

	if b, err = s.Uint256Bytes(); err != nil {
		return err
	}
	stx.FeeCap = new(uint256.Int).SetBytes(b)

	if stx.Gas, err = s.Uint(); err != nil {
		return err
	}

	if b, err = s.Bytes(); err != nil {
		return err
	}
	if len(b) != 20 {
		return fmt.Errorf("wrong size for To: %d", len(b))
	}
	stx.To = &libcommon.Address{}
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
	stx.BlobVersionedHashes = []libcommon.Hash{}
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

func decodeBlobVersionedHashes(hashes *[]libcommon.Hash, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobVersionedHashes: %w", err)
	}
	var b []byte
	_hash := libcommon.Hash{}

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
