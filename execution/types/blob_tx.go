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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	ErrNilToFieldTx                = errors.New("txn: field 'To' can not be 'nil'")
	ErrBlobTxnEmptyBlobs           = errors.New("blob txn must contain at least one blob versioned hash")
	ErrBlobTxnInvalidVersionedHash = errors.New("blob txn versioned hash has invalid version byte")
)

type BlobTx struct {
	DynamicFeeTransaction
	MaxFeePerBlobGas    uint256.Int
	BlobVersionedHashes []common.Hash
}

func (stx *BlobTx) Type() byte { return BlobTxType }

// copyData returns a copy of BlobTx where the TransactionMisc cache fields
// (hash, from) are not copied directly but rebuilt field-by-field, avoiding
// go vet copylocks warnings on the embedded sync/atomic.Pointer.
func (stx *BlobTx) copyData() BlobTx {
	return BlobTx{
		DynamicFeeTransaction: DynamicFeeTransaction{
			CommonTx:   stx.CommonTx.copyData(),
			ChainID:    stx.ChainID,
			TipCap:     stx.TipCap,
			FeeCap:     stx.FeeCap,
			AccessList: stx.AccessList,
		},
		MaxFeePerBlobGas:    stx.MaxFeePerBlobGas,
		BlobVersionedHashes: stx.BlobVersionedHashes,
	}
}

func (stx *BlobTx) GetBlobHashes() []common.Hash {
	return stx.BlobVersionedHashes
}

func (stx *BlobTx) GetBlobGas() uint64 {
	return params.GasPerBlob * uint64(len(stx.BlobVersionedHashes))
}

func (stx *BlobTx) AsMessage(s Signer, baseFee *uint256.Int, rules *chain.Rules) (*Message, error) {
	if !rules.IsCancun {
		return nil, errors.New("BlobTx transactions require Cancun")
	}
	// EIP-4844 transaction validity: a blob txn must specify a recipient (no
	// contract creation), carry at least one versioned hash, and every hash
	// must start with the KZG version byte.
	if stx.To == nil {
		return nil, ErrNilToFieldTx
	}
	if len(stx.BlobVersionedHashes) == 0 {
		return nil, ErrBlobTxnEmptyBlobs
	}
	for _, h := range stx.BlobVersionedHashes {
		if h[0] != kzg.BlobCommitmentVersionKZG {
			return nil, ErrBlobTxnInvalidVersionedHash
		}
	}
	stxTo := accounts.InternAddress(*stx.To)
	msg := Message{
		nonce:            stx.Nonce,
		gasLimit:         stx.GasLimit,
		gasPrice:         stx.FeeCap,
		tipCap:           stx.TipCap,
		feeCap:           stx.FeeCap,
		to:               stxTo,
		amount:           stx.Value,
		data:             stx.Data,
		accessList:       stx.AccessList,
		checkNonce:       true,
		checkTransaction: true,
		checkGas:         true,
	}
	if baseFee != nil {
		msg.gasPrice.Set(baseFee)
	}
	msg.gasPrice.Add(&msg.gasPrice, &stx.TipCap)
	if msg.gasPrice.Gt(&stx.FeeCap) {
		msg.gasPrice.Set(&stx.FeeCap)
	}
	var err error
	if msg.from, err = stx.Sender(s); err != nil {
		return nil, err
	}
	msg.maxFeePerBlobGas = stx.MaxFeePerBlobGas
	msg.blobHashes = stx.BlobVersionedHashes
	return &msg, nil
}

func (stx *BlobTx) Sender(signer Signer) (accounts.Address, error) {
	return recoverSender(stx, &stx.TransactionMisc, signer)
}

func (stx *BlobTx) Hash() common.Hash {
	if hash := stx.hash.Load(); hash != nil {
		return *hash
	}
	payloadSize, accessListLen, blobHashesLen := stx.payloadSize()
	hash := prefixedPayloadHash(BlobTxType, func(w io.Writer, b []byte) error {
		return stx.encodePayload(w, b, payloadSize, accessListLen, blobHashesLen)
	})
	stx.hash.Store(&hash)
	return hash
}

type blobTxSigHash struct {
	ChainID    *uint256.Int
	Nonce      uint64
	GasTipCap  *uint256.Int
	GasFeeCap  *uint256.Int
	Gas        uint64
	To         *common.Address
	Value      *uint256.Int
	Data       []byte
	AccessList AccessList
	BlobFeeCap *uint256.Int
	BlobHashes []common.Hash
}

func (stx *BlobTx) SigningHash(chainID *uint256.Int) common.Hash {
	return prefixedRlpHash(
		BlobTxType,
		&blobTxSigHash{
			ChainID:    chainID,
			Nonce:      stx.Nonce,
			GasTipCap:  &stx.TipCap,
			GasFeeCap:  &stx.FeeCap,
			Gas:        stx.GasLimit,
			To:         stx.To,
			Value:      &stx.Value,
			Data:       stx.Data,
			AccessList: stx.AccessList,
			BlobFeeCap: &stx.MaxFeePerBlobGas,
			BlobHashes: stx.BlobVersionedHashes,
		})
}

func (stx *BlobTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := stx.copy()
	if err := applySignature(signer, stx, sig, &cpy.CommonTx, &cpy.ChainID); err != nil {
		return nil, err
	}
	return cpy, nil
}

func (stx *BlobTx) copy() *BlobTx {
	cpy := &BlobTx{
		DynamicFeeTransaction: *stx.DynamicFeeTransaction.copy(),
		MaxFeePerBlobGas:      stx.MaxFeePerBlobGas,
		BlobVersionedHashes:   make([]common.Hash, len(stx.BlobVersionedHashes)),
	}
	copy(cpy.BlobVersionedHashes, stx.BlobVersionedHashes)
	return cpy
}

func (stx *BlobTx) EncodingSize() int {
	payloadSize, _, _ := stx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (stx *BlobTx) payloadSize() (payloadSize, accessListLen, blobHashesLen int) {
	payloadSize, accessListLen = stx.DynamicFeeTransaction.payloadSize()
	payloadSize += rlp.Uint256Len(stx.MaxFeePerBlobGas)
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

func (stx *BlobTx) encodePayload(w io.Writer, b []byte, payloadSize, accessListLen, blobHashesLen int) error {
	if err := stx.encode1559Prefix(w, b, payloadSize, accessListLen); err != nil {
		return err
	}
	if err := rlp.EncodeUint256(stx.MaxFeePerBlobGas, w, b); err != nil {
		return err
	}
	if err := rlp.EncodeListPrefix(blobHashesLen, w, b); err != nil {
		return err
	}
	if err := encodeBlobVersionedHashes(stx.BlobVersionedHashes, w, b); err != nil {
		return err
	}
	return stx.encodeVRS(w, b)
}

func (stx *BlobTx) EncodeRLP(w io.Writer) error {
	if stx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, accessListLen, blobHashesLen := stx.payloadSize()
	return encodeRLPTyped(w, BlobTxType, payloadSize, func(w io.Writer, b []byte) error {
		return stx.encodePayload(w, b, payloadSize, accessListLen, blobHashesLen)
	})
}

func (stx *BlobTx) MarshalBinary(w io.Writer) error {
	if stx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, accessListLen, blobHashesLen := stx.payloadSize()
	return marshalTyped(w, BlobTxType, func(w io.Writer, b []byte) error {
		return stx.encodePayload(w, b, payloadSize, accessListLen, blobHashesLen)
	})
}

func (stx *BlobTx) DecodeRLP(s *rlp.Stream) error {
	if err := stx.decode1559Prefix(s, true); err != nil {
		return err
	}
	if err := s.ReadUint256(&stx.MaxFeePerBlobGas); err != nil {
		return err
	}
	stx.BlobVersionedHashes = []common.Hash{}
	if err := decodeBlobVersionedHashes(&stx.BlobVersionedHashes, s); err != nil {
		return err
	}
	if len(stx.BlobVersionedHashes) == 0 {
		return errors.New("a blob stx must contain at least one blob")
	}
	if err := stx.decodeVRS(s); err != nil {
		return err
	}
	return s.ListEnd()
}

func decodeBlobVersionedHashes(hashes *[]common.Hash, s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobVersionedHashes: %w", err)
	}
	for s.MoreDataInList() {
		var h common.Hash
		if err = s.ReadBytes(h[:]); err != nil {
			return fmt.Errorf("read blobVersionedHash: %w", err)
		}
		*hashes = append(*hashes, h)
	}
	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobVersionedHashes: %w", err)
	}
	return nil
}
