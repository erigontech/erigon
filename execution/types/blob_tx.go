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
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	ErrNilToFieldTx                = errors.New("txn: field 'To' can not be 'nil'")
	ErrBlobTxnEmptyBlobs           = errors.New("blob txn must contain at least one blob versioned hash")
	ErrBlobTxnInvalidVersionedHash = errors.New("blob txn versioned hash has invalid version byte")
	ErrBlobTxnPreCancun            = errors.New("BlobTx transactions require Cancun")
)

// ValidateBlobPrerequisites checks the EIP-4844 rules a blob-carrying message must satisfy.
func ValidateBlobPrerequisites(blobHashes []common.Hash, contractCreation, isCancun bool) error {
	if !isCancun {
		return ErrBlobTxnPreCancun
	}
	if contractCreation {
		return ErrNilToFieldTx
	}
	if len(blobHashes) == 0 {
		return ErrBlobTxnEmptyBlobs
	}
	for _, h := range blobHashes {
		if h[0] != kzg.BlobCommitmentVersionKZG {
			return ErrBlobTxnInvalidVersionedHash
		}
	}
	return nil
}

type BlobTx struct {
	DynamicFeeTransaction
	MaxFeePerBlobGas    uint256.Int
	BlobVersionedHashes []common.Hash
}

func (btx *BlobTx) Type() byte { return BlobTxType }

// copyData returns a copy of BlobTx where the TransactionMisc cache fields
// (hash, from) are not copied directly but rebuilt field-by-field, avoiding
// go vet copylocks warnings on the embedded sync/atomic.Pointer.
func (btx *BlobTx) copyData() BlobTx {
	return BlobTx{
		DynamicFeeTransaction: DynamicFeeTransaction{
			CommonTx:   btx.CommonTx.copyData(),
			ChainID:    btx.ChainID,
			TipCap:     btx.TipCap,
			FeeCap:     btx.FeeCap,
			AccessList: btx.AccessList,
		},
		MaxFeePerBlobGas:    btx.MaxFeePerBlobGas,
		BlobVersionedHashes: btx.BlobVersionedHashes,
	}
}

func (btx *BlobTx) GetBlobHashes() []common.Hash {
	return btx.BlobVersionedHashes
}

func (btx *BlobTx) GetBlobGas() uint64 {
	return params.GasPerBlob * uint64(len(btx.BlobVersionedHashes))
}

func (btx *BlobTx) AsMessage(s Signer, baseFee *uint256.Int, rules *chain.Rules) (*Message, error) {
	if err := ValidateBlobPrerequisites(btx.BlobVersionedHashes, btx.To == nil, rules.IsCancun); err != nil {
		return nil, err
	}
	stxTo := accounts.InternAddress(*btx.To)
	msg := Message{
		nonce:            btx.Nonce,
		gasLimit:         btx.GasLimit,
		gasPrice:         btx.FeeCap,
		tipCap:           btx.TipCap,
		feeCap:           btx.FeeCap,
		to:               stxTo,
		amount:           btx.Value,
		data:             btx.Data,
		accessList:       btx.AccessList,
		checkNonce:       true,
		checkTransaction: true,
		checkGas:         true,
	}
	if baseFee != nil {
		msg.gasPrice.Set(baseFee)
	}
	msg.gasPrice.Add(&msg.gasPrice, &btx.TipCap)
	if msg.gasPrice.Gt(&btx.FeeCap) {
		msg.gasPrice.Set(&btx.FeeCap)
	}
	var err error
	if msg.from, err = btx.Sender(s); err != nil {
		return nil, err
	}
	msg.maxFeePerBlobGas = btx.MaxFeePerBlobGas
	msg.blobHashes = btx.BlobVersionedHashes
	return &msg, nil
}

func (btx *BlobTx) cachedSender() (sender accounts.Address, ok bool) {
	s := btx.from
	if s.IsNil() {
		return sender, false
	}
	return s, true
}

func (btx *BlobTx) Sender(signer Signer) (accounts.Address, error) {
	if from := btx.from; !from.IsNil() && !from.IsZero() {
		// Sender address can never be zero in a transaction with a valid signer
		return from, nil
	}
	addr, err := signer.Sender(btx)
	if err != nil {
		return accounts.ZeroAddress, err
	}
	btx.from = addr
	return addr, nil
}

func (btx *BlobTx) Hash() common.Hash {
	if hash := btx.hash.Load(); hash != nil {
		return *hash
	}
	payloadSize, accessListLen, blobHashesLen := btx.payloadSize()
	hash := prefixedPayloadHash(BlobTxType, func(w io.Writer, b []byte) error {
		return btx.encodePayload(w, b, payloadSize, accessListLen, blobHashesLen)
	})
	btx.hash.Store(&hash)
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

func (btx *BlobTx) SigningHash(chainID *uint256.Int) common.Hash {
	return prefixedRlpHash(
		BlobTxType,
		&blobTxSigHash{
			ChainID:    chainID,
			Nonce:      btx.Nonce,
			GasTipCap:  &btx.TipCap,
			GasFeeCap:  &btx.FeeCap,
			Gas:        btx.GasLimit,
			To:         btx.To,
			Value:      &btx.Value,
			Data:       btx.Data,
			AccessList: btx.AccessList,
			BlobFeeCap: &btx.MaxFeePerBlobGas,
			BlobHashes: btx.BlobVersionedHashes,
		},
	)
}

func (btx *BlobTx) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	cpy := btx.copy()
	r, s, v, err := signer.SignatureValues(btx, sig)
	if err != nil {
		return nil, err
	}
	cpy.R.Set(r)
	cpy.S.Set(s)
	cpy.V.Set(v)
	cpy.ChainID = *signer.ChainID()
	return cpy, nil
}

func (btx *BlobTx) copy() *BlobTx {
	cpy := &BlobTx{
		DynamicFeeTransaction: *btx.DynamicFeeTransaction.copy(),
		MaxFeePerBlobGas:      btx.MaxFeePerBlobGas,
		BlobVersionedHashes:   make([]common.Hash, len(btx.BlobVersionedHashes)),
	}
	copy(cpy.BlobVersionedHashes, btx.BlobVersionedHashes)
	return cpy
}

func (btx *BlobTx) EncodingSize() int {
	payloadSize, _, _ := btx.payloadSize()
	// Add envelope size and type size
	return 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
}

func (btx *BlobTx) payloadSize() (payloadSize, accessListLen, blobHashesLen int) {
	payloadSize, accessListLen = btx.DynamicFeeTransaction.payloadSize()
	payloadSize += rlp.Uint256Len(btx.MaxFeePerBlobGas)
	// size of BlobVersionedHashes
	blobHashesLen = blobVersionedHashesSize(btx.BlobVersionedHashes)
	payloadSize += rlp.ListPrefixLen(blobHashesLen) + blobHashesLen
	return
}

func blobVersionedHashesSize(hashes []common.Hash) int {
	return 33 * len(hashes)
}

func encodeBlobVersionedHashes(hashes []common.Hash, w io.Writer, b []byte) error {
	for i := range hashes {
		if err := rlp.EncodeString(hashes[i][:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (btx *BlobTx) encodePayload(w io.Writer, b []byte, payloadSize, accessListLen, blobHashesLen int) error {
	// prefix
	if err := rlp.EncodeListPrefix(payloadSize, w, b); err != nil {
		return err
	}
	// encode ChainID
	if err := rlp.EncodeUint256(btx.ChainID, w, b); err != nil {
		return err
	}
	// encode Nonce
	if err := rlp.EncodeU64(btx.Nonce, w, b); err != nil {
		return err
	}
	// encode MaxPriorityFeePerGas
	if err := rlp.EncodeUint256(btx.TipCap, w, b); err != nil {
		return err
	}
	// encode MaxFeePerGas
	if err := rlp.EncodeUint256(btx.FeeCap, w, b); err != nil {
		return err
	}
	// encode GasLimit
	if err := rlp.EncodeU64(btx.GasLimit, w, b); err != nil {
		return err
	}
	// encode To
	if err := EncodeOptionalAddress(btx.To, w, b); err != nil {
		return err
	}
	// encode Value
	if err := rlp.EncodeUint256(btx.Value, w, b); err != nil {
		return err
	}
	// encode Data
	if err := rlp.EncodeString(btx.Data, w, b); err != nil {
		return err
	}
	// prefix
	if err := rlp.EncodeListPrefix(accessListLen, w, b); err != nil {
		return err
	}
	// encode AccessList
	if err := encodeAccessList(btx.AccessList, w, b); err != nil {
		return err
	}
	// encode MaxFeePerBlobGas
	if err := rlp.EncodeUint256(btx.MaxFeePerBlobGas, w, b); err != nil {
		return err
	}
	// prefix
	if err := rlp.EncodeListPrefix(blobHashesLen, w, b); err != nil {
		return err
	}
	// encode BlobVersionedHashes
	if err := encodeBlobVersionedHashes(btx.BlobVersionedHashes, w, b); err != nil {
		return err
	}
	// encode V
	if err := rlp.EncodeUint256(btx.V, w, b); err != nil {
		return err
	}
	// encode R
	if err := rlp.EncodeUint256(btx.R, w, b); err != nil {
		return err
	}
	// encode S
	if err := rlp.EncodeUint256(btx.S, w, b); err != nil {
		return err
	}
	return nil
}

func (btx *BlobTx) EncodeRLP(w io.Writer) error {
	if btx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, accessListLen, blobHashesLen := btx.payloadSize()
	// size of struct prefix and TxType
	envelopeSize := 1 + rlp.ListPrefixLen(payloadSize) + payloadSize
	b := rlp.NewEncodingBuf()
	defer b.Release()
	// envelope
	if err := rlp.EncodeStringPrefix(envelopeSize, w, b[:]); err != nil {
		return err
	}
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := btx.encodePayload(w, b[:], payloadSize, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (btx *BlobTx) MarshalBinary(w io.Writer) error {
	if btx.To == nil {
		return ErrNilToFieldTx
	}
	payloadSize, accessListLen, blobHashesLen := btx.payloadSize()
	b := rlp.NewEncodingBuf()
	defer b.Release()
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	if err := btx.encodePayload(w, b[:], payloadSize, accessListLen, blobHashesLen); err != nil {
		return err
	}
	return nil
}

func (btx *BlobTx) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}
	if err := s.ReadUint256(&btx.ChainID); err != nil {
		return err
	}
	if btx.Nonce, err = s.Uint64(); err != nil {
		return err
	}
	if err := s.ReadUint256(&btx.TipCap); err != nil {
		return err
	}
	if err := s.ReadUint256(&btx.FeeCap); err != nil {
		return err
	}
	if btx.GasLimit, err = s.Uint64(); err != nil {
		return err
	}
	if kind, size, err := s.Kind(); err != nil {
		return err
	} else if kind == rlp.Byte {
		return errors.New("wrong size for To: 1")
	} else if size != length.Addr {
		return fmt.Errorf("wrong size for To: %d", size)
	}
	to, err := s.Addr()
	if err != nil {
		return err
	}
	btx.To = &to
	if err := s.ReadUint256(&btx.Value); err != nil {
		return err
	}
	if btx.Data, err = s.Bytes(); err != nil {
		return err
	}
	// decode AccessList
	btx.AccessList = AccessList{}
	if err := decodeAccessList(&btx.AccessList, s); err != nil {
		return err
	}
	// decode MaxFeePerBlobGas
	if err := s.ReadUint256(&btx.MaxFeePerBlobGas); err != nil {
		return err
	}
	// decode BlobVersionedHashes
	if btx.BlobVersionedHashes, err = decodeHashListTo(s, nil); err != nil {
		return fmt.Errorf("read BlobVersionedHashes: %w", err)
	}
	if len(btx.BlobVersionedHashes) == 0 {
		return errors.New("a blob stx must contain at least one blob")
	}
	// decode V
	if err := s.ReadUint256(&btx.V); err != nil {
		return err
	}
	if err := s.ReadUint256(&btx.R); err != nil {
		return err
	}
	if err := s.ReadUint256(&btx.S); err != nil {
		return err
	}
	return s.ListEnd()
}
