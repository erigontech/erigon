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
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/bits"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	libkzg "github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/rlp"
)

const (
	LEN_48 = 48 // KZGCommitment & KZGProof sizes
)

type KZGCommitment [LEN_48]byte // Compressed BLS12-381 G1 element
type KZGProof [LEN_48]byte
type Blob [params.BlobSize]byte

type BlobKzgs []KZGCommitment
type KZGProofs []KZGProof
type Blobs []Blob

type BlobTxWrapper struct {
	Tx             BlobTx
	WrapperVersion byte
	Commitments    BlobKzgs
	Blobs          Blobs
	Proofs         KZGProofs
}

/* Blob methods */

func (b *Blob) payloadSize() int {
	size := 1                                                 // 0xb7..0xbf
	size += common.BitLenToByteLen(bits.Len(params.BlobSize)) // length encoding size
	size += params.BlobSize                                   // byte_array it self
	return size
}

/* BlobKzgs methods */

func (li BlobKzgs) copy() BlobKzgs {
	cpy := make(BlobKzgs, len(li))
	copy(cpy, li)
	return cpy
}

func (li BlobKzgs) payloadSize() int {
	return 49 * len(li)
}

func (li BlobKzgs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix
	buf := newEncodingBuf()
	l := rlp.EncodeListPrefix(payloadSize, buf[:])
	w.Write(buf[:l])

	for _, cmtmt := range li {
		if err := rlp.EncodeString(cmtmt[:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (li *BlobKzgs) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open BlobKzgs (Commitments): %w", err)
	}
	var b []byte
	cmtmt := KZGCommitment{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == LEN_48 {
			copy((cmtmt)[:], b)
			*li = append(*li, cmtmt)
		} else {
			return fmt.Errorf("wrong size for BlobKzgs (Commitments): %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close BlobKzgs (Commitments): %w", err)
	}

	return nil
}

/* KZGProofs methods */

func (li KZGProofs) copy() KZGProofs {
	cpy := make(KZGProofs, len(li))
	copy(cpy, li)
	return cpy
}

func (li KZGProofs) payloadSize() int {
	return 49 * len(li)
}

func (li KZGProofs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix
	buf := newEncodingBuf()
	l := rlp.EncodeListPrefix(payloadSize, buf[:])
	w.Write(buf[:l])

	for _, proof := range li {
		if err := rlp.EncodeString(proof[:], w, b); err != nil {
			return err
		}
	}
	return nil
}

func (li *KZGProofs) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()

	if err != nil {
		return fmt.Errorf("open KZGProofs (Proofs): %w", err)
	}
	var b []byte
	proof := KZGProof{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == LEN_48 {
			copy((proof)[:], b)
			*li = append(*li, proof)
		} else {
			return fmt.Errorf("wrong size for KZGProofs (Proofs): %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close KZGProofs (Proofs): %w", err)
	}

	return nil
}

/* Blobs methods */

func (blobs Blobs) copy() Blobs {
	cpy := make(Blobs, len(blobs))
	copy(cpy, blobs) // each blob element is an array and gets deep-copied
	return cpy
}

func (blobs Blobs) payloadSize() int {
	if len(blobs) > 0 {
		return len(blobs) * blobs[0].payloadSize()
	}
	return 0
}

func (blobs Blobs) encodePayload(w io.Writer, b []byte, payloadSize int) error {
	// prefix

	buf := newEncodingBuf()
	l := rlp.EncodeListPrefix(payloadSize, buf[:])
	w.Write(buf[:l])
	for _, blob := range blobs {
		if err := rlp.EncodeString(blob[:], w, b); err != nil {
			return err
		}
	}

	return nil
}

func (blobs *Blobs) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return fmt.Errorf("open Blobs: %w", err)
	}
	var b []byte
	blob := Blob{}

	for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {
		if len(b) == params.BlobSize {
			copy((blob)[:], b)
			*blobs = append(*blobs, blob)
		} else {
			return fmt.Errorf("wrong size for Blobs: %d, %v", len(b), b[0])
		}
	}

	if err = s.ListEnd(); err != nil {
		return fmt.Errorf("close Blobs: %w", err)
	}

	return nil
}

// Return KZG commitments, versioned hashes and the proofs that correspond to these blobs
func (blobs Blobs) ComputeCommitmentsAndProofs() (commitments []KZGCommitment, versionedHashes []common.Hash, proofs []KZGProof, err error) {
	commitments = make([]KZGCommitment, len(blobs))
	proofs = make([]KZGProof, len(blobs))
	versionedHashes = make([]common.Hash, len(blobs))

	kzgCtx := libkzg.Ctx()
	for i := 0; i < len(blobs); i++ {
		commitment, err := kzgCtx.BlobToKZGCommitment((*goethkzg.Blob)(&blobs[i]), 1 /*numGoRoutines*/)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not convert blob to commitment: %w", err)
		}

		proof, err := kzgCtx.ComputeBlobKZGProof((*goethkzg.Blob)(&blobs[i]), commitment, 1 /*numGoRoutnes*/)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not compute proof for blob: %w", err)
		}
		commitments[i] = KZGCommitment(commitment)
		proofs[i] = KZGProof(proof)
		versionedHashes[i] = common.Hash(libkzg.KZGToVersionedHash(commitment))
	}

	return commitments, versionedHashes, proofs, nil
}

func toBlobs(_blobs Blobs) []*goethkzg.Blob {
	blobs := make([]*goethkzg.Blob, len(_blobs))
	for i := range _blobs {
		blobs[i] = (*goethkzg.Blob)(&_blobs[i])
	}
	return blobs
}
func toComms(_comms BlobKzgs) []goethkzg.KZGCommitment {
	comms := make([]goethkzg.KZGCommitment, len(_comms))
	for i, _comm := range _comms {
		comms[i] = goethkzg.KZGCommitment(_comm)
	}
	return comms
}
func toProofs(_proofs KZGProofs) []goethkzg.KZGProof {
	proofs := make([]goethkzg.KZGProof, len(_proofs))
	for i, _proof := range _proofs {
		proofs[i] = goethkzg.KZGProof(_proof)
	}
	return proofs
}

func (c KZGCommitment) ComputeVersionedHash() common.Hash {
	return common.Hash(libkzg.KZGToVersionedHash(goethkzg.KZGCommitment(c)))
}

/* BlobTxWrapper methods */

// validateBlobTransactionWrapper implements validate_blob_transaction_wrapper from EIP-4844
func (txw *BlobTxWrapper) ValidateBlobTransactionWrapper() error {
	l1 := len(txw.Tx.BlobVersionedHashes)
	if l1 == 0 {
		return errors.New("a blob txn must contain at least one blob")
	}
	l2 := len(txw.Commitments)
	l3 := len(txw.Blobs)
	l4 := len(txw.Proofs)
	if l1 != l2 || l1 != l3 || l1 != l4 {
		return fmt.Errorf("lengths don't match %v %v %v %v", l1, l2, l3, l4)
	}
	kzgCtx := libkzg.Ctx()
	err := kzgCtx.VerifyBlobKZGProofBatch(toBlobs(txw.Blobs), toComms(txw.Commitments), toProofs(txw.Proofs))
	if err != nil {
		return fmt.Errorf("error during proof verification: %w", err)
	}
	for i, h := range txw.Tx.BlobVersionedHashes {
		if computed := txw.Commitments[i].ComputeVersionedHash(); computed != h {
			return fmt.Errorf("versioned hash %d supposedly %s but does not match computed %s", i, h, computed)
		}
	}
	return nil
}

// Implement transaction interface
func (txw *BlobTxWrapper) Type() byte               { return txw.Tx.Type() }
func (txw *BlobTxWrapper) GetChainID() *uint256.Int { return txw.Tx.GetChainID() }
func (txw *BlobTxWrapper) GetNonce() uint64         { return txw.Tx.GetNonce() }
func (txw *BlobTxWrapper) GetTipCap() *uint256.Int  { return txw.Tx.GetTipCap() }
func (txw *BlobTxWrapper) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	return txw.Tx.GetEffectiveGasTip(baseFee)
}
func (txw *BlobTxWrapper) GetFeeCap() *uint256.Int { return txw.Tx.GetFeeCap() }

func (txw *BlobTxWrapper) GetBlobHashes() []common.Hash { return txw.Tx.GetBlobHashes() }

func (txw *BlobTxWrapper) GetGasLimit() uint64    { return txw.Tx.GetGasLimit() }
func (txw *BlobTxWrapper) GetBlobGas() uint64     { return txw.Tx.GetBlobGas() }
func (txw *BlobTxWrapper) GetValue() *uint256.Int { return txw.Tx.GetValue() }
func (txw *BlobTxWrapper) GetTo() *common.Address { return txw.Tx.GetTo() }

func (txw *BlobTxWrapper) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (*Message, error) {
	return txw.Tx.AsMessage(s, baseFee, rules)
}

func (txw *BlobTxWrapper) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	signedCopy, err := txw.Tx.WithSignature(signer, sig)
	if err != nil {
		return nil, err
	}
	//goland:noinspection GoVetCopyLock
	blobTxnWrapper := &BlobTxWrapper{
		// it's ok to copy here - because it's constructor of object - no parallel access yet
		Tx:             *signedCopy.(*BlobTx), //nolint
		WrapperVersion: txw.WrapperVersion,
		Blobs:          make(Blobs, len(txw.Blobs)),
		Commitments:    make(BlobKzgs, len(txw.Commitments)),
		Proofs:         make(KZGProofs, len(txw.Proofs)),
	}
	copy(blobTxnWrapper.Blobs, txw.Blobs)
	copy(blobTxnWrapper.Commitments, txw.Commitments)
	copy(blobTxnWrapper.Proofs, txw.Proofs)
	return blobTxnWrapper, nil
}

func (txw *BlobTxWrapper) Hash() common.Hash { return txw.Tx.Hash() }

func (txw *BlobTxWrapper) SigningHash(chainID *big.Int) common.Hash {
	return txw.Tx.SigningHash(chainID)
}

func (txw *BlobTxWrapper) GetData() []byte { return txw.Tx.GetData() }

func (txw *BlobTxWrapper) GetAccessList() AccessList { return txw.Tx.GetAccessList() }

func (txw *BlobTxWrapper) GetAuthorizations() []Authorization {
	return nil
}

func (txw *BlobTxWrapper) Protected() bool { return txw.Tx.Protected() }

func (txw *BlobTxWrapper) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	return txw.Tx.RawSignatureValues()
}

func (txw *BlobTxWrapper) cachedSender() (common.Address, bool) { return txw.Tx.cachedSender() }

func (txw *BlobTxWrapper) Sender(s Signer) (common.Address, error) { return txw.Tx.Sender(s) }

func (txw *BlobTxWrapper) GetSender() (common.Address, bool) { return txw.Tx.GetSender() }

func (txw *BlobTxWrapper) SetSender(address common.Address) { txw.Tx.SetSender(address) }

func (txw *BlobTxWrapper) IsContractDeploy() bool { return txw.Tx.IsContractDeploy() }

func (txw *BlobTxWrapper) Unwrap() Transaction { return &txw.Tx }

func (txw *BlobTxWrapper) DecodeRLP(s *rlp.Stream) error {
	_, err := s.List()
	if err != nil {
		return err
	}

	if err := txw.Tx.DecodeRLP(s); err != nil {
		return err
	}

	if kind, _, err := s.Kind(); err == nil {
		if kind == rlp.Byte {
			if err := s.Decode(&txw.WrapperVersion); err != nil {
				return err
			}
		}
	} else {
		return err
	}

	if err := txw.Blobs.DecodeRLP(s); err != nil {
		return err
	}

	if err := txw.Commitments.DecodeRLP(s); err != nil {
		return err
	}

	if err := txw.Proofs.DecodeRLP(s); err != nil {
		return err
	}

	return s.ListEnd()
}

// We deliberately encode only the transaction payload because the only case we need to serialize
// blobs/commitments/proofs is when we reply to GetPooledTransactions (and that's handled by the txpool).
func (txw *BlobTxWrapper) EncodingSize() int {
	return txw.Tx.EncodingSize()
}
func (txw *BlobTxWrapper) payloadSize() (payloadSize int) {
	l, _, _, _, _ := txw.Tx.payloadSize()
	payloadSize += l + rlp.ListPrefixLen(l)
	if txw.WrapperVersion != 0 {
		payloadSize += 1
	}
	l = txw.Blobs.payloadSize()
	payloadSize += l + rlp.ListPrefixLen(l)
	l = txw.Commitments.payloadSize()
	payloadSize += l + rlp.ListPrefixLen(l)
	l = txw.Proofs.payloadSize()
	payloadSize += l + rlp.ListPrefixLen(l)
	return
}
func (txw *BlobTxWrapper) MarshalBinaryWrapped(w io.Writer) error {
	b := newEncodingBuf()
	defer pooledBuf.Put(b)
	// encode TxType
	b[0] = BlobTxType
	if _, err := w.Write(b[:1]); err != nil {
		return err
	}
	payloadSize := txw.payloadSize()
	l := rlp.EncodeListPrefix(payloadSize, b[1:])
	if _, err := w.Write(b[1 : 1+l]); err != nil {
		return err
	}
	bw := bytes.Buffer{}
	if err := txw.Tx.MarshalBinary(&bw); err != nil {
		return err
	}
	if _, err := w.Write(bw.Bytes()[1:]); err != nil {
		return err
	}
	if txw.WrapperVersion != 0 {
		b[0] = txw.WrapperVersion
		if _, err := w.Write(b[:1]); err != nil {
			return err
		}
	}
	if err := txw.Blobs.encodePayload(w, b[:], txw.Blobs.payloadSize()); err != nil {
		return err
	}
	if err := txw.Commitments.encodePayload(w, b[:], txw.Commitments.payloadSize()); err != nil {
		return err
	}
	if err := txw.Proofs.encodePayload(w, b[:], txw.Proofs.payloadSize()); err != nil {
		return err
	}
	return nil
}
func (txw *BlobTxWrapper) MarshalBinary(w io.Writer) error {
	return txw.Tx.MarshalBinary(w)
}
func (txw *BlobTxWrapper) EncodeRLP(w io.Writer) error {
	return txw.Tx.EncodeRLP(w)
}
