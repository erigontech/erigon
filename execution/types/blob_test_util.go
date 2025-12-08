// Copyright 2025 The Erigon Authors
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
	"fmt"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/types/testdata"
)

func MakeBlobTxnRlp() ([]byte, []goethkzg.KZGCommitment) {
	bodyRlp := hexutil.MustDecodeHex(testdata.BodyRlpHex)

	blobsRlpPrefix := hexutil.MustDecodeHex("fa040008")
	blobRlpPrefix := hexutil.MustDecodeHex("ba020000")

	var blob0, blob1 = goethkzg.Blob{}, goethkzg.Blob{}
	copy(blob0[:], hexutil.MustDecodeHex(testdata.ValidBlob1Hex))
	copy(blob1[:], hexutil.MustDecodeHex(testdata.ValidBlob2Hex))

	var err error
	proofsRlpPrefix := hexutil.MustDecodeHex("f862")
	commitment0, _ := kzg.Ctx().BlobToKZGCommitment(&blob0, 0)
	commitment1, _ := kzg.Ctx().BlobToKZGCommitment(&blob1, 0)
	proof0, err := kzg.Ctx().ComputeBlobKZGProof(&blob0, commitment0, 0)
	if err != nil {
		fmt.Println("error", err)
	}
	proof1, err := kzg.Ctx().ComputeBlobKZGProof(&blob1, commitment1, 0)
	if err != nil {
		fmt.Println("error", err)
	}

	wrapperRlp := hexutil.MustDecodeHex("03fa0401fe")
	wrapperRlp = append(wrapperRlp, bodyRlp...)
	wrapperRlp = append(wrapperRlp, blobsRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blobRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blob0[:]...)
	wrapperRlp = append(wrapperRlp, blobRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blob1[:]...)
	wrapperRlp = append(wrapperRlp, proofsRlpPrefix...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, commitment0[:]...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, commitment1[:]...)
	wrapperRlp = append(wrapperRlp, proofsRlpPrefix...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, proof0[:]...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, proof1[:]...)

	return wrapperRlp, []goethkzg.KZGCommitment{commitment0, commitment1}
}

func MakeV1WrappedBlobTxnRlp() ([]byte, []goethkzg.KZGCommitment) {
	bodyRlp := hexutil.MustDecodeHex(testdata.BodyRlpHex)
	blobsRlpPrefix := hexutil.MustDecodeHex("fa040008")
	blobRlpPrefix := hexutil.MustDecodeHex("ba020000")

	var blob0, blob1 = goethkzg.Blob{}, goethkzg.Blob{}
	copy(blob0[:], hexutil.MustDecodeHex(testdata.ValidBlob1Hex))
	copy(blob1[:], hexutil.MustDecodeHex(testdata.ValidBlob2Hex))

	kzgCtx := kzg.Ctx()
	commitment0, _ := kzgCtx.BlobToKZGCommitment(&blob0, 0)
	commitment1, _ := kzgCtx.BlobToKZGCommitment(&blob1, 0)

	_, p1, err := kzgCtx.ComputeCellsAndKZGProofs(&blob0, 4)
	if err != nil {
		fmt.Println("error", err)
		return nil, nil
	}
	_, p2, err := kzgCtx.ComputeCellsAndKZGProofs(&blob1, 4)
	if err != nil {
		fmt.Println("error", err)
		return nil, nil
	}
	proofs := make(KZGProofs, 0, 256)
	for _, pp := range &p1 {
		proofs = append(proofs, (KZGProof(pp)))
	}
	for _, pp := range &p2 {
		proofs = append(proofs, (KZGProof(pp)))
	}

	wrapperRlp := hexutil.MustDecodeHex("03fa04329e")
	wrapperRlp = append(wrapperRlp, bodyRlp...)
	wrapperRlp = append(wrapperRlp, 0x1)

	wrapperRlp = append(wrapperRlp, blobsRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blobRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blob0[:]...)
	wrapperRlp = append(wrapperRlp, blobRlpPrefix...)
	wrapperRlp = append(wrapperRlp, blob1[:]...)

	commitmentRlpPrefix := hexutil.MustDecodeHex("f862")
	wrapperRlp = append(wrapperRlp, commitmentRlpPrefix...)

	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, commitment0[:]...)
	wrapperRlp = append(wrapperRlp, 0xb0)
	wrapperRlp = append(wrapperRlp, commitment1[:]...)

	proofsRlpPrefix := hexutil.MustDecodeHex("f93100") // 256*(48+1)
	wrapperRlp = append(wrapperRlp, proofsRlpPrefix...)
	for _, p := range proofs {
		wrapperRlp = append(wrapperRlp, 0xb0)
		wrapperRlp = append(wrapperRlp, p[:]...)
	}
	return wrapperRlp, []goethkzg.KZGCommitment{commitment0, commitment1}
}

func MakeWrappedBlobTxn(chainId *uint256.Int) *BlobTxWrapper {
	wrappedTxn := BlobTxWrapper{}
	wrappedTxn.Tx.To = &common.Address{129, 26, 117, 44, 140, 214, 151, 227, 203, 39, 39, 156, 51, 14, 209, 173, 167, 69, 168, 215}
	wrappedTxn.Tx.Nonce = 0
	wrappedTxn.Tx.GasLimit = 100000
	wrappedTxn.Tx.Value = uint256.NewInt(0)
	wrappedTxn.Tx.Data = []byte{4, 247}
	wrappedTxn.Tx.ChainID = chainId
	wrappedTxn.Tx.TipCap = uint256.NewInt(10000000000)
	wrappedTxn.Tx.FeeCap = uint256.NewInt(10000000000)
	wrappedTxn.Tx.MaxFeePerBlobGas = uint256.NewInt(123)

	wrappedTxn.Blobs = make(Blobs, 2)
	wrappedTxn.Commitments = make(BlobKzgs, 2)
	wrappedTxn.Proofs = make(KZGProofs, 2)

	copy(wrappedTxn.Blobs[0][:], hexutil.MustDecodeHex(testdata.ValidBlob1Hex))
	copy(wrappedTxn.Blobs[1][:], hexutil.MustDecodeHex(testdata.ValidBlob2Hex))

	commitment0, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(&wrappedTxn.Blobs[0]), 0)
	if err != nil {
		panic(err)
	}
	commitment1, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(&wrappedTxn.Blobs[1]), 0)
	if err != nil {
		panic(err)
	}
	copy(wrappedTxn.Commitments[0][:], commitment0[:])
	copy(wrappedTxn.Commitments[1][:], commitment1[:])

	proof0, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&wrappedTxn.Blobs[0]), commitment0, 0)
	if err != nil {
		panic(err)
	}
	proof1, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&wrappedTxn.Blobs[1]), commitment1, 0)
	if err != nil {
		panic(err)
	}
	copy(wrappedTxn.Proofs[0][:], proof0[:])
	copy(wrappedTxn.Proofs[1][:], proof1[:])

	wrappedTxn.Tx.BlobVersionedHashes = make([]common.Hash, 2)
	wrappedTxn.Tx.BlobVersionedHashes[0] = common.Hash(kzg.KZGToVersionedHash(commitment0))
	wrappedTxn.Tx.BlobVersionedHashes[1] = common.Hash(kzg.KZGToVersionedHash(commitment1))
	return &wrappedTxn
}

func MakeV1WrappedBlobTxn(chainId *uint256.Int) *BlobTxWrapper {
	wrappedTxn := BlobTxWrapper{}
	wrappedTxn.Tx.To = &common.Address{129, 26, 117, 44, 140, 214, 151, 227, 203, 39, 39, 156, 51, 14, 209, 173, 167, 69, 168, 215}
	wrappedTxn.Tx.Nonce = 0
	wrappedTxn.Tx.GasLimit = 100000
	wrappedTxn.Tx.Value = uint256.NewInt(0)
	wrappedTxn.Tx.Data = []byte{4, 247}
	wrappedTxn.Tx.ChainID = chainId
	wrappedTxn.Tx.TipCap = uint256.NewInt(10000000000)
	wrappedTxn.Tx.FeeCap = uint256.NewInt(10000000000)
	wrappedTxn.Tx.MaxFeePerBlobGas = uint256.NewInt(123)

	// Wrapper version = 0x1
	wrappedTxn.WrapperVersion = 1

	wrappedTxn.Blobs = make(Blobs, 2)
	wrappedTxn.Commitments = make(BlobKzgs, 2)
	wrappedTxn.Proofs = make(KZGProofs, 0, 2*params.CellsPerExtBlob)

	copy(wrappedTxn.Blobs[0][:], hexutil.MustDecodeHex(testdata.ValidBlob1Hex))
	copy(wrappedTxn.Blobs[1][:], hexutil.MustDecodeHex(testdata.ValidBlob2Hex))

	kzgCtx := kzg.Ctx()
	commitment0, err := kzgCtx.BlobToKZGCommitment((*goethkzg.Blob)(&wrappedTxn.Blobs[0]), 0)
	if err != nil {
		panic(err)
	}
	commitment1, err := kzgCtx.BlobToKZGCommitment((*goethkzg.Blob)(&wrappedTxn.Blobs[1]), 0)
	if err != nil {
		panic(err)
	}
	copy(wrappedTxn.Commitments[0][:], commitment0[:])
	copy(wrappedTxn.Commitments[1][:], commitment1[:])

	_, p1, err := kzgCtx.ComputeCellsAndKZGProofs((*goethkzg.Blob)(&wrappedTxn.Blobs[0]), 4)
	if err != nil {
		panic(err)
	}
	_, p2, err := kzgCtx.ComputeCellsAndKZGProofs((*goethkzg.Blob)(&wrappedTxn.Blobs[1]), 4)
	if err != nil {
		panic(err)
	}

	for _, pp := range &p1 {
		wrappedTxn.Proofs = append(wrappedTxn.Proofs, (KZGProof(pp)))
	}
	for _, pp := range &p2 {
		wrappedTxn.Proofs = append(wrappedTxn.Proofs, (KZGProof(pp)))
	}

	wrappedTxn.Tx.BlobVersionedHashes = make([]common.Hash, 2)
	wrappedTxn.Tx.BlobVersionedHashes[0] = common.Hash(kzg.KZGToVersionedHash(commitment0))
	wrappedTxn.Tx.BlobVersionedHashes[1] = common.Hash(kzg.KZGToVersionedHash(commitment1))
	return &wrappedTxn
}
