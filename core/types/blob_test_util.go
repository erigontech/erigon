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

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon/core/types/testdata"
)

func MakeBlobTxnRlp() ([]byte, []gokzg4844.KZGCommitment) {
	bodyRlp := hexutil.MustDecodeHex(testdata.BodyRlpHex)

	blobsRlpPrefix := hexutil.MustDecodeHex("fa040008")
	blobRlpPrefix := hexutil.MustDecodeHex("ba020000")

	var blob0, blob1 = gokzg4844.Blob{}, gokzg4844.Blob{}
	copy(blob0[:], hexutil.MustDecodeHex(testdata.ValidBlob1Hex))
	copy(blob1[:], hexutil.MustDecodeHex(testdata.ValidBlob2Hex))

	var err error
	proofsRlpPrefix := hexutil.MustDecodeHex("f862")
	commitment0, _ := kzg.Ctx().BlobToKZGCommitment(blob0[:], 0)
	commitment1, _ := kzg.Ctx().BlobToKZGCommitment(blob1[:], 0)
	proof0, err := kzg.Ctx().ComputeBlobKZGProof(blob0[:], commitment0, 0)
	if err != nil {
		fmt.Println("error", err)
	}
	proof1, err := kzg.Ctx().ComputeBlobKZGProof(blob1[:], commitment1, 0)
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

	return wrapperRlp, []gokzg4844.KZGCommitment{commitment0, commitment1}
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

	commitment0, err := kzg.Ctx().BlobToKZGCommitment(wrappedTxn.Blobs[0][:], 0)
	if err != nil {
		panic(err)
	}
	commitment1, err := kzg.Ctx().BlobToKZGCommitment(wrappedTxn.Blobs[1][:], 0)
	if err != nil {
		panic(err)
	}
	copy(wrappedTxn.Commitments[0][:], commitment0[:])
	copy(wrappedTxn.Commitments[1][:], commitment1[:])

	proof0, err := kzg.Ctx().ComputeBlobKZGProof(wrappedTxn.Blobs[0][:], commitment0, 0)
	if err != nil {
		panic(err)
	}
	proof1, err := kzg.Ctx().ComputeBlobKZGProof(wrappedTxn.Blobs[1][:], commitment1, 0)
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
