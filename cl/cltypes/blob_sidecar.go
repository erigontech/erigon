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

package cltypes

import (
	"encoding/json"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/cl/utils"
)

const CommitmentBranchSize = 17

type BlobSidecar struct {
	Index                    uint64                   `json:"index,string"`
	Blob                     Blob                     `json:"blob"` // define byte vector of 4096 * 32 bytes
	KzgCommitment            common.Bytes48           `json:"kzg_commitment"`
	KzgProof                 common.Bytes48           `json:"kzg_proof"`
	SignedBlockHeader        *SignedBeaconBlockHeader `json:"signed_block_header"`
	CommitmentInclusionProof solid.HashVectorSSZ      `json:"kzg_commitment_inclusion_proof"`
}

func NewBlobSidecar(index uint64, blob *Blob, kzgCommitment common.Bytes48, kzgProof common.Bytes48, signedBlockHeader *SignedBeaconBlockHeader, commitmentInclusionProof solid.HashVectorSSZ) *BlobSidecar {
	return &BlobSidecar{
		Index:                    index,
		Blob:                     *blob,
		KzgCommitment:            kzgCommitment,
		KzgProof:                 kzgProof,
		SignedBlockHeader:        signedBlockHeader,
		CommitmentInclusionProof: commitmentInclusionProof,
	}
}

func (b *BlobSidecar) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

func (b *BlobSidecar) UnmarshalJSON(buf []byte) error {
	var tmp struct {
		Index                    uint64                   `json:"index,string"`
		Blob                     *Blob                    `json:"blob"`
		KzgCommitment            common.Bytes48           `json:"kzg_commitment"`
		KzgProof                 common.Bytes48           `json:"kzg_proof"`
		SignedBlockHeader        *SignedBeaconBlockHeader `json:"signed_block_header"`
		CommitmentInclusionProof solid.HashVectorSSZ      `json:"kzg_commitment_inclusion_proof"`
	}
	tmp.Blob = &Blob{}
	tmp.CommitmentInclusionProof = solid.NewHashVector(CommitmentBranchSize)
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	b.Index = tmp.Index
	b.Blob = *tmp.Blob
	b.KzgCommitment = tmp.KzgCommitment
	b.KzgProof = tmp.KzgProof
	b.SignedBlockHeader = tmp.SignedBlockHeader
	b.CommitmentInclusionProof = tmp.CommitmentInclusionProof
	return nil
}

func (b *BlobSidecar) EncodingSizeSSZ() int {
	return length.BlockNum + 4096*32 + length.Bytes48 + length.Bytes48 + CommitmentBranchSize*length.Hash + length.Bytes96 + length.Hash*3 + length.BlockNum*2
}

func (b *BlobSidecar) DecodeSSZ(buf []byte, version int) error {
	b.CommitmentInclusionProof = solid.NewHashVector(CommitmentBranchSize)
	b.SignedBlockHeader = &SignedBeaconBlockHeader{}
	return ssz2.UnmarshalSSZ(buf, version, b.getSchema()...)
}

func (b *BlobSidecar) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema()...)
}

func (b *BlobSidecar) Clone() clonable.Clonable {
	blob := &Blob{}
	return NewBlobSidecar(b.Index, blob, b.KzgCommitment, b.KzgProof, b.SignedBlockHeader, b.CommitmentInclusionProof)
}

func (b *BlobSidecar) getSchema() []interface{} {
	s := []interface{}{&b.Index, b.Blob[:], b.KzgCommitment[:], b.KzgProof[:]}
	if b.SignedBlockHeader != nil {
		s = append(s, b.SignedBlockHeader)
	}
	if b.CommitmentInclusionProof != nil {
		s = append(s, b.CommitmentInclusionProof)
	}
	return s
}

type BlobIdentifier struct {
	BlockRoot common.Hash `json:"block_root"`
	Index     uint64      `json:"index,string"`
}

func NewBlobIdentifier(blockRoot common.Hash, index uint64) *BlobIdentifier {
	return &BlobIdentifier{
		BlockRoot: blockRoot,
		Index:     index,
	}
}

func (b *BlobIdentifier) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

func (b *BlobIdentifier) EncodingSizeSSZ() int {
	return 32 + 8
}

func (b *BlobIdentifier) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.getSchema()...)
}

func (b *BlobIdentifier) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema()...)
}

func (*BlobIdentifier) Clone() clonable.Clonable {
	return &BlobIdentifier{}
}

func (b *BlobIdentifier) getSchema() []interface{} {
	return []interface{}{
		b.BlockRoot[:],
		&b.Index,
	}
}

func VerifyCommitmentInclusionProof(commitment common.Bytes48, commitmentInclusionProof solid.HashVectorSSZ, commitmentIndex uint64, version clparams.StateVersion, bodyRoot [32]byte) bool {
	// Initialize the merkle tree leaf
	value, err := merkle_tree.HashTreeRoot(commitment[:])
	if err != nil {
		return false
	}
	bodyDepth := 4
	commitmentsDepth := uint64(13) // log2(4096) + 1 = 13
	bIndex := uint64(11)

	if commitmentInclusionProof == nil || commitmentInclusionProof.Length() < bodyDepth+int(commitmentsDepth) {
		return false
	}
	// Start by constructing the commitments subtree
	for i := uint64(0); i < commitmentsDepth; i++ {
		curr := commitmentInclusionProof.Get(int(i))
		if (commitmentIndex / utils.PowerOf2(i) % 2) == 1 {
			value = utils.Sha256(append(curr[:], value[:]...))
		} else {
			value = utils.Sha256(append(value[:], curr[:]...))
		}
	}
	// Construct up the block giga tree
	for i := uint64(0); i < uint64(bodyDepth); i++ {
		curr := commitmentInclusionProof.Get(int(i + commitmentsDepth))
		if (bIndex / utils.PowerOf2(i) % 2) == 1 {
			value = utils.Sha256(append(curr[:], value[:]...))
		} else {
			value = utils.Sha256(append(value[:], curr[:]...))
		}
	}
	return value == bodyRoot
}
