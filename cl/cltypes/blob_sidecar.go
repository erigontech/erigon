package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

const BRANCH_SIZE = 17

type BlobSidecar struct {
	Index                    uint64                   `json:"index,string"`
	Blob                     Blob                     `json:"blob"` // define byte vector of 4096 * 32 bytes
	KzgCommitment            libcommon.Bytes48        `json:"kzg_commitment"`
	KzgProof                 libcommon.Bytes48        `json:"kzg_proof"`
	SignedBlockHeader        *SignedBeaconBlockHeader `json:"signed_block_header"`
	CommitmentInclusionProof solid.HashVectorSSZ      `json:"proof"`
}

func NewBlobSidecar(index uint64, blob *Blob, kzgCommitment libcommon.Bytes48, kzgProof libcommon.Bytes48, signedBlockHeader *SignedBeaconBlockHeader, commitmentInclusionProof solid.HashVectorSSZ) *BlobSidecar {
	return &BlobSidecar{
		Index:                    index,
		Blob:                     *blob,
		KzgCommitment:            kzgCommitment,
		KzgProof:                 kzgProof,
		SignedBlockHeader:        new(SignedBeaconBlockHeader),
		CommitmentInclusionProof: solid.NewHashVector(BRANCH_SIZE),
	}
}

func (b *BlobSidecar) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

func (b *BlobSidecar) EncodingSizeSSZ() int {
	size := 8 + 4096*32 + 48 + 48
	if b.SignedBlockHeader != nil {
		size += b.SignedBlockHeader.EncodingSizeSSZ()
	}
	if b.CommitmentInclusionProof != nil {
		size += b.CommitmentInclusionProof.EncodingSizeSSZ()
	}
	return size
}

func (b *BlobSidecar) DecodeSSZ(buf []byte, version int) error {
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
	BlockRoot libcommon.Hash `json:"block_root"`
	Index     uint64         `json:"index,string"`
}

func NewBlobIdentifier(blockRoot libcommon.Hash, index uint64) *BlobIdentifier {
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

func (b *BlobIdentifier) Clone() clonable.Clonable {
	return NewBlobIdentifier(b.BlockRoot, b.Index)
}

func (b *BlobIdentifier) getSchema() []interface{} {
	return []interface{}{
		b.BlockRoot[:],
		&b.Index,
	}
}
