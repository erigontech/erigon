package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

const MAX_BLOBS_PER_BLOCK = 4096
const (
	MAX_REQUEST_BLOCKS_DENEB              = 128
	MAX_REQUEST_BLOB_SIDECARS             = MAX_REQUEST_BLOCKS_DENEB * MAX_BLOBS_PER_BLOCK
	MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS = 4096
	BLOB_SIDECAR_SUBNET_COUNT             = 6
)

type BlobSidecar struct {
	Index                    uint64                   `json:"index,string"`
	Blob                     Blob                     `json:"blob"` // define byte vector of 4096 * 32 bytes
	KzgCommitment            libcommon.Bytes48        `json:"kzg_commitment"`
	KzgProof                 libcommon.Bytes48        `json:"kzg_proof"`
	SignedBlockHeader        *SignedBeaconBlockHeader `json:"signed_block_header"`
	CommitmentInclusionProof solid.HashVectorSSZ      `json:"proof"`
}

func NewBlobSidecar(index uint64, blob Blob, kzgCommitment libcommon.Bytes48, kzgProof libcommon.Bytes48, signedBlockHeader *SignedBeaconBlockHeader, commitmentInclusionProof solid.HashVectorSSZ) *BlobSidecar {
	fmt.Println("NewBlobSidecar:", index, kzgCommitment, kzgProof, signedBlockHeader, commitmentInclusionProof)

	return &BlobSidecar{
		Index:                    index,
		Blob:                     blob,
		KzgCommitment:            kzgCommitment,
		KzgProof:                 kzgProof,
		SignedBlockHeader:        signedBlockHeader,
		CommitmentInclusionProof: commitmentInclusionProof,
	}
}

// EncodeSSZ encodes the BlobSidecar in SSZ format
func (b *BlobSidecar) EncodeSSZ(buf []byte) ([]byte, error) {
	fmt.Println("EncodeSSZ:", b.SignedBlockHeader)
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

// encodingSize
func (b *BlobSidecar) EncodingSizeSSZ() int {
	size := 8 + 4096*32 + 48 + 48
	fmt.Println("EncodingSizeSSZ:", size)
	fmt.Println("EncodingSizeSSZ:", b.SignedBlockHeader)
	fmt.Println("EncodingSizeSSZ:", b.CommitmentInclusionProof)

	if b.SignedBlockHeader != nil {
		fmt.Println("Signed:", b.SignedBlockHeader)
		size += b.SignedBlockHeader.EncodingSizeSSZ()
	}
	if b.CommitmentInclusionProof != nil {
		fmt.Println("Signed:", b.CommitmentInclusionProof)
		size += b.CommitmentInclusionProof.EncodingSizeSSZ()
	}
	return size
}

// DecodeSSZ decodes BlobSidecar in SSZ format.
func (b *BlobSidecar) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.getSchema()...)
}

func (b *BlobSidecar) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema()...)
}

func (b *BlobSidecar) Clone() clonable.Clonable {
	return NewBlobSidecar(b.Index, b.Blob, b.KzgCommitment, b.KzgProof, b.SignedBlockHeader, b.CommitmentInclusionProof)
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

// EncodeSSZ encodes the BlobIdentifier in SSZ format
func (b *BlobIdentifier) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

// encodingSize
func (b *BlobIdentifier) EncodingSizeSSZ() int {
	return 32 + 8
}

// DecodeSSZ decodes BlobIdentifier in SSZ format.
func (b *BlobIdentifier) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.getSchema()...)
}

// HashSSZ encodes the BlobIdentifier in SSZ tree format.
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
