package cltypes

import (
	"fmt"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type Blob gokzg4844.Blob
type KZGProof gokzg4844.KZGProof // [48]byte

type BlobSideCar struct {
	BlockRoot       libcommon.Hash
	Index           uint64 // index of blob in block
	Slot            uint64
	BlockParentRoot libcommon.Hash
	ProposerIndex   uint64 // validator index
	Blob            *Blob
	KZGCommitment   KZGCommitment
	KZGProof        KZGProof
}

// https://github.com/ethereum/consensus-specs/blob/3a2304981a3b820a22b518fe4859f4bba0ebc83b/specs/deneb/polynomial-commitments.md#custom-types
const BYTES_PER_FIELD_ELEMENT = 32
const FIELD_ELEMENTS_PER_BLOB = 4096
const BYTES_PER_BLOB = uint64(BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_BLOB)

type KZGCommitment gokzg4844.KZGCommitment

func (b *KZGCommitment) Copy() *KZGCommitment {
	copy := *b
	return &copy
}

func (b *KZGCommitment) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, b[:]...), nil
}

func (b *KZGCommitment) DecodeSSZ(buf []byte, version int) error {
	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[KZGCommitment] err: %w", ssz.ErrLowBufferSize)
	}
	copy(b[:], buf)

	return nil
}

func (b *KZGCommitment) EncodingSizeSSZ() int {
	return 48
}

func (b *KZGCommitment) HashSSZ() ([32]byte, error) {
	return merkle_tree.BytesRoot(b[:])
}
