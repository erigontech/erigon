package cltypes

import (
	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type Root [32]byte
type Slot uint64
type Blob gokzg4844.Blob
type KZGCommitment gokzg4844.KZGCommitment // [48]byte
type KZGProof gokzg4844.KZGProof           // [48]byte

type BlobSideCar struct {
	BlockRoot       Root
	Index           uint64 // index of blob in block
	Slot            Slot
	BlockParentRoot Root
	ProposerIndex   uint64 // validator index
	Blob            Blob
	KZGCommitment   KZGCommitment
	KZGProof        KZGProof
}

// https://github.com/ethereum/consensus-specs/blob/3a2304981a3b820a22b518fe4859f4bba0ebc83b/specs/deneb/polynomial-commitments.md#custom-types
const BYTES_PER_FIELD_ELEMENT = 32
const FIELD_ELEMENTS_PER_BLOB = 4096
const BYTES_PER_BLOB = uint64(BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_BLOB)

func (b *BlobSideCar) Copy() *BlobSideCar {
	copied := *b
	return &copied
}

func (b *BlobSideCar) EncodeSSZ(buf []byte) ([]byte, error) {
	buf = append(buf, b.BlockRoot[:]...)
	buf = append(buf, ssz.Uint64SSZ(b.Index)...)
	buf = append(buf, ssz.Uint64SSZ(uint64(b.Slot))...)
	buf = append(buf, b.BlockParentRoot[:]...)
	buf = append(buf, ssz.Uint64SSZ(b.ProposerIndex)...)
	buf = append(buf, b.Blob[:]...)
	buf = append(buf, b.KZGCommitment[:]...)
	buf = append(buf, b.KZGProof[:]...)

	return buf, nil
}

func (b *BlobSideCar) DecodeSSZ(buf []byte) error {
	pos := 0 // current position at the buffer

	copy(b.BlockRoot[:], buf[pos:32])
	pos += 32

	b.Index = ssz.UnmarshalUint64SSZ(buf[pos : pos+8])
	pos += 8

	b.Slot = Slot(ssz.UnmarshalUint64SSZ(buf[pos:]))
	pos += 8

	copy(b.BlockParentRoot[:], buf[pos:pos+32])
	pos += 32

	b.ProposerIndex = ssz.UnmarshalUint64SSZ(buf[pos : pos+8])
	pos += 8

	copy(b.Blob[:], buf[pos:pos+int(BYTES_PER_BLOB)])
	pos += int(BYTES_PER_BLOB)

	copy(b.KZGCommitment[:], buf[pos:pos+48])
	pos += 48

	copy(b.KZGProof[:], buf[pos:pos+48])

	return nil
}

func (b *BlobSideCar) EncodingSizeSSZ() int {
	return 131_256
}

func (b *BlobSideCar) HashSSZ() (libcommon.Hash, error) {
	KZGCommitmentLeave, err := merkle_tree.PublicKeyRoot(b.KZGCommitment)
	if err != nil {
		return libcommon.Hash{}, err
	}
	KZGProofLeave, err := merkle_tree.PublicKeyRoot(b.KZGProof)
	if err != nil {
		return libcommon.Hash{}, err
	}

	blobLeave := [][32]byte{}
	previous_pos := 0
	for pos := 32; pos < int(BYTES_PER_BLOB)/32; pos += 32 {
		blobLeave = append(blobLeave, libcommon.BytesToHash(b.Blob[previous_pos:pos]))
		previous_pos = pos
	}

	blobRoot, err := merkle_tree.ArraysRoot(blobLeave, 4096)
	if err != nil {
		return libcommon.Hash{}, err
	}

	return merkle_tree.ArraysRoot([][32]byte{
		b.BlockRoot,
		merkle_tree.Uint64Root(b.Index),
		merkle_tree.Uint64Root(uint64(b.Slot)),
		b.BlockParentRoot,
		merkle_tree.Uint64Root(b.ProposerIndex),
		blobRoot,
		KZGCommitmentLeave,
		KZGProofLeave,
	}, 8)
}

type SignedBlobSideCar struct {
	Message   BlobSideCar
	Signature [96]byte
}

func (b *SignedBlobSideCar) Copy() *SignedBlobSideCar {
	copy := *b
	return &copy
}

func (b *SignedBlobSideCar) EncodeSSZ(buf []byte) ([]byte, error) {
	encodedMessage, _ := b.Message.EncodeSSZ([]byte{})

	buf = append(buf, encodedMessage...)
	buf = append(buf, b.Signature[:]...)

	return buf, nil
}

func (b *SignedBlobSideCar) DecodeSSZ(buf []byte) error {
	pos := b.Message.EncodingSizeSSZ()
	err := b.Message.DecodeSSZ(buf[:pos])
	if err != nil {
		return err
	}
	copy(b.Signature[:], buf[pos:])

	return nil
}

func (b *SignedBlobSideCar) EncodingSizeSSZ() int {
	return b.Message.EncodingSizeSSZ() + 96
}

func (b *SignedBlobSideCar) HashSSZ() (libcommon.Hash, error) {
	messageLeave, err := b.Message.HashSSZ()
	if err != nil {
		return libcommon.Hash{}, err
	}

	signatureLeave, err := merkle_tree.SignatureRoot(b.Signature)
	if err != nil {
		return libcommon.Hash{}, err
	}

	return merkle_tree.ArraysRoot([][32]byte{
		messageLeave,
		signatureLeave,
	}, 2)
}

type BlobIdentifier struct {
	BlockRoot Root
	Index     uint64
}

func (b *BlobIdentifier) Copy() *BlobIdentifier {
	copy := *b
	return &copy
}

func (b *BlobIdentifier) EncodeSSZ(buf []byte) ([]byte, error) {
	buf = append(buf, b.BlockRoot[:]...)
	buf = append(buf, ssz.Uint64SSZ(b.Index)...)

	return buf, nil
}

func (b *BlobIdentifier) DecodeSSZ(buf []byte) error {
	copy(b.BlockRoot[:], buf[:32])
	b.Index = ssz.UnmarshalUint64SSZ(buf[32:])
	return nil
}

func (b *BlobIdentifier) EncodingSizeSSZ() int {
	return 40
}

func (b *BlobIdentifier) HashSSZ() (libcommon.Hash, error) {
	return merkle_tree.ArraysRoot([][32]byte{
		b.BlockRoot,
		merkle_tree.Uint64Root(b.Index),
	}, 2)
}
