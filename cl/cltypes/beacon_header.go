package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common"
)

/*
 * BeaconBlockHeader is the message we validate in the lightclient.
 * It contains the hash of the block body, and state root data.
 */
type BeaconBlockHeader struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    libcommon.Hash
	Root          libcommon.Hash
	BodyRoot      libcommon.Hash
}

func (b *BeaconBlockHeader) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, b.SizeSSZ())
	ssz_utils.MarshalUint64SSZ(buf, b.Slot)
	ssz_utils.MarshalUint64SSZ(buf[8:], b.ProposerIndex)
	copy(buf[16:], b.ParentRoot[:])
	copy(buf[48:], b.Root[:])
	copy(buf[80:], b.BodyRoot[:])
	return buf, nil
}

func (b *BeaconBlockHeader) UnmarshalSSZ(buf []byte) error {
	b.Slot = ssz_utils.UnmarshalUint64SSZ(buf)
	b.ProposerIndex = ssz_utils.UnmarshalUint64SSZ(buf[8:])
	copy(b.ParentRoot[:], buf[16:])
	copy(b.Root[:], buf[48:])
	copy(b.BodyRoot[:], buf[80:])
	return nil
}

func (b *BeaconBlockHeader) HashTreeRoot() ([32]byte, error) {
	return merkle_tree.ArraysRoot([][32]byte{
		merkle_tree.Uint64Root(b.Slot),
		merkle_tree.Uint64Root(b.ProposerIndex),
		b.ParentRoot,
		b.Root,
		b.BodyRoot,
	}, 8)
}

func (b *BeaconBlockHeader) SizeSSZ() int {
	return length.Hash*3 + common.BlockNumberLength*2
}

/*
 * SignedBeaconBlockHeader is a beacon block header + validator signature.
 */
type SignedBeaconBlockHeader struct {
	Header    *BeaconBlockHeader
	Signature [96]byte
}

func (b *SignedBeaconBlockHeader) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, b.SizeSSZ())
	marshalledHeader, err := b.Header.MarshalSSZ()
	if err != nil {
		return nil, err
	}

	copy(buf, marshalledHeader)
	copy(buf[b.Header.SizeSSZ():], b.Signature[:])
	return buf, nil
}

func (b *SignedBeaconBlockHeader) UnmarshalSSZ(buf []byte) error {
	if err := b.Header.UnmarshalSSZ(buf); err != nil {
		return err
	}
	copy(b.Signature[:], buf[b.Header.SizeSSZ():])
	return nil
}

func (b *SignedBeaconBlockHeader) HashTreeRoot() ([32]byte, error) {
	signatureRoot, err := merkle_tree.SignatureRoot(b.Signature)
	if err != nil {
		return [32]byte{}, err
	}

	headerRoot, err := b.Header.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ArraysRoot([][32]byte{
		headerRoot,
		signatureRoot,
	}, 8)
}

func (b *SignedBeaconBlockHeader) SizeSSZ() int {
	return b.Header.SizeSSZ() + 96
}
