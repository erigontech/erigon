package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
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

func (b *BeaconBlockHeader) Copy() *BeaconBlockHeader {
	copied := *b
	return &copied
}
func (b *BeaconBlockHeader) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, ssz.Uint64SSZ(b.Slot)...)
	buf = append(buf, ssz.Uint64SSZ(b.ProposerIndex)...)
	buf = append(buf, b.ParentRoot[:]...)
	buf = append(buf, b.Root[:]...)
	buf = append(buf, b.BodyRoot[:]...)
	return buf, nil
}

func (b *BeaconBlockHeader) DecodeSSZ(buf []byte, _ int) error {
	b.Slot = ssz.UnmarshalUint64SSZ(buf)
	b.ProposerIndex = ssz.UnmarshalUint64SSZ(buf[8:])
	copy(b.ParentRoot[:], buf[16:])
	copy(b.Root[:], buf[48:])
	copy(b.BodyRoot[:], buf[80:])
	return nil
}

func (b *BeaconBlockHeader) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Slot, b.ProposerIndex, b.ParentRoot[:], b.Root[:], b.BodyRoot[:])

}

func (b *BeaconBlockHeader) EncodingSizeSSZ() int {
	return length.Hash*3 + length.BlockNum*2
}

/*
 * SignedBeaconBlockHeader is a beacon block header + validator signature.
 */
type SignedBeaconBlockHeader struct {
	Header    *BeaconBlockHeader
	Signature [96]byte
}

func (b *SignedBeaconBlockHeader) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	var err error
	buf, err = b.Header.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}
	buf = append(buf, b.Signature[:]...)
	return buf, nil
}

func (b *SignedBeaconBlockHeader) DecodeSSZ(buf []byte, version int) error {
	b.Header = new(BeaconBlockHeader)
	if err := b.Header.DecodeSSZ(buf, version); err != nil {
		return err
	}
	copy(b.Signature[:], buf[b.Header.EncodingSizeSSZ():])
	return nil
}

func (b *SignedBeaconBlockHeader) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Header, b.Signature[:])
}

func (b *SignedBeaconBlockHeader) EncodingSizeSSZ() int {
	return b.Header.EncodingSizeSSZ() + 96
}
