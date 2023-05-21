package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
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
	return ssz2.Encode(dst, b.Slot, b.ProposerIndex, b.ParentRoot[:], b.Root[:], b.BodyRoot[:])
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

func (*BeaconBlockHeader) Static() bool {
	return true
}

/*
 * SignedBeaconBlockHeader is a beacon block header + validator signature.
 */
type SignedBeaconBlockHeader struct {
	Header    *BeaconBlockHeader
	Signature [96]byte
}

func (b *SignedBeaconBlockHeader) Static() bool {
	return true
}

func (b *SignedBeaconBlockHeader) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.Encode(dst, b.Header, b.Signature[:])
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
