package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

/*
 * BeaconBlockHeader is the message we validate in the lightclient.
 * It contains the hash of the block body, and state root data.
 */
type BeaconBlockHeader struct {
	Slot          uint64         `json:"slot"`
	ProposerIndex uint64         `json:"proposer_index"`
	ParentRoot    libcommon.Hash `json:"parent_root"`
	Root          libcommon.Hash `json:"state_root"`
	BodyRoot      libcommon.Hash `json:"body_root"`
}

func (b *BeaconBlockHeader) Copy() *BeaconBlockHeader {
	copied := *b
	return &copied
}
func (b *BeaconBlockHeader) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.Slot, b.ProposerIndex, b.ParentRoot[:], b.Root[:], b.BodyRoot[:])
}

func (b *BeaconBlockHeader) DecodeSSZ(buf []byte, v int) error {
	return ssz2.UnmarshalSSZ(buf, v, &b.Slot, &b.ProposerIndex, b.ParentRoot[:], b.Root[:], b.BodyRoot[:])
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
	Header    *BeaconBlockHeader `json:"message"`
	Signature libcommon.Bytes96  `json:"signature"`
}

func (b *SignedBeaconBlockHeader) Static() bool {
	return true
}

func (b *SignedBeaconBlockHeader) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.Header, b.Signature[:])
}

func (b *SignedBeaconBlockHeader) DecodeSSZ(buf []byte, version int) error {
	b.Header = new(BeaconBlockHeader)
	return ssz2.UnmarshalSSZ(buf, version, b.Header, b.Signature[:])

}

func (b *SignedBeaconBlockHeader) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Header, b.Signature[:])
}

func (b *SignedBeaconBlockHeader) EncodingSizeSSZ() int {
	return b.Header.EncodingSizeSSZ() + 96
}
