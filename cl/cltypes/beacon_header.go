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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"

	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

/*
 * BeaconBlockHeader is the message we validate in the lightclient.
 * It contains the hash of the block body, and state root data.
 */
type BeaconBlockHeader struct {
	Slot          uint64      `json:"slot,string"`
	ProposerIndex uint64      `json:"proposer_index,string"`
	ParentRoot    common.Hash `json:"parent_root"`
	Root          common.Hash `json:"state_root"`
	BodyRoot      common.Hash `json:"body_root"`
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
	Signature common.Bytes96     `json:"signature"`
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
