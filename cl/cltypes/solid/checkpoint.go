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

package solid

import (
	"bytes"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

const CheckpointSizeSSZ = 40

type Checkpoint struct {
	Epoch uint64      `json:"epoch,string"`
	Root  common.Hash `json:"root"`
}

// EncodingSizeSSZ returns the size of the Checkpoint object when encoded as SSZ.
func (*Checkpoint) EncodingSizeSSZ() int {
	return CheckpointSizeSSZ
}

// DecodeSSZ decodes the Checkpoint object from SSZ-encoded data.
func (c *Checkpoint) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &c.Epoch, c.Root[:])
}

// EncodeSSZ encodes the Checkpoint object into SSZ format.
func (c *Checkpoint) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, c.Epoch, c.Root[:])
}

// Clone returns a new Checkpoint object that is a copy of the current object.
func (c *Checkpoint) Clone() clonable.Clonable {
	return &Checkpoint{}
}

// Equal checks if the Checkpoint object is equal to another Checkpoint object.
func (c *Checkpoint) Equal(other Checkpoint) bool {
	return c.Epoch == other.Epoch && bytes.Equal(c.Root[:], other.Root[:])
}

// Copy returns a copy of the Checkpoint object.
func (c *Checkpoint) Copy() *Checkpoint {
	return &Checkpoint{
		Epoch: c.Epoch,
		Root:  c.Root,
	}
}

// HashSSZ returns the hash of the Checkpoint object when encoded as SSZ.
func (c Checkpoint) HashSSZ() (o [32]byte, err error) {
	return merkle_tree.HashTreeRoot(c.Epoch, c.Root[:])
}

// Static always returns true, indicating that the Checkpoint object is static.
func (c Checkpoint) Static() bool {
	return true
}
