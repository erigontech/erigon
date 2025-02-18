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
	"encoding/binary"
	"encoding/json"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

// ExtraData type stores data as a byte slice and its length.
type ExtraData struct {
	data []byte
	l    int
}

// NewExtraData creates a new instance of ExtraData type with initialized byte slice of length 32.
func NewExtraData() *ExtraData {
	return &ExtraData{
		data: make([]byte, 32),
	}
}

func (e *ExtraData) UnmarshalJSON(buf []byte) error {
	if err := json.Unmarshal(buf, (*hexutil.Bytes)(&e.data)); err != nil {
		return err
	}
	e.l = len(e.data)
	return nil
}

func (e ExtraData) MarshalJSON() ([]byte, error) {
	return json.Marshal(hexutil.Bytes(e.data[:e.l]))
}

// Clone creates a new instance of ExtraData.
func (*ExtraData) Clone() clonable.Clonable {
	return NewExtraData()
}

// Static always returns false, indicating that the ExtraData object is not static.
func (*ExtraData) Static() bool {
	return false
}

// EncodeSSZ appends ExtraData bytes to the provided buffer.
func (e *ExtraData) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, e.Bytes()...), nil
}

// EncodingSizeSSZ returns the length of ExtraData.
func (e *ExtraData) EncodingSizeSSZ() int {
	return e.l
}

// HashSSZ returns the Merkle Root of the ExtraData byte slice.
func (e *ExtraData) HashSSZ() ([32]byte, error) {
	leaves := make([]byte, length.Hash*2)
	copy(leaves, e.data[:e.l])
	binary.LittleEndian.PutUint64(leaves[length.Hash:], uint64(e.l))
	if err := merkle_tree.MerkleRootFromFlatLeaves(leaves, leaves); err != nil {
		return [32]byte{}, err
	}
	return common.BytesToHash(leaves[:length.Hash]), nil
}

// DecodeSSZ sets the ExtraData bytes from the provided buffer.
func (e *ExtraData) DecodeSSZ(buf []byte, _ int) error {
	e.SetBytes(buf)
	return nil
}

// Bytes returns a copy of the ExtraData bytes.
func (e *ExtraData) Bytes() []byte {
	return common.Copy(e.data[:e.l])
}

// SetBytes sets the ExtraData bytes from the provided byte slice.
func (e *ExtraData) SetBytes(buf []byte) {
	copy(e.data, buf)
	e.l = len(buf)
	if e.l > 32 {
		e.l = len(e.data)
	}
}
