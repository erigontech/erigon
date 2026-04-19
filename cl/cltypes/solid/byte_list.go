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
	"fmt"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
)

// ByteListSSZ is a variable-length SSZ byte list (ByteList[N]) with a
// configurable maximum length. Unlike ExtraData (which is hard-coded to
// 32 bytes max), this type supports arbitrary limits such as
// MAX_BYTES_PER_TRANSACTION (2^30).
type ByteListSSZ struct {
	data []byte
	// limit is the maximum number of bytes allowed (the N in ByteList[N]).
	limit uint64
}

// NewByteListSSZ creates a new empty ByteListSSZ with the given maximum length.
func NewByteListSSZ(limit uint64) *ByteListSSZ {
	return &ByteListSSZ{
		data:  nil,
		limit: limit,
	}
}

func (b *ByteListSSZ) UnmarshalJSON(buf []byte) error {
	var h hexutil.Bytes
	if err := json.Unmarshal(buf, &h); err != nil {
		return err
	}
	if uint64(len(h)) > b.limit {
		return fmt.Errorf("ByteListSSZ: JSON data length %d exceeds limit %d", len(h), b.limit)
	}
	b.data = []byte(h)
	return nil
}

func (b ByteListSSZ) MarshalJSON() ([]byte, error) {
	return json.Marshal(hexutil.Bytes(b.data))
}

// Clone creates a new empty ByteListSSZ with the same limit.
func (b *ByteListSSZ) Clone() clonable.Clonable {
	return NewByteListSSZ(b.limit)
}

// Static returns false — ByteListSSZ is a variable-length type.
func (*ByteListSSZ) Static() bool {
	return false
}

// EncodeSSZ appends the raw bytes to dst.
func (b *ByteListSSZ) EncodeSSZ(dst []byte) ([]byte, error) {
	if uint64(len(b.data)) > b.limit {
		return nil, fmt.Errorf("ByteListSSZ: data length %d exceeds limit %d", len(b.data), b.limit)
	}
	return append(dst, b.data...), nil
}

// EncodingSizeSSZ returns the current length of the byte data.
func (b *ByteListSSZ) EncodingSizeSSZ() int {
	return len(b.data)
}

// DecodeSSZ sets the data from the provided buffer.
func (b *ByteListSSZ) DecodeSSZ(buf []byte, _ int) error {
	if uint64(len(buf)) > b.limit {
		return fmt.Errorf("ByteListSSZ: SSZ data length %d exceeds limit %d", len(buf), b.limit)
	}
	b.data = common.Copy(buf)
	return nil
}

// HashSSZ computes the SSZ hash tree root for this byte list.
// For a ByteList[N], the hash tree root is:
//
//	mix_in_length(merkleize(pack(value), limit=ceil(N/32)), len(value))
func (b *ByteListSSZ) HashSSZ() ([32]byte, error) {
	// Number of 32-byte chunks needed to hold limit bytes.
	chunkLimit := (b.limit + 31) / 32

	// Pack the data into 32-byte chunks and merkleize with the chunk limit.
	leafCount := merkle_tree.NextPowerOfTwo(uint64((len(b.data) + 31) / length.Hash))
	if leafCount == 0 {
		leafCount = 1
	}
	leaves := make([]byte, leafCount*uint64(length.Hash))
	copy(leaves, b.data)

	if err := merkle_tree.MerkleRootFromFlatLeavesWithLimit(leaves, leaves, chunkLimit); err != nil {
		return [32]byte{}, err
	}

	// mix_in_length: hash(root || length)
	var result [32]byte
	mixIn := make([]byte, 64)
	copy(mixIn[:32], leaves[:length.Hash])
	binary.LittleEndian.PutUint64(mixIn[32:], uint64(len(b.data)))
	if err := merkle_tree.HashByteSlice(result[:], mixIn); err != nil {
		return [32]byte{}, err
	}
	return result, nil
}

// Bytes returns a copy of the underlying data.
func (b *ByteListSSZ) Bytes() []byte {
	return common.Copy(b.data)
}

// SetBytes replaces the underlying data with a copy of the provided slice.
// It returns an error if the data exceeds the configured limit.
func (b *ByteListSSZ) SetBytes(buf []byte) error {
	if uint64(len(buf)) > b.limit {
		return fmt.Errorf("ByteListSSZ: data length %d exceeds limit %d", len(buf), b.limit)
	}
	b.data = common.Copy(buf)
	return nil
}

// Len returns the current length of the data.
func (b *ByteListSSZ) Len() int {
	return len(b.data)
}
