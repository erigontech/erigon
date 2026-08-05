// Copyright 2026 The Erigon Authors
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

package ssz2_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common/clonable"
	commonssz "github.com/erigontech/erigon/common/ssz"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalSSZStrictRejectsNonCanonicalOffset(t *testing.T) {
	malformed := []byte{5, 0, 0, 0, 0, 1}

	require.NoError(t, ssz2.UnmarshalSSZ(malformed, 0, solid.NewByteListSSZ(2)))
	require.ErrorIs(t, ssz2.UnmarshalSSZStrict(malformed, 0, solid.NewByteListSSZ(2)), commonssz.ErrBadOffset)
}

func TestUnmarshalSSZStrictRejectsDescendingOffsets(t *testing.T) {
	malformed := []byte{8, 0, 0, 0, 7, 0, 0, 0}

	require.ErrorIs(
		t,
		ssz2.UnmarshalSSZStrict(malformed, 0, solid.NewByteListSSZ(1), solid.NewByteListSSZ(1)),
		commonssz.ErrBadOffset,
	)
}

func TestUnmarshalSSZStrictRejectsTrailingBytes(t *testing.T) {
	var x uint64
	buf := make([]byte, 9)

	require.NoError(t, ssz2.UnmarshalSSZ(buf, 0, &x))
	require.ErrorIs(t, ssz2.UnmarshalSSZStrict(buf, 0, &x), commonssz.ErrTrailingBytes)
}

// staticStubSSZ is a fixed-size schema field whose strict decoding fails
// unless it receives exactly its own encoding.
type staticStubSSZ struct{}

func (*staticStubSSZ) Static() bool         { return true }
func (*staticStubSSZ) EncodingSizeSSZ() int { return 4 }

func (*staticStubSSZ) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, 0, 0, 0, 0), nil
}

func (*staticStubSSZ) DecodeSSZ([]byte, int) error { return nil }

func (*staticStubSSZ) DecodeSSZStrict(buf []byte, _ int) error {
	if len(buf) != 4 {
		return fmt.Errorf("static stub got %d bytes, want exactly 4", len(buf))
	}
	return nil
}

func (*staticStubSSZ) Clone() clonable.Clonable { return &staticStubSSZ{} }

func TestUnmarshalSSZStrictGivesStaticFieldsExactRanges(t *testing.T) {
	var tail uint64
	buf := make([]byte, 12)

	require.NoError(t, ssz2.UnmarshalSSZStrict(buf, 0, &staticStubSSZ{}, &tail))
}

func TestUnmarshalSSZStrictPropagatesToNestedList(t *testing.T) {
	list := solid.NewDynamicListSSZ[solid.Validator](1)
	list.Append(solid.NewValidator())
	encoded, err := list.EncodeSSZ(nil)
	require.NoError(t, err)

	nested := make([]byte, len(encoded)+1)
	copy(nested[:4], encoded[:4])
	copy(nested[5:], encoded[4:])
	binary.LittleEndian.PutUint32(nested, 5)

	malformed := make([]byte, 4, 4+len(nested))
	binary.LittleEndian.PutUint32(malformed, 4)
	malformed = append(malformed, nested...)

	require.NoError(t, ssz2.UnmarshalSSZ(malformed, 0, solid.NewDynamicListSSZ[solid.Validator](1)))
	require.ErrorIs(
		t,
		ssz2.UnmarshalSSZStrict(malformed, 0, solid.NewDynamicListSSZ[solid.Validator](1)),
		commonssz.ErrBadOffset,
	)
}
