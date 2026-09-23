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

package ssz

import (
	"errors"
	"testing"

	"github.com/erigontech/erigon/common/clonable"
	"github.com/stretchr/testify/require"
)

type strictListTestElement struct{}

func (*strictListTestElement) EncodeSSZ(dst []byte) ([]byte, error) {
	return dst, nil
}

func (*strictListTestElement) EncodingSizeSSZ() int {
	return 0
}

func (*strictListTestElement) DecodeSSZ([]byte, int) error {
	return nil
}

func (*strictListTestElement) Clone() clonable.Clonable {
	return &strictListTestElement{}
}

var errStrictListTest = errors.New("strict list decode")

type strictRejectingListTestElement struct{}

func (*strictRejectingListTestElement) DecodeSSZ([]byte, int) error {
	return nil
}

func (*strictRejectingListTestElement) DecodeSSZStrict([]byte, int) error {
	return errStrictListTest
}

func (*strictRejectingListTestElement) Clone() clonable.Clonable {
	return &strictRejectingListTestElement{}
}

func TestDecodeDynamicListStrictRejectsNonCanonicalOffsets(t *testing.T) {
	tests := []struct {
		name    string
		encoded []byte
		wantErr error
	}{
		{name: "short offset", encoded: []byte{4}, wantErr: ErrLowBufferSize},
		{name: "zero offset", encoded: []byte{0, 0, 0, 0}, wantErr: ErrBadOffset},
		{name: "unaligned offset", encoded: []byte{5, 0, 0, 0, 0}, wantErr: ErrBadOffset},
		{name: "offset past end", encoded: []byte{8, 0, 0, 0}, wantErr: ErrBadOffset},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeDynamicListStrict[*strictListTestElement](
				test.encoded,
				0,
				uint32(len(test.encoded)),
				1,
				0,
			)
			require.ErrorIs(t, err, test.wantErr)
		})
	}
}

func TestDecodeDynamicListStrictPropagatesToElements(t *testing.T) {
	encoded := []byte{4, 0, 0, 0}

	_, err := DecodeDynamicList[*strictRejectingListTestElement](encoded, 0, uint32(len(encoded)), 1, 0)
	require.NoError(t, err)
	_, err = DecodeDynamicListStrict[*strictRejectingListTestElement](encoded, 0, uint32(len(encoded)), 1, 0)
	require.ErrorIs(t, err, errStrictListTest)
}

func TestDynamicListSingleEmptyElementRoundTrip(t *testing.T) {
	encoded, err := EncodeDynamicList(nil, []*strictListTestElement{{}})
	require.NoError(t, err)
	require.Equal(t, []byte{4, 0, 0, 0}, encoded)

	tests := []struct {
		name   string
		decode func([]byte) ([]*strictListTestElement, error)
	}{
		{
			name: "permissive",
			decode: func(buf []byte) ([]*strictListTestElement, error) {
				return DecodeDynamicList[*strictListTestElement](buf, 0, uint32(len(buf)), 1, 0)
			},
		},
		{
			name: "strict",
			decode: func(buf []byte) ([]*strictListTestElement, error) {
				return DecodeDynamicListStrict[*strictListTestElement](buf, 0, uint32(len(buf)), 1, 0)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoded, err := test.decode(encoded)
			require.NoError(t, err)
			require.Len(t, decoded, 1)

			roundTripped, err := EncodeDynamicList(nil, decoded)
			require.NoError(t, err)
			require.Equal(t, encoded, roundTripped)
		})
	}
}
