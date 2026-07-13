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

package base_encoding

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test64(t *testing.T) {
	number := uint64(9992)

	out := Encode64ToBytes4(number)
	require.Equal(t, Decode64FromBytes4(out), number)

	out = EncodeCompactUint64(number)
	require.Equal(t, DecodeCompactUint64(out), number)
}

func TestDiff64(t *testing.T) {
	old := make([]byte, 800000)
	new := make([]byte, 800008)
	inc := 1
	for i := 0; i < 80; i++ {
		if i%9 == 0 {
			inc++
		}
		old[i] = byte(i)
		new[i] = byte(i + inc)
	}

	var b bytes.Buffer

	err := ComputeCompressedSerializedUint64ListDiff(&b, old, new)
	require.NoError(t, err)

	out := b.Bytes()
	new2, err := ApplyCompressedSerializedUint64ListDiff(old, nil, out, false)
	require.NoError(t, err)

	require.Equal(t, new, new2)

	new3, err := ApplyCompressedSerializedUint64ListDiff(new2, nil, out, true)
	require.NoError(t, err)

	require.Equal(t, old, new3[:len(old)])
}

func TestDiff64Effective(t *testing.T) {
	sizeOld := 800
	sizeNew := 816
	old := make([]byte, sizeOld*121)
	new := make([]byte, sizeNew*121)
	previous := make([]byte, sizeOld*8)
	expected := make([]byte, sizeNew*8)
	for i := 0; i < sizeNew; i++ {
		validatorOffset := i * 121
		newNum := i + 32
		oldNum := i + 12
		binary.LittleEndian.PutUint64(expected[i*8:], uint64(newNum))
		binary.LittleEndian.PutUint64(new[validatorOffset+80:], uint64(newNum))
		if i < len(old)/121 {
			binary.LittleEndian.PutUint64(previous[i*8:], uint64(oldNum))
			binary.LittleEndian.PutUint64(old[validatorOffset+80:], uint64(oldNum))
		}
	}

	require.Equal(t, previous, AppendEffectiveBalances(nil, old))
	require.Equal(t, expected, AppendEffectiveBalances(nil, new))

	var b bytes.Buffer

	err := ComputeCompressedSerializedUint64ListDiff(&b, AppendEffectiveBalances(nil, old), AppendEffectiveBalances(nil, new))
	require.NoError(t, err)

	out := b.Bytes()
	new2, err := ApplyCompressedSerializedUint64ListDiff(previous, nil, out, false)
	require.NoError(t, err)

	require.Equal(t, expected, new2)
}

func TestAppendEffectiveBalances(t *testing.T) {
	const vSize = 121
	require.Empty(t, AppendEffectiveBalances(nil, nil))

	for _, validators := range []int{1, 800} {
		ssz := make([]byte, validators*vSize)
		expected := make([]byte, validators*8)
		for i := range ssz {
			ssz[i] = byte(i*7 + 1) // noise in non-effective-balance fields
		}
		for i := 0; i < validators; i++ {
			binary.LittleEndian.PutUint64(ssz[i*vSize+80:], uint64(i+32))
			binary.LittleEndian.PutUint64(expected[i*8:], uint64(i+32))
		}

		require.Equal(t, expected, AppendEffectiveBalances(nil, ssz))
	}

	// a truncated trailing record (past the effective-balance offset) must be ignored
	twoValidators := make([]byte, 2*vSize)
	binary.LittleEndian.PutUint64(twoValidators[80:], 111)
	binary.LittleEndian.PutUint64(twoValidators[vSize+80:], 222)
	withPartialTail := append(twoValidators, make([]byte, 90)...)
	expected := make([]byte, 16)
	binary.LittleEndian.PutUint64(expected[0:], 111)
	binary.LittleEndian.PutUint64(expected[8:], 222)
	require.Equal(t, expected, AppendEffectiveBalances(nil, withPartialTail))
}

func TestDiffValidators(t *testing.T) {
	vals := 3
	old := make([]byte, vals*121)
	new := make([]byte, 121*(vals+1))
	inc := 1
	for i := 0; i < vals*121; i++ {
		if i%9 == 0 {
			inc++
		}
		old[i] = byte(i)
		new[i] = byte(i + inc)
	}

	var b bytes.Buffer

	err := ComputeCompressedSerializedValidatorSetListDiff(&b, old, new)
	require.NoError(t, err)

	out := b.Bytes()
	new2, err := ApplyCompressedSerializedValidatorListDiff(old, nil, out, false)
	require.NoError(t, err)

	require.Equal(t, new, new2)
}
