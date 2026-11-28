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

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func Test64(t *testing.T) {
	number := uint64(9992)

	out := Encode64ToBytes4(number)
	require.Equal(t, Decode64FromBytes4(out), number)
}

func TestDiff64(t *testing.T) {
	old := make([]byte, 800000)
	new := make([]byte, 800008)
	inc := 1
	for i := range 80 {
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

func validatorSetWithEffectiveBalances(t *testing.T, balances []uint64) *solid.ValidatorSet {
	t.Helper()
	set := solid.NewValidatorSet(len(balances))
	for _, balance := range balances {
		v := solid.NewValidator()
		v.SetPublicKey([48]byte{1, 2, 3})
		v.SetEffectiveBalance(balance)
		set.Append(v)
	}
	return set
}

func TestDiff64Effective(t *testing.T) {
	sizeOld := 800
	sizeNew := 816
	oldBalances := make([]uint64, sizeOld)
	newBalances := make([]uint64, sizeNew)
	previous := make([]byte, sizeOld*8)
	expected := make([]byte, sizeNew*8)
	for i := range sizeNew {
		newNum := i + 32
		oldNum := i + 12
		binary.LittleEndian.PutUint64(expected[i*8:], uint64(newNum))
		newBalances[i] = uint64(newNum)
		if i < sizeOld {
			binary.LittleEndian.PutUint64(previous[i*8:], uint64(oldNum))
			oldBalances[i] = uint64(oldNum)
		}
	}
	old := validatorSetWithEffectiveBalances(t, oldBalances)
	new := validatorSetWithEffectiveBalances(t, newBalances)

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
	require.Empty(t, AppendEffectiveBalances(nil, solid.NewValidatorSet(0)))

	const validators = 800
	balances := make([]uint64, validators)
	packed := make([]byte, validators*8)
	for i := range validators {
		balances[i] = uint64(i + 32)
		binary.LittleEndian.PutUint64(packed[i*8:], uint64(i+32))
	}
	require.Equal(t, packed, AppendEffectiveBalances(nil, validatorSetWithEffectiveBalances(t, balances)))
}
