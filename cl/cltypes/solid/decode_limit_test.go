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

package solid_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

func TestValidatorSetDecodeSSZRejectsOverLimit(t *testing.T) {
	validator := []byte(solid.NewValidator())
	encoded := append(append(append([]byte{}, validator...), validator...), validator...)

	set := solid.NewValidatorSet(2)
	require.Error(t, set.DecodeSSZ(encoded, 0))
}

func TestValidatorSetDecodeSSZAcceptsLimit(t *testing.T) {
	validator := []byte(solid.NewValidator())
	encoded := append(append([]byte{}, validator...), validator...)

	set := solid.NewValidatorSet(2)
	require.NoError(t, set.DecodeSSZ(encoded, 0))
	require.Equal(t, 2, set.Length())
}

func TestHashListDecodeSSZRejectsOverLimit(t *testing.T) {
	list := solid.NewHashList(2)
	require.Error(t, list.DecodeSSZ(make([]byte, 3*length.Hash), 0))
}

func TestHashListDecodeSSZAcceptsLimit(t *testing.T) {
	list := solid.NewHashList(2)
	require.NoError(t, list.DecodeSSZ(make([]byte, 2*length.Hash), 0))
	require.Equal(t, 2, list.Length())
}

func TestUint64ListDecodeSSZRejectsOverLimit(t *testing.T) {
	list := solid.NewUint64ListSSZ(2)
	require.Error(t, list.DecodeSSZ(make([]byte, 3*8), 0))
}

func TestUint64ListDecodeSSZAcceptsLimit(t *testing.T) {
	list := solid.NewUint64ListSSZ(2)
	require.NoError(t, list.DecodeSSZ(make([]byte, 2*8), 0))
	require.Equal(t, 2, list.Length())
}

func TestHashListHashSSZRejectsOverLimit(t *testing.T) {
	list := solid.NewHashList(2)
	for i := range 3 {
		list.Append(common.Hash{byte(i)})
	}
	require.NotPanics(t, func() {
		_, err := list.HashSSZ()
		require.Error(t, err)
	})
}

func TestValidatorSetHashSSZRejectsOverLimit(t *testing.T) {
	set := solid.NewValidatorSet(2)
	for range 3 {
		set.Append(solid.NewValidator())
	}
	require.NotPanics(t, func() {
		_, err := set.HashSSZ()
		require.Error(t, err)
	})
}

func TestUint64ListHashSSZRejectsOverLimit(t *testing.T) {
	list := solid.NewUint64ListSSZ(2)
	for i := range 3 {
		list.Append(uint64(i))
	}
	require.NotPanics(t, func() {
		_, err := list.HashSSZ()
		require.Error(t, err)
	})
}
