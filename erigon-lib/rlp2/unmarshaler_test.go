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

package rlp_test

import (
	"testing"

	rlp "github.com/erigontech/erigon/erigon-lib/rlp2"
	"github.com/stretchr/testify/require"
)

type plusOne int

func (p *plusOne) UnmarshalRLP(data []byte) error {
	var s int
	err := rlp.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	(*p) = plusOne(s + 1)
	return nil
}

func TestDecoder(t *testing.T) {

	type simple struct {
		Key   string
		Value string
	}

	t.Run("ShortString", func(t *testing.T) {
		t.Run("ToString", func(t *testing.T) {
			bts := []byte{0x83, 'd', 'o', 'g'}
			var s string
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, "dog", s)
		})
		t.Run("ToBytes", func(t *testing.T) {
			bts := []byte{0x83, 'd', 'o', 'g'}
			var s []byte
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, []byte("dog"), s)
		})
		t.Run("ToInt", func(t *testing.T) {
			bts := []byte{0x82, 0x04, 0x00}
			var s int
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, 1024, s)
		})
		t.Run("ToIntUnmarshaler", func(t *testing.T) {
			bts := []byte{0x82, 0x04, 0x00}
			var s plusOne
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, plusOne(1025), s)
		})
		t.Run("ToSimpleStruct", func(t *testing.T) {
			bts := []byte{0xc8, 0x83, 'c', 'a', 't', 0x83, 'd', 'o', 'g'}
			var s simple
			err := rlp.Unmarshal(bts, &s)
			require.NoError(t, err)
			require.EqualValues(t, simple{Key: "cat", Value: "dog"}, s)
		})
	})
}
