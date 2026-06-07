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

package accounts

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestAddress_NilZeroValue(t *testing.T) {
	t.Parallel()
	require.True(t, NilAddress.IsNil())
	require.True(t, NilAddress.IsZero())
	require.Equal(t, common.Address{}, NilAddress.Value())
	require.Equal(t, "<nil>", NilAddress.String())

	require.False(t, ZeroAddress.IsNil())
	require.True(t, ZeroAddress.IsZero())

	a := InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000ff"))
	require.False(t, a.IsNil())
	require.False(t, a.IsZero())
	require.Equal(t, common.HexToAddress("0x00000000000000000000000000000000000000ff"), a.Value())
	require.Equal(t, a.Value(), a.Handle().Value())
	require.Equal(t, a.Value().String(), a.String())
}

func TestAddress_TextRoundTrip(t *testing.T) {
	t.Parallel()
	orig := common.HexToAddress("0x00000000000000000000000000000000000000ff")
	a := InternAddress(orig)

	text, err := a.MarshalText()
	require.NoError(t, err)

	var got Address
	require.NoError(t, got.UnmarshalText(text))
	require.Equal(t, a, got)

	var fromJSON Address
	require.NoError(t, fromJSON.UnmarshalJSON([]byte(`"`+orig.Hex()+`"`)))
	require.Equal(t, a, fromJSON)
}

func TestAddress_MarshalTextNil(t *testing.T) {
	t.Parallel()
	text, err := NilAddress.MarshalText()
	require.NoError(t, err)
	require.Nil(t, text)
}

func TestAddress_UnmarshalErrors(t *testing.T) {
	t.Parallel()
	var a Address
	require.Error(t, a.UnmarshalText([]byte("not-hex")))
	require.Error(t, a.UnmarshalJSON([]byte("not-json")))
}

func TestAddress_Cmp(t *testing.T) {
	t.Parallel()
	lo := InternAddress(common.HexToAddress("0x01"))
	hi := InternAddress(common.HexToAddress("0x02"))

	require.Equal(t, 0, NilAddress.Cmp(NilAddress)) //nolint:gocritic // intentional self-comparison
	require.Equal(t, -1, NilAddress.Cmp(lo))
	require.Equal(t, +1, lo.Cmp(NilAddress))
	require.Equal(t, -1, lo.Cmp(hi))
	require.Equal(t, +1, hi.Cmp(lo))
	require.Equal(t, 0, lo.Cmp(lo)) //nolint:gocritic // intentional self-comparison
}

func TestAddress_Format(t *testing.T) {
	t.Parallel()
	require.Equal(t, "<nil>", fmt.Sprintf("%x", NilAddress))
	require.NotEqual(t, "<nil>", fmt.Sprintf("%x", ZeroAddress))
}

func TestStorageKey_NilZeroValue(t *testing.T) {
	t.Parallel()
	require.True(t, NilKey.IsNil())
	require.Equal(t, common.Hash{}, NilKey.Value())
	require.Equal(t, "<nil>", NilKey.String())

	k := InternKey(common.HexToHash("0xabc"))
	require.False(t, k.IsNil())
	require.Equal(t, common.HexToHash("0xabc"), k.Value())
	require.NotEqual(t, "<nil>", k.String())
}

func TestStorageKey_Cmp(t *testing.T) {
	t.Parallel()
	lo := InternKey(common.HexToHash("0x01"))
	hi := InternKey(common.HexToHash("0x02"))

	require.Equal(t, 0, NilKey.Cmp(NilKey)) //nolint:gocritic // intentional self-comparison
	require.Equal(t, -1, NilKey.Cmp(lo))
	require.Equal(t, +1, lo.Cmp(NilKey))
	require.Equal(t, -1, lo.Cmp(hi))
	require.Equal(t, +1, hi.Cmp(lo))
}

func TestStorageKey_Format(t *testing.T) {
	t.Parallel()
	require.Equal(t, "<nil>", fmt.Sprintf("%x", NilKey))
	require.NotEqual(t, "<nil>", fmt.Sprintf("%x", ZeroKey))
}

func TestCodeHash_EmptyNilZero(t *testing.T) {
	t.Parallel()
	require.True(t, NilCodeHash.IsNil())
	require.True(t, NilCodeHash.IsEmpty())
	require.True(t, NilCodeHash.IsZero())
	require.Equal(t, common.Hash{}, NilCodeHash.Value())
	require.Equal(t, "<nil>", NilCodeHash.String())

	require.True(t, ZeroCodeHash.IsEmpty())
	require.True(t, ZeroCodeHash.IsZero())
	require.False(t, ZeroCodeHash.IsNil())

	require.True(t, EmptyCodeHash.IsEmpty())
	require.False(t, EmptyCodeHash.IsZero())

	h := nonEmptyCodeHash(1, 2, 3)
	require.False(t, h.IsEmpty())
	require.False(t, h.IsNil())
	require.NotEqual(t, "<nil>", h.String())
}

func TestCodeHash_Cmp(t *testing.T) {
	t.Parallel()
	lo := InternCodeHash(common.HexToHash("0x01"))
	hi := InternCodeHash(common.HexToHash("0x02"))

	require.Equal(t, 0, NilCodeHash.Cmp(NilCodeHash)) //nolint:gocritic // intentional self-comparison
	require.Equal(t, -1, NilCodeHash.Cmp(lo))
	require.Equal(t, +1, lo.Cmp(NilCodeHash))
	require.Equal(t, -1, lo.Cmp(hi))
	require.Equal(t, +1, hi.Cmp(lo))
}

func TestCodeHash_Format(t *testing.T) {
	t.Parallel()
	require.Equal(t, "<nil>", fmt.Sprintf("%x", NilCodeHash))
	require.NotEqual(t, "<nil>", fmt.Sprintf("%x", EmptyCodeHash))
}
