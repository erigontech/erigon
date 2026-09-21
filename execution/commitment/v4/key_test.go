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

package v4

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/stretchr/testify/require"
)

func TestNodeKeyRoundTrip(t *testing.T) {
	var addrHash [32]byte
	for i := range addrHash {
		addrHash[i] = byte(i)
	}

	for count := 0; count <= 64; count++ {
		path := make([]byte, count)
		for i := range path {
			path[i] = byte((i + count) & 0x0f)
		}

		for _, tc := range []struct {
			name string
			key  []byte
			tag  byte
			addr []byte
		}{
			{name: "account", key: AccountNodeKey(path, nil), tag: tagAccountNode},
			{name: "storage", key: StorageNodeKey(addrHash, path, nil), tag: tagStorageNode, addr: addrHash[:]},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tag, gotAddr, gotPath, err := ParseKey(tc.key)
				require.NoError(t, err)
				require.Equal(t, tc.tag, tag)
				require.Equal(t, tc.addr, gotAddr)
				if count == 0 {
					require.Empty(t, gotPath)
				} else {
					require.Equal(t, path, gotPath)
				}
			})
		}
	}
}

func TestRootAndStateKeys(t *testing.T) {
	var addrHash [32]byte
	addrHash[0] = 0xab

	require.Equal(t, []byte{tagAccountNode, 0}, AccountRootKey())
	require.Equal(t, append([]byte{tagStorageNode}, append(addrHash[:], 0)...), StorageRootKey(addrHash))
	require.Equal(t, []byte{tagState}, StateKey())

	tag, addr, path, err := ParseKey(AccountRootKey())
	require.NoError(t, err)
	require.Equal(t, tagAccountNode, tag)
	require.Empty(t, addr)
	require.Empty(t, path)

	tag, addr, path, err = ParseKey(StorageRootKey(addrHash))
	require.NoError(t, err)
	require.Equal(t, tagStorageNode, tag)
	require.Equal(t, addrHash[:], addr)
	require.Empty(t, path)

	tag, addr, path, err = ParseKey(StateKey())
	require.NoError(t, err)
	require.Equal(t, tagState, tag)
	require.Empty(t, addr)
	require.Empty(t, path)
}

func TestNodeKeyUsesDestination(t *testing.T) {
	dst := []byte{0xaa, 0xbb}
	key := AccountNodeKey([]byte{1, 2, 3}, dst)
	require.Equal(t, []byte{0xaa, 0xbb, tagAccountNode, 0x12, 0x30, 3}, key)
	require.Equal(t, []byte{0xaa, 0xbb}, dst)
}

func TestParseKeyRejectsMalformedPath(t *testing.T) {
	valid := AccountNodeKey([]byte{1, 2, 3}, nil)
	for name, key := range map[string][]byte{
		"missing length":       {tagAccountNode},
		"short packed path":    {tagAccountNode, 0x12, 4},
		"long packed path":     {tagAccountNode, 0x12, 0x30, 2},
		"non-zero odd padding": {tagAccountNode, 0x12, 0x31, 3},
		"unknown tag":          {0x43, 0},
		"state suffix":         {tagState, 0},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := ParseKey(key)
			require.Error(t, err)
		})
	}

	badCount := bytes.Clone(valid)
	badCount[len(badCount)-1] = 65
	_, _, _, err := ParseKey(badCount)
	require.ErrorIs(t, err, ErrKeyPathLength)
}

func TestV1KeyedTagDisjointness(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x6b657973, 0x7634))
	for range 10_000 {
		path := make([]byte, rng.IntN(65))
		for j := range path {
			path[j] = byte(rng.IntN(16))
		}
		compact := nibbles.HexToCompact(path)
		require.Less(t, compact[0], byte(tagAccountNode))
	}

	require.Equal(t, AccountRootKey(), nibbles.EncodeKeyV2([]byte{4, 0}))
}

func TestAssertV1Keyed(t *testing.T) {
	require.NoError(t, AssertV1Keyed(false))
	require.ErrorIs(t, AssertV1Keyed(true), ErrV4RequiresV1Keyed)
}
