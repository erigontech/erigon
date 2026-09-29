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

package v3

import (
	"math/rand/v2"
	"testing"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/stretchr/testify/require"
)

func TestNodeKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E8/NodeKeyRoundTrip", func(t *testing.T) {
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
					{name: "account", key: AccountNodeKey(path, nil), tag: tagAccountNode, addr: []byte{}},
					{name: "storage", key: StorageNodeKey(addrHash, path, nil), tag: tagStorageNode, addr: addrHash[:]},
				} {
					t.Run(tc.name, func(t *testing.T) {
						require.Equal(t, tc.tag, tc.key[0])
						require.Equal(t, tc.addr, tc.key[1:1+len(tc.addr)])
						require.Equal(t, byte(count), tc.key[len(tc.key)-1])
						require.Equal(t, path, unpackPath(tc.key[1+len(tc.addr):len(tc.key)-1], count, []byte{}))
					})
				}
			}
		}},
		{"E8/RootKeys", func(t *testing.T) {
			var addrHash [32]byte
			addrHash[0] = 0xab

			require.Equal(t, []byte{tagAccountNode, 0}, AccountNodeKey(nil, nil))
			require.Equal(t, append([]byte{tagStorageNode}, append(addrHash[:], 0)...), StorageNodeKey(addrHash, nil, nil))
		}},
		{"E8/NodeKeyUsesDestination", func(t *testing.T) {
			dst := []byte{0xaa, 0xbb}
			key := AccountNodeKey([]byte{1, 2, 3}, dst)
			require.Equal(t, []byte{0xaa, 0xbb, tagAccountNode, 0x12, 0x30, 3}, key)
			require.Equal(t, []byte{0xaa, 0xbb}, dst)
		}},
		{"E8/V1KeyedTagDisjointness", func(t *testing.T) {
			rng := rand.New(rand.NewPCG(0x6b657973, 0x7634))
			for range 10_000 {
				path := make([]byte, rng.IntN(65))
				for j := range path {
					path[j] = byte(rng.IntN(16))
				}
				compact := nibbles.HexToCompact(path)
				require.Less(t, compact[0], byte(tagAccountNode))
			}

			require.Equal(t, AccountNodeKey(nil, nil), nibbles.EncodeKeyV2([]byte{4, 0}))
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}
