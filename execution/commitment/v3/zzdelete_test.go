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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func slotPath(prefix ...byte) []byte {
	p := make([]byte, 0, 64)
	p = append(p, prefix...)
	fill := byte(0xd)
	if len(prefix) != 0 {
		fill = (prefix[len(prefix)-1] + 7) & 0x0f
	}
	for len(p) < 64 {
		p = append(p, fill)
	}
	return p
}

func slotValue(path []byte) []byte {
	return []byte{path[0] + 1, path[1] + 1, path[2] + 1, path[3] + 1}
}

func requireRecordsContain(t *testing.T, want, got map[string][]byte) {
	t.Helper()
	for k, v := range want {
		require.Equal(t, v, got[k], "record %x must match the fresh trie's", k)
	}
}

func liveRecords(ctx *mockContext) map[string][]byte {
	out := make(map[string][]byte)
	for k, v := range ctx.branches {
		if len(v) != 0 {
			out[k] = v
		}
	}
	return out
}

func seedStorage(t *testing.T, ctx *mockContext, addr [32]byte, paths [][]byte) [32]byte {
	t.Helper()
	entries := make([]storageEntry, 0, len(paths))
	for _, p := range paths {
		entries = append(entries, entryOf(p, phaseAStorageUpdate(slotValue(p))))
	}
	root, err := runStorageTask(ctx, storageTask{addrHash: addr, entries: entries})
	require.NoError(t, err)
	return root
}

func TestPhaseAStorageDeleteMatchesFreshTrie(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed [][]byte
		drop []int
	}{
		{"sole sibling leaves", [][]byte{slotPath(0), slotPath(1)}, []int{1}},
		{"collapse nested branch", [][]byte{slotPath(1, 2, 3), slotPath(1, 2, 4), slotPath(9)}, []int{1}},
		{"drop middle of three", [][]byte{slotPath(1), slotPath(2), slotPath(3)}, []int{1}},
		{"drop one of deep pair", [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2), slotPath(5, 6)}, []int{0}},
		{"drop deep pair, keep far leaf", [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2), slotPath(5, 6)}, []int{0, 1}},
		{"drop deep pair, two far leaves", [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2), slotPath(5, 6), slotPath(8)}, []int{0, 1}},
		{"drop deep pair, no far leaf", [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2)}, []int{0, 1}},
		{"drop all but one", [][]byte{slotPath(7), slotPath(8), slotPath(9)}, []int{0, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var addr [32]byte
			addr[0] = 0x5a

			dropped := make(map[int]bool, len(tc.drop))
			for _, i := range tc.drop {
				dropped[i] = true
			}

			staged := newMockContext()
			seedStorage(t, staged, addr, tc.seed)
			deletes := make([]storageEntry, 0, len(tc.drop))
			for _, i := range tc.drop {
				deletes = append(deletes, entryOf(tc.seed[i], &commitment.Update{Flags: commitment.DeleteUpdate}))
			}
			got, err := runStorageTask(staged, storageTask{addrHash: addr, entries: deletes})
			require.NoError(t, err)

			survivors := make([][]byte, 0, len(tc.seed))
			for i, p := range tc.seed {
				if !dropped[i] {
					survivors = append(survivors, p)
				}
			}
			fresh := newMockContext()
			want := seedStorage(t, fresh, addr, survivors)

			require.Equal(t, want, got, "root after delete must equal the fresh trie's root")
			requireRecordsContain(t, liveRecords(fresh), liveRecords(staged))
		})
	}
}

func TestPhaseAStorageDeleteThenReinsertRoundTrips(t *testing.T) {
	var addr [32]byte
	addr[0] = 0x7c
	paths := [][]byte{slotPath(2), slotPath(3, 1), slotPath(3, 2)}

	ctx := newMockContext()
	before := seedStorage(t, ctx, addr, paths)
	beforeRecords := liveRecords(ctx)

	_, err := runStorageTask(ctx, storageTask{addrHash: addr, entries: []storageEntry{
		entryOf(paths[1], &commitment.Update{Flags: commitment.DeleteUpdate}),
	}})
	require.NoError(t, err)

	after, err := runStorageTask(ctx, storageTask{addrHash: addr, entries: []storageEntry{
		entryOf(paths[1], phaseAStorageUpdate(slotValue(paths[1]))),
	}})
	require.NoError(t, err)

	require.Equal(t, before, after)
	require.Equal(t, beforeRecords, liveRecords(ctx))
}
