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
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
	"github.com/stretchr/testify/require"
)

func TestFoldParity(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E29/FoldEmptyTrie", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				n := fork(nil)
				n.plane = plane
				got, err := fold(n, 0)
				require.NoError(t, err)
				require.Equal(t, empty.RootHash[:], got[:])
			}
		}},
		{"E29/FoldSingleLeafMatchesHexPatriciaHashed", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				key := foldKey(plane, 1)
				path := commitment.KeyToHexNibbleHash(key)
				value := foldValue(plane, 1)
				n := fork(nil)
				n.plane = plane
				require.NoError(t, insert(n, path, value))

				want := hexPatriciaRoot(t, plane, [][]byte{key}, [][]byte{path}, []commitment.Update{foldUpdate(plane, 1)})
				require.Equal(t, want, foldPlaneRoot(t, n))
			}
		}},
		{"E29/seeded", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				for _, tc := range []struct {
					name        string
					count       int
					seed        int64
					checkBranch bool
				}{
					{"each-plane/1", 1, 1 + int64(plane), false},
					{"each-plane/2", 2, 2 + int64(plane), plane == planeStorage},
					{"each-plane/16", 16, 16 + int64(plane), false},
					{"each-plane/1000", 1000, 1000 + int64(plane), false},
					{"mixed-planes/16", 16, 91 + int64(plane), false},
				} {
					t.Run(fmtPlane(plane)+"/"+tc.name, func(t *testing.T) {
						keys, paths := distinctKeysAndPaths(plane, tc.count, tc.seed)
						updates := make([]commitment.Update, tc.count)
						n := fork(nil)
						n.plane = plane
						for i, path := range paths {
							updates[i] = foldUpdate(plane, i+1)
							require.NoError(t, insert(n, path, foldValue(plane, i+1)))
						}
						want := hexPatriciaRoot(t, plane, keys, paths, updates)
						if tc.checkBranch {
							var refs [16][]byte
							for i, path := range paths {
								key := append(append([]byte(nil), path[1:]...), nibbles.Terminator)
								refs[path[0]] = storageLeafRef(nibbles.HexToCompact(key), foldValue(plane, i+1), nil)
							}
							got, err := fold(n, 0)
							require.NoError(t, err)
							require.Equal(t, branchRef(&refs), got)
						}
						require.Equal(t, want, foldPlaneRoot(t, n))
					})
				}
			}
		}},
		{"E30-E39/FoldUsesStoredChildHashWithoutStateReads", func(t *testing.T) {
			ctx := newMockContext()
			n := fork(nil)
			n.plane = planeStorage
			stored := bytes.Repeat([]byte{0x31}, 32)
			n.setStoredChild(2, stored, []byte{3, 4})
			path := append([]byte{5}, bytes.Repeat([]byte{6}, 63)...)
			n.setLeaf(5, packPath(path[1:], nil), []byte{7})

			got, err := fold(n, 0)
			require.NoError(t, err)
			var refs [16][]byte
			wrapped := extensionRef([]byte{3, 4}, stored)
			refs[2] = wrapped[:]
			refs[5] = storageLeafRef(nibbles.HexToCompact(append(append([]byte(nil), path[1:]...), nibbles.Terminator)), []byte{7}, nil)
			want := branchRef(&refs)
			require.Equal(t, want, got)
			require.Empty(t, ctx.accountCalls)
			require.Empty(t, ctx.storageCalls)
			require.Empty(t, ctx.branchCalls)
		}},
		{"E30/FoldRejectsUnknownPlane", func(t *testing.T) {
			n := fork(nil)
			n.plane = 0xff
			n.setLeaf(0, packPath(bytes.Repeat([]byte{1}, 63), nil), []byte{1})
			_, err := fold(n, 0)
			require.ErrorIs(t, err, errFoldPlane)
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

var foldStorageAddr = bytes.Repeat([]byte{0x5a}, length.Addr)

func foldPlaneRoot(t *testing.T, n *node) []byte {
	t.Helper()
	got, err := fold(n, 0)
	require.NoError(t, err)
	if n.plane != planeStorage {
		return got[:]
	}
	owner := foldUpdate(planeAccount, 1)
	account := fork(nil)
	account.plane = planeAccount
	require.NoError(t, insert(account, commitment.KeyToHexNibbleHash(foldStorageAddr), encodeAccountLeaf(&owner, got[:], nil)))
	root, err := fold(account, 0)
	require.NoError(t, err)
	return root[:]
}

func hexPatriciaRoot(t *testing.T, plane byte, keys, paths [][]byte, updates []commitment.Update) []byte {
	t.Helper()
	ctx := runner.NewMemory(runner.ContextSpec{})
	trie := commitment.NewHexPatriciaHashed(length.Addr, ctx, commitment.TrieConfig{DeferBranchUpdates: false})
	defer trie.Release()
	batch := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	for i := range paths {
		key := keys[i]
		if plane == planeStorage {
			key = append(bytes.Clone(foldStorageAddr), key...)
		}
		batch.TouchPlainKeyDirect(string(key), &updates[i])
	}
	if plane == planeStorage {
		owner := foldUpdate(planeAccount, 1)
		batch.TouchPlainKeyDirect(string(foldStorageAddr), &owner)
	}
	got, err := trie.Process(context.Background(), batch, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return got
}

func foldValue(plane byte, number int) []byte {
	if plane == planeStorage {
		return []byte{byte(number), byte(number >> 8)}
	}
	update := foldUpdate(plane, number)
	return encodeAccountLeaf(&update, empty.RootHash[:], nil)
}

func foldUpdate(plane byte, number int) commitment.Update {
	if plane == planeStorage {
		return *storageUpdate(commitmenttest.Storage(commitmenttest.StorageSpec{Number: number}))
	}
	return *testAccountUpdate(commitmenttest.Account(commitmenttest.AccountSpec{Kind: "fold", Number: number}))
}

func distinctKeysAndPaths(plane byte, count int, seed int64) ([][]byte, [][]byte) {
	keys, err := commitmenttest.Keys(commitmenttest.MathRand(seed), commitmenttest.KeySpec{Kind: "random-distinct", Size: 20, Count: count})
	if err != nil {
		panic(err)
	}
	paths := make([][]byte, 0, len(keys))
	for _, key := range keys {
		paths = append(paths, commitment.KeyToHexNibbleHash(key))
	}
	return keys, paths
}

func foldKey(plane byte, number int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "fold", Size: 20}, number)
}

func itoa(value int) string {
	return strconv.Itoa(value)
}
