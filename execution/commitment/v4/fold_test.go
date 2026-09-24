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
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestFoldEmptyTrie(t *testing.T) {
	for _, plane := range []byte{planeAccount, planeStorage} {
		n := fork(nil)
		n.plane = plane
		got, err := fold(n, 0)
		require.NoError(t, err)
		require.Equal(t, empty.RootHash[:], got[:])
	}
}

func TestFoldSingleLeafMatchesHexPatriciaHashed(t *testing.T) {
	for _, plane := range []byte{planeAccount, planeStorage} {
		key := foldKey(plane, 1)
		path := commitment.KeyToHexNibbleHash(key)
		value := foldValue(plane, 1)
		n := fork(nil)
		n.plane = plane
		require.NoError(t, insert(n, path, value))

		got, err := fold(n, 0)
		require.NoError(t, err)
		want := hexPatriciaRoot(t, plane, [][]byte{key}, [][]byte{path}, []commitment.Update{foldUpdate(plane, 1)})
		require.Equal(t, want, got[:])
	}
}

func TestFoldMatchesHexPatriciaHashedForEachPlane(t *testing.T) {
	for _, plane := range []byte{planeAccount, planeStorage} {
		for _, count := range []int{1, 2, 16, 1000} {
			t.Run(fmtPlane(plane)+"/"+itoa(count), func(t *testing.T) {
				keys, paths := distinctKeysAndPaths(plane, count, int64(count)+int64(plane))
				updates := make([]commitment.Update, count)
				n := fork(nil)
				n.plane = plane
				for i, path := range paths {
					updates[i] = foldUpdate(plane, i+1)
					value := foldValue(plane, i+1)
					require.NoError(t, insert(n, path, value))
				}
				got, err := fold(n, 0)
				require.NoError(t, err)
				want := hexPatriciaRoot(t, plane, keys, paths, updates)
				if plane == planeStorage && count == 2 {
					var refs [16][]byte
					for i, path := range paths {
						key := append(append([]byte(nil), path...), nibbles.Terminator)
						refs[path[0]] = storageLeafRef(nibbles.HexToCompact(key), foldValue(plane, i+1), nil)
					}
					require.Equal(t, branchRef(&refs), got)
				}
				require.Equal(t, want, got[:])
			})
		}
	}
}

func TestFoldMatchesHexPatriciaHashedMixedPlanes(t *testing.T) {
	for _, plane := range []byte{planeAccount, planeStorage} {
		keys, paths := distinctKeysAndPaths(plane, 16, 91+int64(plane))
		updates := make([]commitment.Update, len(keys))
		n := fork(nil)
		n.plane = plane
		for i, path := range paths {
			updates[i] = foldUpdate(plane, i+1)
			require.NoError(t, insert(n, path, foldValue(plane, i+1)))
		}
		got, err := fold(n, 0)
		require.NoError(t, err)
		want := hexPatriciaRoot(t, plane, keys, paths, updates)
		require.Equal(t, want, got[:])
	}
}

func TestFoldUsesStoredChildHashWithoutStateReads(t *testing.T) {
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
	refs[5] = storageLeafRef(mustCompact(append(append([]byte(nil), path...), nibbles.Terminator)), []byte{7}, nil)
	want := branchRef(&refs)
	require.Equal(t, want, got)
	require.Empty(t, ctx.accountCalls)
	require.Empty(t, ctx.storageCalls)
	require.Empty(t, ctx.branchCalls)
}

func TestFoldRejectsUnknownPlane(t *testing.T) {
	n := fork(nil)
	n.plane = 0xff
	n.setLeaf(0, packPath(bytes.Repeat([]byte{1}, 63), nil), []byte{1})
	_, err := fold(n, 0)
	require.ErrorIs(t, err, errFoldPlane)
}

type foldContext struct {
	branches map[string][]byte
	accounts int
	storage  int
}

func (c *foldContext) Branch(key []byte) ([]byte, kv.Step, error) {
	return bytes.Clone(c.branches[string(key)]), 0, nil
}

func (c *foldContext) PutBranch(key, data, _ []byte) error {
	c.branches[string(key)] = bytes.Clone(data)
	return nil
}

func (c *foldContext) Account([]byte) (*commitment.Update, error) {
	c.accounts++
	return &commitment.Update{}, nil
}

func (c *foldContext) Storage([]byte) (*commitment.Update, error) {
	c.storage++
	return &commitment.Update{}, nil
}

func hexPatriciaRoot(t *testing.T, plane byte, keys, paths [][]byte, updates []commitment.Update) []byte {
	t.Helper()
	ctx := &foldContext{branches: make(map[string][]byte)}
	accountKeyLen := int16(length.Addr)
	if plane == planeStorage {
		accountKeyLen = 0
	}
	trie := commitment.NewHexPatriciaHashed(accountKeyLen, ctx, commitment.TrieConfig{DeferBranchUpdates: false})
	defer trie.Release()
	batch := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	for i := range paths {
		batch.TouchPlainKeyDirect(string(keys[i]), &updates[i])
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
		var storage [32]byte
		storage[0] = byte(number)
		storage[1] = byte(number >> 8)
		return commitment.Update{Flags: commitment.StorageUpdate, StorageLen: 2, Storage: storage}
	}
	return commitment.Update{CodeHash: empty.CodeHash, Flags: commitment.CodeUpdate | commitment.NonceUpdate | commitment.BalanceUpdate, Nonce: uint64(number), Balance: *uint256.NewInt(uint64(number * 3))}
}

func distinctKeysAndPaths(plane byte, count int, seed int64) ([][]byte, [][]byte) {
	rng := rand.New(rand.NewSource(seed))
	keys := make([][]byte, 0, count)
	paths := make([][]byte, 0, count)
	seen := make(map[string]struct{}, count)
	for len(paths) < count {
		key := make([]byte, length.Addr)
		if plane == planeStorage {
			key = make([]byte, length.Addr)
		}
		rng.Read(key)
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		keys = append(keys, key)
		path := commitment.KeyToHexNibbleHash(key)
		paths = append(paths, path)
	}
	return keys, paths
}

func foldKey(plane byte, number int) []byte {
	key := make([]byte, length.Addr)
	if plane == planeStorage {
		key = make([]byte, length.Addr)
	}
	for i := range key {
		key[i] = byte(number + i*17)
	}
	return key
}

func mustCompact(path []byte) []byte {
	return nibbles.HexToCompact(path)
}

func itoa(value int) string {
	return strconv.Itoa(value)
}
