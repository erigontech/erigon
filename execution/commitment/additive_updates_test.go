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

package commitment

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// TestPrefixTrieInsertDuplicateMerges: a second Insert of the same key must
// merge the carried update per-field (copy-on-write, so a snapshot holding the
// old pointer is unaffected) and must not inflate subtree counts.
func TestPrefixTrieInsertDuplicateMerges(t *testing.T) {
	t.Parallel()

	trie := newPrefixTrie()
	key := nibs(0x01, 0x02, 0x03)
	first := &Update{Flags: BalanceUpdate, Balance: *uint256.NewInt(100)}
	trie.Insert(key, []byte("pk"), first)
	require.Equal(t, uint32(1), trie.root.subtreeCount)

	trie.Insert(key, []byte("pk"), &Update{Flags: NonceUpdate, Nonce: 5})

	assert.Equal(t, uint32(1), trie.root.subtreeCount, "duplicate insert must not inflate subtreeCount")

	var got *Update
	count := 0
	require.NoError(t, dfsSubtree(trie.root, nil, func(_, _ []byte, upd *Update) error {
		got = upd
		count++
		return nil
	}))
	require.Equal(t, 1, count, "duplicate insert must not add a second key")
	require.NotNil(t, got)
	assert.Equal(t, BalanceUpdate|NonceUpdate, got.Flags, "flags must accumulate")
	assert.Equal(t, uint64(100), got.Balance.Uint64())
	assert.Equal(t, uint64(5), got.Nonce)

	// Copy-on-write: the first update object must not have been mutated (a
	// concurrent fold snapshot may still hold it).
	assert.Equal(t, BalanceUpdate, first.Flags, "merge must not mutate the previously stored update")
}

// additiveCorpus builds a batch where every key is touched twice with partial
// updates, plus the merged equivalents the sequential oracle folds:
//   - accA: Balance then Nonce (flags must accumulate)
//   - accB: Delete then Balance (resurrection must clear DeleteUpdate)
//   - one storage slot of accA: 42 then 7 (last write wins)
func additiveCorpus() (keys [][]byte, partials [][2]*Update, merged []Update) {
	accA := []byte("\x4c\x88\x85\x35\x84\x1a\xcb\xe0\x70\x9b\x07\x58\x08\x3f\x61\xd3\x75\xbc\x02\xb4")
	accB := []byte("\x68\xee\x6c\x0e\x9c\xdc\x73\xb2\xb2\xd5\x2d\xbd\x79\xf1\x9d\x24\xfe\x25\xe2\xf9")
	slot := append(append([]byte{}, accA...), make([]byte, 31)...)
	slot = append(slot, 0x01)

	keys = [][]byte{accA, accB, slot}

	balA := &Update{Flags: BalanceUpdate, Balance: *uint256.NewInt(1000)}
	nonceA := &Update{Flags: NonceUpdate, Nonce: 7}
	delB := &Update{Flags: DeleteUpdate}
	balB := &Update{Flags: BalanceUpdate, Balance: *uint256.NewInt(500)}
	stale := &Update{Flags: StorageUpdate}
	stale.StorageLen = 1
	stale.Storage[0] = 42
	final := &Update{Flags: StorageUpdate}
	final.StorageLen = 1
	final.Storage[0] = 7

	partials = [][2]*Update{{balA, nonceA}, {delB, balB}, {stale, final}}

	merged = []Update{
		{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(1000), Nonce: 7},
		{Flags: BalanceUpdate, Balance: *uint256.NewInt(500)},
		*final,
	}
	return keys, partials, merged
}

// TestUpdatesModeParallel_AdditiveTouchDirect: two partial TouchPlainKeyDirect
// calls per key must merge like ModeUpdate, producing the same root as the
// sequential trie folding the merged state.
func TestUpdatesModeParallel_AdditiveTouchDirect(t *testing.T) {
	t.Parallel()

	keys, partials, merged := additiveCorpus()
	seqRoot, _ := sequentialRoot(t, keys, merged)

	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	require.NoError(t, parMs.applyPlainUpdates(keys, merged))

	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer parTrie.Release()
	parTrie.SetNumWorkers(2)
	parTrie.ResetContext(parMs)

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	for i, k := range keys {
		ut.TouchPlainKeyDirect(string(k), partials[i][0])
		ut.TouchPlainKeyDirect(string(k), partials[i][1])
	}
	require.Equal(t, uint64(len(keys)), ut.Size(), "re-touches must not add keys")

	parRoot, err := parTrie.Process(context.Background(), ut, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, seqRoot, parRoot, "additive partial touches must fold to the merged root")
}

// TestStreaming_AdditiveTouchKey: the streaming committer must merge repeated
// carried touches of one key the same way.
func TestStreaming_AdditiveTouchKey(t *testing.T) {
	t.Parallel()

	keys, partials, merged := additiveCorpus()
	seqRoot, _ := sequentialRoot(t, keys, merged)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, merged))

	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(2)
	for i, k := range keys {
		sc.TouchKey(KeyToHexNibbleHash(k), k, partials[i][0])
		sc.TouchKey(KeyToHexNibbleHash(k), k, partials[i][1])
	}

	root, err := sc.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, seqRoot, root, "streaming additive touches must fold to the merged root")
}
