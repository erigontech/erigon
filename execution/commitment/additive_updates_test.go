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
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// additiveCorpus returns keys touched twice with partial updates and the merged equivalents the sequential oracle folds.
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

// TestAdditiveTouch asserts both concurrent engines merge two partial touches of the same key into the sequential merged-state root.
func TestAdditiveTouch(t *testing.T) {
	t.Parallel()

	t.Run("parallel", func(t *testing.T) {
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
	})

	t.Run("streaming", func(t *testing.T) {
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
	})
}
