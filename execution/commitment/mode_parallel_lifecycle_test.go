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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func lifecycleCorpus() (k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update, kc [][]byte, uc []Update) {
	var addrs []string
	for i, nib := range []int{1, 3, 7, 0xb} {
		addrs = append(addrs, addrHex(findAddressForNibble(nib, 600+i)), addrHex(findAddressForNibble(nib, 700+i)))
	}

	ub1 := NewUpdateBuilder()
	for i, a := range addrs {
		ub1.Balance(a, uint64(1000+i))
	}
	ub1.Storage(addrs[0], hex.EncodeToString(slotHashBytes(1)), "beef")
	ub1.Storage(addrs[0], hex.EncodeToString(slotHashBytes(2)), "f00d")
	k1, u1 = ub1.Build()

	ub2 := NewUpdateBuilder().Balance(addrs[2], 2222).Balance(addrs[5], 5555)
	k2, u2 = ub2.Build()

	ubc := NewUpdateBuilder()
	for i, a := range addrs {
		bal := uint64(1000 + i)
		switch i {
		case 2:
			bal = 2222
		case 5:
			bal = 5555
		}
		ubc.Balance(a, bal)
	}
	ubc.Storage(addrs[0], hex.EncodeToString(slotHashBytes(1)), "beef")
	ubc.Storage(addrs[0], hex.EncodeToString(slotHashBytes(2)), "f00d")
	kc, uc = ubc.Build()
	return k1, u1, k2, u2, kc, uc
}

func touchBatch(t *testing.T, ms *MockState, ut *Updates, keys [][]byte, upds []Update) {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	for _, k := range keys {
		ut.TouchPlainKey(string(k), nil, ut.TouchAccount)
	}
}

type countingSink struct{ calls int }

func (c *countingSink) TouchKey(hashedKey, plainKey []byte, update *Update) { c.calls++ }

// The dedup map only dedups plain-key interning: every touch — including a repeat of an
// already-collected key — must be forwarded to the streamer, or a scheduler's eagerly
// folded split goes stale within the block.
func TestModeParallel_RetouchReachesStreamer(t *testing.T) {
	t.Parallel()
	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	sink := &countingSink{}
	ut.SetStreamingCommitter(sink)

	a := findAddressForNibble(3, 999)
	ut.TouchPlainKey(string(a), nil, ut.TouchAccount)
	ut.TouchPlainKey(string(a), nil, ut.TouchAccount)
	require.Equal(t, 2, sink.calls, "a re-touch must be forwarded to the streamer, not deduped")
	require.Equal(t, uint64(1), ut.Size(), "interning stays deduped")
}

// A node carries one ModeParallel Updates buffer across blocks: a key re-touched in a
// later block must land in that block's fold instead of being dropped by stale per-buffer
// dedup state. Pins the carried-buffer lifecycle end to end.
func TestModeParallel_StreamingRetouchAcrossBlocks(t *testing.T) {
	t.Parallel()
	k1, u1, k2, u2, kc, uc := lifecycleCorpus()
	oracle, _ := engineRoot(t, modeSeq, 0, kc, uc)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	cfg := DefaultTrieConfig()
	cfg.Variant = VariantStreamingHexPatricia
	trie, ut := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
	defer ut.Close()
	defer trie.Release()
	pt := trie.(*ParallelPatriciaHashed)
	pt.SetNumWorkers(4)
	pt.SetTrieContextFactory(mockTrieCtxFactory(ms))
	pt.ResetContext(ms)

	touchBatch(t, ms, ut, k1, u1)
	processRoot(t, trie, ut)

	touchBatch(t, ms, ut, k2, u2)
	got := processRoot(t, trie, ut)
	require.Equal(t, oracle, got, "re-touched keys were dropped by the stale dedup map")
}

// Process must consume the ModeParallel collection the way HashSort consumes
// ModeDirect/ModeUpdate: a carried Updates buffer starts every block empty, so block N+1
// folds only its own touches instead of the union of everything since batch start.
func TestModeParallel_ProcessConsumesUpdates(t *testing.T) {
	t.Parallel()
	k1, u1, k2, u2, kc, uc := lifecycleCorpus()

	t.Run("parallel", func(t *testing.T) {
		t.Parallel()
		oracle, _ := engineRoot(t, modeSeq, 0, kc, uc)

		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		tr := newParTrie(t, ms, 4)
		defer tr.Release()
		ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
		defer ut.Close()

		touchBatch(t, ms, ut, k1, u1)
		require.Equal(t, uint64(len(k1)), ut.Size())
		processRoot(t, tr, ut)
		require.Zero(t, ut.Size(), "Process left the touched-key collection unconsumed")
		if root := ut.parallel.trie.root; root != nil {
			require.Zero(t, root.subtreeCount, "Process left the prefix trie populated")
		}

		touchBatch(t, ms, ut, k2, u2)
		require.Equal(t, uint64(len(k2)), ut.Size(), "block-2 collection must hold only block-2 keys")
		got := processRoot(t, tr, ut)
		require.Zero(t, ut.Size())
		require.Equal(t, oracle, got)

		again := processRoot(t, tr, ut)
		require.Equal(t, got, again, "a zero-touch Process must return the carried root")
	})

	t.Run("streaming", func(t *testing.T) {
		t.Parallel()
		oracle, _ := engineRoot(t, modeSeq, 0, kc, uc)

		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		cfg := DefaultTrieConfig()
		cfg.Variant = VariantStreamingHexPatricia
		trie, ut := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
		defer ut.Close()
		defer trie.Release()
		pt := trie.(*ParallelPatriciaHashed)
		pt.SetNumWorkers(4)
		pt.SetTrieContextFactory(mockTrieCtxFactory(ms))
		pt.ResetContext(ms)

		touchBatch(t, ms, ut, k1, u1)
		processRoot(t, trie, ut)
		require.Zero(t, ut.Size(), "streaming Process left the touched-key collection unconsumed")
		if root := ut.parallel.trie.root; root != nil {
			require.Zero(t, root.subtreeCount, "streaming Process left the dual-inserted prefix trie populated")
		}

		touchBatch(t, ms, ut, k2, u2)
		got := processRoot(t, trie, ut)
		require.Zero(t, ut.Size())
		require.Equal(t, oracle, got)

		again := processRoot(t, trie, ut)
		require.Equal(t, got, again, "a zero-touch streaming Process must return the carried root, not the empty root")
	})
}

// A failed Process must leave the collection intact so the caller's retry folds the
// block's touches; only a successful fold consumes them.
func TestModeParallel_ErrorKeepsCollection(t *testing.T) {
	t.Parallel()
	k1, u1, _, _, _, _ := lifecycleCorpus()
	oracle, _ := engineRoot(t, modeSeq, 0, k1, u1)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("parallel", func(t *testing.T) {
		t.Parallel()
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		tr := newParTrie(t, ms, 4)
		defer tr.Release()
		ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
		defer ut.Close()

		touchBatch(t, ms, ut, k1, u1)
		_, err := tr.Process(canceled, ut, "", nil, WarmupConfig{})
		require.Error(t, err)
		require.Equal(t, uint64(len(k1)), ut.Size(), "error path must keep the collection for the retry")

		got := processRoot(t, tr, ut)
		require.Zero(t, ut.Size())
		require.Equal(t, oracle, got)
	})

	t.Run("streaming", func(t *testing.T) {
		t.Parallel()
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		cfg := DefaultTrieConfig()
		cfg.Variant = VariantStreamingHexPatricia
		trie, ut := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
		defer ut.Close()
		defer trie.Release()
		pt := trie.(*ParallelPatriciaHashed)
		pt.SetNumWorkers(4)
		pt.SetTrieContextFactory(mockTrieCtxFactory(ms))
		pt.ResetContext(ms)

		touchBatch(t, ms, ut, k1, u1)
		_, err := trie.Process(canceled, ut, "", nil, WarmupConfig{})
		require.Error(t, err)
		require.Equal(t, uint64(len(k1)), ut.Size(), "error path must keep the collection for the retry")

		got := processRoot(t, trie, ut)
		require.Zero(t, ut.Size())
		require.Equal(t, oracle, got)
	})
}
