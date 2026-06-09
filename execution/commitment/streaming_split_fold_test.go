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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// TestStreaming_StorageInteriorSplits is the headline Task-3 check: a whale
// account whose storage spans many slots must fold via the flat per-first-nibble
// fan-out (storageRootLocal, ~16-way below the account/storage boundary) while
// still matching the sequential root and stored branch set. The account folds
// through storageRootLocal (DeepLocalFolds), never as a split-point (its depth-64
// node carries the account terminator).
func TestStreaming_StorageInteriorSplits(t *testing.T) {
	t.Parallel()
	_, _, _, _, pk, upds, _ := whaleByNibble(20_000)

	seqRoot, seqMs := sequentialRoot(t, pk, upds)

	for _, w := range []int{1, 4, 8} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		require.NoError(t, ms.applyPlainUpdates(pk, upds))

		sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
		sc.SetNumWorkers(w)
		for i := range pk {
			sc.TouchKey(KeyToHexNibbleHash(pk[i]), pk[i], nil)
		}
		root, err := sc.Process(context.Background())
		require.NoError(t, err)

		require.Equalf(t, seqRoot, root, "whale storage-interior split(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, ms)
		require.NotZerof(t, sc.DeepLocalFolds(), "account must fold through storageRootLocal (workers=%d)", w)
		sc.Release()
	}
}

// whaleCollapseCorpus builds a two-block whale-storage delete corpus that drives
// the concurrent deep-fold path. Block 1 (pk/upds) is a whale whose storage spans
// many slots. Block 2 (k2/u2) touches the account and rewrites a SUBSET of the
// slots — a third deleted, a third updated, a third left untouched on disk — so
// the block-2 fold still crosses deepStorageThreshold (routing through
// storageRootLocal's split fan-out) while the deletes collapse interior storage
// branches AND untouched on-disk siblings must be preserved across the fold. That
// untouched-sibling read is what makes the engine self-flush mid-fold; the fold
// must write only deferred (applied once at end of block) and stay parity-clean.
func whaleCollapseCorpus() (pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) {
	var addr []byte
	addr, _, _, _, pk, upds, _ = whaleByNibble(30_000)

	k2 = [][]byte{addr}
	u2 = []Update{{Flags: BalanceUpdate | NonceUpdate}}
	u2[0].Balance.SetUint64(99)
	u2[0].Nonce = 7
	for i := range pk {
		if len(pk[i]) == length.Addr || i%3 == 2 {
			continue // leave every third slot untouched on disk
		}
		var u Update
		if i%3 == 0 {
			u.Flags = DeleteUpdate
		} else {
			u.Flags = StorageUpdate
			u.StorageLen = 4
			binary.BigEndian.PutUint32(u.Storage[:4], uint32(i)*97+3)
		}
		k2 = append(k2, pk[i])
		u2 = append(u2, u)
	}
	return pk, upds, k2, u2
}

// whaleFullCollapseCorpus is whaleCollapseCorpus's full-deletion variant: block 2
// touches the account and DELETES every storage slot, so the whale becomes
// storage-less (storageRoot == emptyRoot). Every first-storage-nibble subtree
// collapses to empty, exercising the all-children-collapsed path that must yield
// the empty-trie root rather than a zero hash.
func whaleFullCollapseCorpus() (pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) {
	var addr []byte
	addr, _, _, _, pk, upds, _ = whaleByNibble(30_000)

	k2 = [][]byte{addr}
	u2 = []Update{{Flags: BalanceUpdate | NonceUpdate}}
	u2[0].Balance.SetUint64(99)
	u2[0].Nonce = 7
	for i := range pk {
		if len(pk[i]) == length.Addr {
			continue
		}
		k2 = append(k2, pk[i])
		u2 = append(u2, Update{Flags: DeleteUpdate})
	}
	return pk, upds, k2, u2
}

// TestStreaming_StorageCollapseAcrossSplit drives a delete batch that collapses
// storage branches below the account/storage boundary while the whale is EMBEDDED
// among thousands of other accounts. Embedding is load-bearing: a single-account
// storage trie produces a degenerate incremental-collapse root that no
// embedding-insensitive concurrent per-first-nibble fold can match (Task 3 of
// docs/plans/20260609-streaming-collapse-fold-fix.md). Embedded, the collapse root
// is canonical and the streaming fold must reproduce it — root + every stored
// branch byte-identical to sequential at every worker count. A collapse self-flush
// that dropped a deletion (phantom child) or clobbered an untouched on-disk sibling
// would break branch parity.
func TestStreaming_StorageCollapseAcrossSplit(t *testing.T) {
	t.Parallel()
	wk1, wu1, wk2, wu2 := whaleCollapseCorpus()
	mk, mu := buildMixedCorpus(0x5EED, 3000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)

	seqRoot, seqMs := runIncremental(t, modeSeq, 0, k1, u1, wk2, wu2)

	for _, w := range []int{1, 4, 8} {
		strRoot, strMs := runIncremental(t, modeStreaming, w, k1, u1, wk2, wu2)
		require.Equalf(t, seqRoot, strRoot, "whale storage collapse(workers=%d) root != sequential", w)
		requireBranchParity(t, seqMs, strMs)
	}
}
