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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// A whale's storage is deleted ENTIRELY (deep fold, empty-storage case), then a LATER block
// re-populates it. The deep fold used to persist the emptied account leaf with a storage-root hash of
// empty.RootHash; single-trie leaves it with no storage-root hash (hashLen 0 — computeCellHash supplies
// empty.RootHash at hash time). A stored empty.RootHash makes needUnfolding descend into the
// storage-root branch record that was deleted when the storage emptied, so the re-populate fails with
// "empty branch data read during unfold" on the parallel/streaming engines.
//
// The whale shares nibbles [7,8] with two siblings so its account leaf persists as a child of the
// [7,8] branch record — carrying its (empty) storage-root hash — exactly as mainnet account
// b4a038…e41 (hashed fd698ec0…) sits under the dense fd698e branch at block 5231408. A lone whale
// instead propagates its leaf up a level, which hides the bug; the collapse tests keep a survivor slot
// and never exercise the empty-storage branch at all.
func TestDeepFold_EmptyStorageThenRepopulate(t *testing.T) {
	t.Parallel()

	w := findAddressForHexPrefix([]byte{7, 8, 1}, 101)
	s1 := findAddressForHexPrefix([]byte{7, 8, 2}, 102)
	s2 := findAddressForHexPrefix([]byte{7, 8, 3}, 103)
	f0 := findAddressForHexPrefix([]byte{0}, 104)
	ff := findAddressForHexPrefix([]byte{0xf}, 105)

	const slots = 1500 // > deepStorageThreshold(1000) => the whale folds concurrently

	locs := make([]string, slots)
	for i := range locs {
		locs[i] = common.Bytes2Hex(slotHashBytes(i))
	}

	// Batch 1: create the cluster; the whale gets a deep storage trie.
	b1 := NewUpdateBuilder().
		Balance(addrHex(w), 100).Balance(addrHex(s1), 5).Balance(addrHex(s2), 6).
		Balance(addrHex(f0), 7).Balance(addrHex(ff), 8)
	for _, loc := range locs {
		b1.Storage(addrHex(w), loc, "01")
	}
	k1, u1 := b1.Build()

	// Batch 2: keep the whale alive but delete every storage slot — the empty-storage deep-fold case.
	b2 := NewUpdateBuilder().Balance(addrHex(w), 200)
	for _, loc := range locs {
		b2.DeleteStorage(addrHex(w), loc)
	}
	k2, u2 := b2.Build()

	// Batch 3: re-populate every slot (again above the deep threshold), re-expanding the emptied whale.
	b3 := NewUpdateBuilder()
	for _, loc := range locs {
		b3.Storage(addrHex(w), loc, "02")
	}
	k3, u3 := b3.Build()

	batches := []engineBatch{{k1, u1}, {k2, u2}, {k3, u3}}

	seqRoots, seqMs := runEngineBatches(t, modeSeq, 0, batches)
	for _, tc := range []struct {
		name string
		mode runMode
	}{
		{"parallel", modeParallel},
		{"streaming", modeStreaming},
		{"streaming_scheduled", modeStreamingScheduled},
	} {
		for _, w := range []int{1, 4, 8} {
			roots, ms := runEngineBatches(t, tc.mode, w, batches)
			for i := range batches {
				require.Equalf(t, seqRoots[i], roots[i], "%s(workers=%d) batch %d root != sequential", tc.name, w, i+1)
			}
			requireBranchParity(t, seqMs, ms)
		}
	}
}
