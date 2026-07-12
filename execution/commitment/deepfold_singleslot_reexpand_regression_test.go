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
)

// A whale's storage collapses to a single surviving SLOT (not a subtree) in one block, then a LATER
// block re-expands it with >deepStorageThreshold touched slots so the re-expansion runs through the
// deep fold. The collapse deletes the depth-64 branch record, leaving an afterMap=0 tombstone on disk
// and carrying the survivor on the account leaf. On re-expansion the deep fold's unfoldStorageBase must
// not seed an empty base from that tombstone (which drops the survivor and diverges the root); it has
// to fall back so the storage rebuilds from the account leaf's carried survivor, matching sequential.
func TestDeepFold_SingleSlotCollapseThenReexpand(t *testing.T) {
	t.Parallel()

	addr, _, _, _, wk1, wu1, groups := whaleByNibble(30_000)

	surv := -1
	for x := 0; x < 16; x++ {
		if len(groups[x]) >= 1 {
			surv = x
			break
		}
	}
	require.GreaterOrEqual(t, surv, 0, "need a survivor nibble with >=1 slot")

	// Batch 2: touch the account and delete every slot except one, collapsing the storage onto a single
	// surviving slot. Collect the deleted slots to re-add.
	wk2 := [][]byte{addr}
	wu2 := []Update{{Flags: BalanceUpdate | NonceUpdate}}
	wu2[0].Balance.SetUint64(99)
	wu2[0].Nonce = 7
	var reAdd []storKV
	for x := 0; x < 16; x++ {
		for j, kv := range groups[x] {
			if x == surv && j == 0 {
				continue // keep exactly one slot alive
			}
			wk2 = append(wk2, kv.pk)
			wu2 = append(wu2, Update{Flags: DeleteUpdate})
			reAdd = append(reAdd, kv)
		}
	}
	require.Greater(t, len(reAdd), int(deepStorageThreshold),
		"re-expansion must cross deepStorageThreshold to exercise the deep fold")

	// Batch 3: re-add > deepStorageThreshold of the deleted slots across many nibbles, re-expanding the
	// storage from the tombstone the collapse left.
	wk3 := [][]byte{}
	wu3 := []Update{}
	for i := 0; i < len(reAdd) && i < int(deepStorageThreshold)+500; i++ {
		wk3 = append(wk3, reAdd[i].pk)
		wu3 = append(wu3, reAdd[i].upd)
	}

	// Surround the whale with a dense account trie so its account leaf is a child in a branch record.
	mk, mu := buildMixedCorpus(0xC0FFEE, 4000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)

	batches := []engineBatch{{k1, u1}, {wk2, wu2}, {wk3, wu3}}

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
