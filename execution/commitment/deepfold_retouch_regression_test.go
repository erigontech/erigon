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

// A whale's touched storage collapses to a single surviving first-nibble (deep fold, single-child),
// then a LATER block re-touches its storage. The deep fold persists the collapsed storage root as a
// bare hash on the account leaf, dropping the extension navigation, so the re-touch unfold demands a
// storage-root branch record at the 64-nibble account prefix that the single-child collapse never
// wrote — "empty branch data read during unfold" on the parallel/streaming engines. Sequential folds
// the storage in place (fillFromLowerCell keeps the extension) and stays correct, so this only
// surfaces on a re-touch after the collapse — the existing collapse tests stop at the collapse block.
func TestDeepFold_SurvivorCollapseThenRetouch(t *testing.T) {
	t.Parallel()

	addr, _, _, _, wk1, wu1, groups := whaleByNibble(30_000)

	surv := -1
	for x := range 16 {
		if len(groups[x]) >= 2 {
			surv = x
			break
		}
	}
	require.GreaterOrEqual(t, surv, 0, "need a survivor nibble with >=2 slots")

	// Batch 2: touch the account and delete every non-survivor slot (>>deepStorageThreshold), so the
	// storage root collapses onto the single survivor nibble. Remember one deleted slot to re-add.
	wk2 := [][]byte{addr}
	wu2 := []Update{{Flags: BalanceUpdate | NonceUpdate}}
	wu2[0].Balance.SetUint64(99)
	wu2[0].Nonce = 7
	var reAdd storKV
	haveReAdd := false
	for x := range 16 {
		if x == surv {
			continue
		}
		for _, kv := range groups[x] {
			wk2 = append(wk2, kv.pk)
			wu2 = append(wu2, Update{Flags: DeleteUpdate})
			if !haveReAdd {
				reAdd = kv
				haveReAdd = true
			}
		}
	}
	require.True(t, haveReAdd, "need a deleted slot to re-add")

	// Batch 3: re-add the deleted slot under its (non-survivor) nibble, re-expanding the storage — this
	// unfolds the account's storage subtree from the persisted (collapsed) state.
	wk3 := [][]byte{reAdd.pk}
	wu3 := []Update{reAdd.upd}

	// Surround the whale with a dense account trie so it is a child in a branch record whose persisted
	// cell must carry the storage extension.
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
