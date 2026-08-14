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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeepFold_SingleSlotCollapseThenDeepReexpand(t *testing.T) {
	t.Parallel()

	const seed = 900 // < deepStorageThreshold: batch 1 and the batch-2 collapse both stream
	addr, _, _, _, wk1, wu1, groups := whaleByNibble(seed)
	a := hex.EncodeToString(addr)

	survNib := -1
	for x := range 16 {
		if len(groups[x]) >= 1 {
			survNib = x
			break
		}
	}
	require.GreaterOrEqual(t, survNib, 0, "need a survivor nibble")

	mk, mu := buildMixedCorpus(0xC0FFEE, 4000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)

	wk2 := [][]byte{addr}
	wu2 := []Update{{Flags: BalanceUpdate | NonceUpdate}}
	wu2[0].Balance.SetUint64(99)
	wu2[0].Nonce = 7
	deleted := 0
	kept := false
	for x := range 16 {
		for _, kv := range groups[x] {
			if !kept {
				kept = true
				continue
			}
			wk2 = append(wk2, kv.pk)
			wu2 = append(wu2, Update{Flags: DeleteUpdate})
			deleted++
		}
	}
	require.True(t, kept, "need a survivor slot")
	require.LessOrEqual(t, deleted, int(deepStorageThreshold), "collapse must stream, not deep-fold")

	b3 := NewUpdateBuilder()
	b3.Balance(a, 200)
	added := 0
	for nib := 0; nib < 16 && added <= int(deepStorageThreshold)+200; nib++ {
		if nib == survNib {
			continue
		}
		for _, loc := range storageLocsForNibble(byte(nib), 200, nib*1_000_003+7) {
			b3.Storage(a, loc, "01")
			added++
		}
	}
	require.Greater(t, added, int(deepStorageThreshold), "re-expansion must cross the deep-fold threshold")
	wk3, wu3 := b3.Build()

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
