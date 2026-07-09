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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUnifiedDispatch_ParallelMatchesStreaming pins that routing ModeParallel (processMounted,
// scheduler-never-started) through the same deriveFoldFrontier + fold pool as the streaming Process
// path (scheduler mechanics untouched) yields a byte-identical result: equal root AND equal stored
// branches after every batch of an N>=3 chain over the four fold-DAG shapes. The two paths share one
// dispatch, so any divergence between them is a routing bug the per-mode oracle checks could miss if
// both drifted from the sequential trie the same way.
func TestUnifiedDispatch_ParallelMatchesStreaming(t *testing.T) {
	t.Parallel()
	corpora := []struct {
		name    string
		batches []engineBatch
	}{
		{"balanced", balancedBatches()},
		{"mega_whale", megaWhaleBatches(40_000)},
		{"delete_to_collapse", deleteToCollapseBatches()},
		{"extension_topped", extensionToppedBatches()},
	}
	for _, corpus := range corpora {
		t.Run(corpus.name, func(t *testing.T) {
			t.Parallel()
			for _, w := range []int{1, 4} {
				t.Run(fmt.Sprintf("w%d", w), func(t *testing.T) {
					parMs := NewMockState(t)
					parMs.SetConcurrentCommitment(true)
					strMs := NewMockState(t)
					strMs.SetConcurrentCommitment(true)

					var parBlob, strBlob []byte
					for i, b := range corpus.batches {
						var parRoot, strRoot []byte
						parRoot, parBlob = processModeBatchState(t, parMs, modeParallel, w, b.keys, b.upds, parBlob)
						strRoot, strBlob = processModeBatchState(t, strMs, modeStreaming, w, b.keys, b.upds, strBlob)
						require.Equalf(t, strRoot, parRoot, "batch %d: parallel root != streaming root", i+1)
						if mism := branchStoreMismatches(strMs, parMs); len(mism) != 0 {
							branchDiff(t, strMs, parMs)
							t.Fatalf("batch %d: parallel vs streaming branch store differs (%d prefixes)", i+1, len(mism))
						}
					}
				})
			}
		})
	}
}
