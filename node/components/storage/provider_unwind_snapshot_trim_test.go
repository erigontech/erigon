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

package storage

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestCollectFilesPastBlock_StraddleFileSurvives pins the contract
// that fixed live-rig issue #2 from the 2026-06-01 cycle: the block
// snapshot file whose range straddles toBlock (FromBlock ≤ toBlock <
// ToBlock) MUST NOT be removed. The straddle file still holds the
// headers / bodies for blocks in [FromBlock, toBlock]; removing it
// strands those blocks (snapshot trimmed + writable DB doesn't carry
// them post-OtterSync) and the next BlockReader.HeaderByNumber for
// any of those blocks returns nil.
//
// Pre-fix, collectFilesPastBlock used `e.ToBlock > toBlock` which
// trimmed the straddle file alongside the strictly-past files. The
// secondary mode-B failure on hoodi (debug_setHead 2,912,999 →
// 2,912,500 after a first mode-B to 2,912,999 had trimmed the
// 002910-002920 headers file) surfaced this directly with "no header
// for block 2912500".
//
// The fix flips the criterion to `e.FromBlock > toBlock` — straddle
// files stay, strictly-past files go. The writable DB's
// CanonicalHash truncation (already in unwindDBPastBlock) gates the
// straddle file's post-toBlock content from being visible via
// canonical lookup.
func TestCollectFilesPastBlock_StraddleFileSurvives(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	// Three 10K-block header files representative of the hoodi
	// failure shape: the second straddles toBlock = 2,912,999.
	files := []*snapshot.FileEntry{
		{Name: "v1.1-002900-002910-headers.seg", FromBlock: 2_900_000, ToBlock: 2_910_000, Local: true},
		{Name: "v1.1-002910-002920-headers.seg", FromBlock: 2_910_000, ToBlock: 2_920_000, Local: true}, // STRADDLE
		{Name: "v1.1-002920-002930-headers.seg", FromBlock: 2_920_000, ToBlock: 2_930_000, Local: true}, // PAST
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv}
	// Aggregator stays nil; state files are out of scope for this
	// block-trim test.
	out := p.collectFilesPastBlock(2_912_999, 0)

	got := make([]string, len(out))
	for i, e := range out {
		got[i] = e.Name
	}
	sort.Strings(got)

	require.Equal(t,
		[]string{"v1.1-002920-002930-headers.seg"},
		got,
		"only files whose FromBlock > toBlock should be trimmed; "+
			"the 002910-002920 file straddles toBlock=2,912,999 "+
			"(FromBlock=2,910,000 ≤ 2,912,999 < ToBlock=2,920,000) "+
			"and must stay so blocks 2,910,000..2,912,999 remain "+
			"readable via BlockReader.HeaderByNumber")
}

// TestCollectFilesPastBlock_ExactBoundaryStays pins the boundary
// case: a file whose FromBlock == toBlock+1 is strictly past — its
// first block is one beyond the new tip — and must be trimmed. A
// file whose ToBlock == toBlock+1 stays (it covers toBlock).
func TestCollectFilesPastBlock_ExactBoundaryStays(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	files := []*snapshot.FileEntry{
		// File covering exactly [2_900_000, 2_913_000): ToBlock = toBlock+1
		// → contains blocks up to toBlock=2,912,999 → STAY.
		{Name: "stay-up-to-toblock.seg", FromBlock: 2_900_000, ToBlock: 2_913_000, Local: true},
		// File starting exactly at toBlock+1 → strictly past → REMOVE.
		{Name: "go-from-toblock-plus-one.seg", FromBlock: 2_913_000, ToBlock: 2_920_000, Local: true},
		// File whose FromBlock == toBlock → straddles (covers toBlock + post-toBlock) → STAY.
		{Name: "stay-fromblock-eq-toblock.seg", FromBlock: 2_912_999, ToBlock: 2_915_000, Local: true},
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv}
	out := p.collectFilesPastBlock(2_912_999, 0)

	got := make([]string, len(out))
	for i, e := range out {
		got[i] = e.Name
	}
	sort.Strings(got)

	require.Equal(t,
		[]string{"go-from-toblock-plus-one.seg"},
		got,
		"only the file whose FromBlock > toBlock should be trimmed; "+
			"FromBlock == toBlock means the file's first block IS toBlock "+
			"(its content is needed); FromBlock == toBlock+1 is the first "+
			"file strictly past the new tip")
}

// TestCollectFilesPastBlock_AllPastRemoved pins that when no file
// straddles, every past file is collected (no false negatives).
func TestCollectFilesPastBlock_AllPastRemoved(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	files := []*snapshot.FileEntry{
		{Name: "in-range.seg", FromBlock: 2_900_000, ToBlock: 2_910_000, Local: true},
		{Name: "past-a.seg", FromBlock: 2_920_000, ToBlock: 2_930_000, Local: true},
		{Name: "past-b.seg", FromBlock: 2_930_000, ToBlock: 2_940_000, Local: true},
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv}
	out := p.collectFilesPastBlock(2_910_000, 0)

	got := make([]string, len(out))
	for i, e := range out {
		got[i] = e.Name
	}
	sort.Strings(got)

	require.Equal(t,
		[]string{"past-a.seg", "past-b.seg"},
		got,
		"every file whose FromBlock > toBlock must be collected; in-range stays")
}
