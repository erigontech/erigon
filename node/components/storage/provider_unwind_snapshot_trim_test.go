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
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// stubAggregator is a minimal StateAggregator that satisfies the
// interface for tests where we only need a non-nil sentinel.
// StepSize returns 390625 (the hoodi/mainnet step size); other methods
// are inert.
type stubAggregator struct{}

func (stubAggregator) Files() []string                                     { return nil }
func (stubAggregator) OpenFolder() error                                   { return nil }
func (stubAggregator) BuildMissedAccessors(_ context.Context, _ int) error { return nil }
func (stubAggregator) LockCollation()                                      {}
func (stubAggregator) UnlockCollation()                                    {}
func (stubAggregator) StepSize() uint64                                    { return 390625 }
func (stubAggregator) WipeWritableShadowPast(_ context.Context, _ kv.TemporalRwTx, _ uint64) error {
	return nil
}

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

// TestCollectFilesPastBlock_StateDomainFilesPastStepBoundary pins the
// state-domain trim contract that was MISSING coverage before and was
// the root cause of the post-mode-B catch-up wedge surfaced live on
// hoodi 2026-06-02: state-domain files whose ToStep > stepBoundary
// MUST be collected for removal, otherwise SeekCommitment sees the
// snapshot's KeyCommitmentState at a higher txNum than the writable
// shadow's mode-B anchor and returns ErrBehindCommitment → catch-up
// downloader gives up → chain wedges at the unwind target.
//
// Aggregator is set to a sentinel (any non-nil value) because the
// collect function only uses it as a "state trim is in scope" guard.
func TestCollectFilesPastBlock_StateDomainFilesPastStepBoundary(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	files := []*snapshot.FileEntry{
		// State files at step ranges spanning the boundary.
		// Boundary step = 266 (kept). Past steps = 267+.
		{Name: "domain/v1.0-commitment.0-256.kv", Domain: "commitment", FromStep: 0, ToStep: 256, Local: true},
		{Name: "domain/v1.0-commitment.256-264.kv", Domain: "commitment", FromStep: 256, ToStep: 264, Local: true},
		{Name: "domain/v1.0-commitment.264-266.kv", Domain: "commitment", FromStep: 264, ToStep: 266, Local: true},
		{Name: "domain/v1.0-commitment.266-267.kv", Domain: "commitment", FromStep: 266, ToStep: 267, Local: true},
		{Name: "domain/v1.0-accounts.266-267.kv", Domain: "accounts", FromStep: 266, ToStep: 267, Local: true},
		{Name: "domain/v1.0-accounts.264-266.kv", Domain: "accounts", FromStep: 264, ToStep: 266, Local: true},
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	// Aggregator sentinel — collectFilesPastBlock only checks for non-nil.
	// Any non-zero-pointer value works.
	p := &Provider{Inventory: inv, Aggregator: stubAggregator{}}
	out := p.collectFilesPastBlock(2_912_500, 266)

	got := make([]string, len(out))
	for i, e := range out {
		got[i] = e.Name
	}
	sort.Strings(got)

	require.Equal(t,
		[]string{
			"domain/v1.0-accounts.266-267.kv",
			"domain/v1.0-commitment.266-267.kv",
		},
		got,
		"state-domain files with ToStep > stepBoundary must be collected; files at the boundary step or below stay")
}
