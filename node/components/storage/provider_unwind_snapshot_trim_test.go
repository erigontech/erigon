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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// stubAggregator is a minimal StateAggregator that satisfies the
// interface for tests where we only need a non-nil sentinel.
// StepSize returns 390625 (the hoodi/mainnet step size); other methods
// are inert.
type stubAggregator struct{}

func (stubAggregator) Files() []string   { return nil }
func (stubAggregator) OpenFolder() error { return nil }
func (stubAggregator) BuildMissedAccessors(_ context.Context, _ int, _ ...kv.BuildAccessorsOption) error {
	return nil
}
func (stubAggregator) LockCollation()   {}
func (stubAggregator) UnlockCollation() {}
func (stubAggregator) StepSize() uint64 { return 390625 }
func (stubAggregator) WipeWritableShadowPast(_ context.Context, _ kv.TemporalRwTx, _ uint64) error {
	return nil
}
func (stubAggregator) HistoryCompressions(_ kv.Domain) (seg.FileCompression, seg.FileCompression) {
	return seg.CompressNone, seg.CompressNone
}
func (stubAggregator) DomainCompression(_ kv.Domain) seg.FileCompression {
	return seg.CompressNone
}
func (stubAggregator) Unwind(_ uint64)            {}
func (stubAggregator) SetUnwindInProgress(_ bool) {}
func (stubAggregator) WaitForBuildAndMergeQuiescence(_ time.Duration) error {
	return nil
}
func (stubAggregator) DomainKVFilePathV4(_ kv.Domain, _, _ uint64) string { return "" }
func (stubAggregator) DomainKVFilePath(_ kv.Domain, _, _ kv.Step) string  { return "" }
func (stubAggregator) BuildKVAccessors(_ context.Context, _ kv.Domain, _, _ string) error {
	return nil
}
func (stubAggregator) HistoryFilePathV4(_ kv.Domain, _, _ uint64) string { return "" }
func (stubAggregator) EFFilePathV4(_ kv.Domain, _, _ uint64) string      { return "" }
func (stubAggregator) BuildHistoryAccessors(_ context.Context, _ kv.Domain, _, _, _ string) error {
	return nil
}
func (stubAggregator) BuildIndexAccessors(_ context.Context, _ kv.Domain, _, _ string) error {
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
	out := p.collectFilesPastBlock(2_912_999, 0, 0)

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
	out := p.collectFilesPastBlock(2_912_999, 0, 0)

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
	out := p.collectFilesPastBlock(2_910_000, 0, 0)

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
	out := p.collectFilesPastBlock(2_912_500, 266, 0)

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

// TestCollectFilesPastBlock_StraddleStateFilePreserved pins the
// fix to the soak v14 iter-3 wedge: when the aggregator has merged
// step files into wider chunks, the file STRADDLING stepBoundary
// (FromStep < stepBoundary AND ToStep > stepBoundary) must NOT be
// collected for removal. The regen path truncates it in place —
// destroying it would leave the unwind with
// no commitment anchor → Caplin's catchup wedges on `behind
// commitment`.
//
// Pre-fix predicate `e.ToStep > stepBoundary` swept the straddle
// file alongside entirely-past files. Post-fix predicate `e.FromStep
// >= stepBoundary` only collects files whose entire range lies past
// the boundary, leaving the straddle for regen to truncate in place.
func TestCollectFilesPastBlock_StraddleStateFilePreserved(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	// Match the v14 iter-3 layout: stepBoundary=275 falls inside
	// the merged 272-276 chunk. The 272-276 file MUST stay (regen
	// will truncate it in place at lastTxNum); the 276-280 file is
	// entirely past the boundary and MUST be removed.
	files := []*snapshot.FileEntry{
		{Name: "domain/v2.0-commitment.264-272.kv", Domain: snapshot.DomainCommitment, FromStep: 264, ToStep: 272, Local: true},
		{Name: "domain/v2.0-commitment.272-276.kv", Domain: snapshot.DomainCommitment, FromStep: 272, ToStep: 276, Local: true}, // STRADDLE
		{Name: "domain/v2.0-commitment.276-280.kv", Domain: snapshot.DomainCommitment, FromStep: 276, ToStep: 280, Local: true}, // PAST
		{Name: "domain/v2.0-accounts.272-276.kv", Domain: snapshot.DomainAccounts, FromStep: 272, ToStep: 276, Local: true},     // STRADDLE
		{Name: "domain/v2.0-accounts.276-280.kv", Domain: snapshot.DomainAccounts, FromStep: 276, ToStep: 280, Local: true},     // PAST
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv, Aggregator: stubAggregator{}}
	out := p.collectFilesPastBlock(3_006_443, 275, 0)

	got := make([]string, len(out))
	for i, e := range out {
		got[i] = e.Name
	}
	sort.Strings(got)

	require.Equal(t,
		[]string{
			"domain/v2.0-accounts.276-280.kv",
			"domain/v2.0-commitment.276-280.kv",
		},
		got,
		"straddle files (FromStep < stepBoundary ≤ ToStep) MUST be preserved for regen; "+
			"only entirely-past files (FromStep >= stepBoundary) are trimmed")
}

// TestCollectFilesPastBlock_StateFileEntirelyPastStillTrimmed is the
// regression-safety twin of the straddle test: a file whose entire
// step range is past stepBoundary (FromStep >= stepBoundary) MUST
// still be collected. This pins that the broadened "leave the
// straddle" predicate doesn't accidentally exempt non-straddle past
// files.
func TestCollectFilesPastBlock_StateFileEntirelyPastStillTrimmed(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	files := []*snapshot.FileEntry{
		{Name: "domain/v2.0-commitment.275-280.kv", Domain: snapshot.DomainCommitment, FromStep: 275, ToStep: 280, Local: true}, // FromStep == stepBoundary → strictly past
		{Name: "domain/v2.0-commitment.280-288.kv", Domain: snapshot.DomainCommitment, FromStep: 280, ToStep: 288, Local: true}, // FromStep > stepBoundary
	}
	for _, e := range files {
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv, Aggregator: stubAggregator{}}
	out := p.collectFilesPastBlock(3_006_443, 275, 0)

	got := make([]string, len(out))
	for i, e := range out {
		got[i] = e.Name
	}
	sort.Strings(got)

	require.Equal(t,
		[]string{
			"domain/v2.0-commitment.275-280.kv",
			"domain/v2.0-commitment.280-288.kv",
		},
		got,
		"files with FromStep >= stepBoundary lie entirely past the boundary and must be trimmed")
}

// TestCollectFilesPastBlock_V4StateFilesPastBoundaryCollected pins that v4
// state files are trimmed when they lie past the unwind target. They are
// named by txNum and registered without a step axis, so a step comparison
// never selects them. Left visible, a v4 commitment file above the target
// makes SeekCommitment resume past it while the other domains were rewound:
// execution skips the gap and the first block after it fails on gas used.
func TestCollectFilesPastBlock_V4StateFilesPastBoundaryCollected(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	for _, name := range []string{
		"domain/v2.2-commitment.256-319.kv",
		// v4 #1 cut at the target (lastTxNum 124947128): stays.
		"domain/v4.0-commitment.124609375-124947129.kv",
		// Produced by an earlier, shallower unwind: entirely past the target.
		"domain/v4.0-commitment.126562500-126579863.kv",
		"domain/v4.0-commitment.126579863-126953125.kv",
		"domain/v4.0-accounts.126562500-126579863.kv",
	} {
		e := &snapshot.FileEntry{Name: name, Local: true}
		snapshot.PopulateFromName(e)
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv, Aggregator: stubAggregator{}}
	out := p.collectFilesPastBlock(3_543_422, 320, 124_947_128)

	got := make([]string, len(out))
	for i, e := range out {
		got[i] = e.Name
	}
	sort.Strings(got)
	require.Equal(t,
		[]string{
			"domain/v4.0-accounts.126562500-126579863.kv",
			"domain/v4.0-commitment.126562500-126579863.kv",
			"domain/v4.0-commitment.126579863-126953125.kv",
		},
		got,
		"v4 state files past the unwind target must be trimmed; the v4 file cut at the target stays")
}

// TestCollectFilesPastBlock_V4TailPastTargetInBoundaryStepCollected pins that
// a v4 tail starting inside the boundary step but after the target is trimmed.
// Unwinds cut v4 files mid-step, so a later, deeper unwind can land in the
// same step ahead of such a tail; a step comparison keeps it even though all
// of its content lies past the target.
func TestCollectFilesPastBlock_V4TailPastTargetInBoundaryStepCollected(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	for _, name := range []string{
		// v4 #1 straddling the target (lastTxNum 126570000): stays for regen.
		"domain/v4.0-commitment.126562500-126579863.kv",
		// v4 #2 tail in the same step, starting past the target.
		"domain/v4.0-commitment.126579863-126953125.kv",
	} {
		e := &snapshot.FileEntry{Name: name, Local: true}
		snapshot.PopulateFromName(e)
		require.NoError(t, inv.AddFile(e))
	}

	p := &Provider{Inventory: inv, Aggregator: stubAggregator{}}
	out := p.collectFilesPastBlock(3_605_000, 325, 126_570_000)

	got := make([]string, len(out))
	for i, e := range out {
		got[i] = e.Name
	}
	require.Equal(t,
		[]string{"domain/v4.0-commitment.126579863-126953125.kv"},
		got,
		"a v4 tail wholly past the target must be trimmed even inside the boundary step")
}
