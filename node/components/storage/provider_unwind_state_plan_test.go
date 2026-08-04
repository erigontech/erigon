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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// classifyStateFileForUnwind: per-file rule tests. Each case is one
// (FromStep, ToStep, stepBoundary, boundaryAligned) tuple and the
// expected action. Boundary-value coverage at each of the four
// action regions. boundaryAligned=true means lastTxNum+1 lands
// exactly on a step edge (stepBoundary*stepSize); false means the
// unwind target is mid-step within [(stepBoundary-1)*stepSize,
// stepBoundary*stepSize).
func TestClassifyStateFileForUnwind(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		from, to uint64
		boundary uint64
		aligned  bool
		want     stateFileAction
	}{
		// === actionKeep: ToStep <= stepBoundary - 1 (file entirely below boundary)
		{"keep — well below boundary", 0, 256, 278, true, actionKeep},
		{"keep — ToStep one below boundary", 272, 277, 278, true, actionKeep},
		{"keep — equal-step file deep below", 100, 101, 278, true, actionKeep},
		// Alignment irrelevant for below-boundary — file's content is entirely valid.
		{"keep — mid-step alignment doesn't change below-boundary verdict", 272, 277, 278, false, actionKeep},

		// === actionRegenInPlace: ToStep == stepBoundary AND target lands exactly on step edge
		{"in-place — ToStep equals boundary, narrow file, aligned", 277, 278, 278, true, actionRegenInPlace},
		{"in-place — ToStep equals boundary, broad file, aligned", 272, 278, 278, true, actionRegenInPlace},
		{"in-place — boundary at 0 + simple aligned file", 0, 1, 1, true, actionRegenInPlace},

		// === actionRegenTruncate: mid-step boundary reclassifies aligned-ToStep as straddler
		// (the file's [(ToStep-1)*stepSize, ToStep*stepSize) range spans past lastTxNum's mid-step position).
		// This is the load-bearing rule for the mode-C completeness bug: a mid-step mode-B unwind
		// target must NOT overwrite the step-aligned .kv name; emit a v4 mid-step file instead.
		{"truncate — ToStep equals boundary but mid-step target (mode-C completeness)", 302, 303, 303, false, actionRegenTruncate},
		{"truncate — broad boundary-file at mid-step target", 272, 278, 278, false, actionRegenTruncate},

		// === actionRegenTruncate: FromStep < stepBoundary < ToStep (topological straddler)
		{"truncate — broad straddler crosses boundary, aligned", 272, 280, 278, true, actionRegenTruncate},
		{"truncate — narrow straddler, aligned", 277, 280, 278, true, actionRegenTruncate},
		{"truncate — broad straddler crosses boundary, mid-step", 272, 280, 278, false, actionRegenTruncate},
		{"truncate — boundary near range start", 272, 273, 272, true, actionRemove}, // F == boundary → remove, not truncate

		// === actionRemove: FromStep >= stepBoundary (entirely past)
		{"remove — entirely past boundary", 280, 284, 278, true, actionRemove},
		{"remove — starts at boundary step", 278, 279, 278, true, actionRemove},
		{"remove — far past boundary", 500, 600, 278, true, actionRemove},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyStateFileForUnwind(stateFileRange{tc.from, tc.to}, tc.boundary, tc.aligned)
			require.Equal(t, tc.want, got,
				"file [%d, %d) at stepBoundary=%d aligned=%v", tc.from, tc.to, tc.boundary, tc.aligned)
		})
	}
}

// planStateFileActions: scenario-level tests. Each scenario pins the
// expected post-unwind file-set transformation for a realistic
// boundary file shape that the soak has surfaced.

// TestPlan_NoFilesPastBoundary: clean below-the-boundary case. No-op.
func TestPlan_NoFilesPastBoundary(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
	}
	got := planStateFileActions(files, 278, true)
	require.Equal(t, files, got.keep)
	require.Empty(t, got.regen)
	require.Empty(t, got.remove)
}

// TestPlan_AlignedBoundary: the boundary lands exactly on the top
// file's ToStep — content needs as-of-lastTxNum rewrite but the
// filename doesn't change. This is the pre-truncated-rename behaviour
// and must keep working.
func TestPlan_AlignedBoundary(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
		{272, 278}, // exactly at boundary
	}
	got := planStateFileActions(files, 278, true)
	require.Equal(t, []stateFileRange{{0, 256}, {256, 272}}, got.keep)
	require.Equal(t, []stateFileRange{{272, 278}}, got.regen)
	require.Equal(t, []bool{true}, got.inPlace, "aligned boundary file must regen in place")
	require.Empty(t, got.remove)
}

// TestPlan_SingleStraddler: the truncated-rename case. Broad file
// straddles the boundary. Truncate to [FromStep, stepBoundary).
func TestPlan_SingleStraddler(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
		{272, 280}, // straddles 278
	}
	got := planStateFileActions(files, 278, true)
	require.Equal(t, []stateFileRange{{0, 256}, {256, 272}}, got.keep)
	require.Equal(t, []stateFileRange{{272, 280}}, got.regen)
	require.Equal(t, []bool{false}, got.inPlace, "straddler must regen with truncation")
	require.Empty(t, got.remove)
}

// TestPlan_FilesEntirelyPastBoundary: this is the iter-3 mode_b bug
// we caught live. The straddling broad gets regen'd, AND files
// entirely past the boundary must be REMOVED — they contain post-
// unwind-target state that's now stale. Pre-fix, only the
// boundary-file was touched and these entirely-past files persisted
// on disk serving stale reads.
func TestPlan_FilesEntirelyPastBoundary(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
		{272, 278}, // aligned — regen in place
		{278, 282}, // entirely past — REMOVE
		{282, 284}, // entirely past — REMOVE
	}
	got := planStateFileActions(files, 278, true)
	require.Equal(t, []stateFileRange{{0, 256}, {256, 272}}, got.keep)
	require.Equal(t, []stateFileRange{{272, 278}}, got.regen)
	require.Equal(t, []bool{true}, got.inPlace)
	require.Equal(t,
		[]stateFileRange{{278, 282}, {282, 284}}, got.remove,
		"all files entirely past boundary must be staged for removal")
}

// TestPlan_StraddlerPlusEntirelyPast: a mix — broad straddler that
// needs truncation, plus several files entirely past.
func TestPlan_StraddlerPlusEntirelyPast(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
		{272, 280}, // straddles
		{280, 282}, // entirely past
		{282, 284}, // entirely past
	}
	got := planStateFileActions(files, 278, true)
	require.Equal(t, []stateFileRange{{0, 256}, {256, 272}}, got.keep)
	require.Equal(t, []stateFileRange{{272, 280}}, got.regen)
	require.Equal(t, []bool{false}, got.inPlace)
	require.Equal(t, []stateFileRange{{280, 282}, {282, 284}}, got.remove)
}

// TestPlan_Iter3ModeBLayout reproduces the EXACT on-disk file layout
// from the 2026-06-30 iter-3 mode_b wedge. This is the load-bearing
// scenario the fix exists for. Layout was:
//
//	accounts.0-256, 256-272, 272-276, 272-280, 276-278, 278-279,
//	  280-282, 280-284
//
// With stepBoundary=278, the planner must:
//   - keep:    0-256, 256-272, 272-276
//   - regen aligned (in place): 276-278
//   - regen truncate: 272-280 → 272-278
//   - remove:  278-279, 280-282, 280-284
//
// Pre-fix the planner only regen'd ONE file per domain, leaving the
// entirely-past files on disk serving stale state. Reads at the first
// post-unwind block hit those and produced wrong gas (~4,800 over
// canonical for block 3,091,971 — typical EIP-3529 SSTORE refund
// miscalculation from a single wrong-original-value slot read).
func TestPlan_Iter3ModeBLayout(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
		{272, 276},
		{272, 280},
		{276, 278},
		{278, 279},
		{280, 282},
		{280, 284},
	}
	got := planStateFileActions(files, 278, true)

	require.Equal(t,
		[]stateFileRange{{0, 256}, {256, 272}, {272, 276}}, got.keep,
		"files entirely below boundary kept unchanged")

	require.Equal(t,
		[]stateFileRange{{272, 280}, {276, 278}}, got.regen,
		"broad straddler AND aligned-at-boundary file both regen — straddler truncates, aligned rewrites in place")
	require.Equal(t,
		[]bool{false, true}, got.inPlace,
		"272-280 truncates (false), 276-278 in-place (true)")

	require.Equal(t,
		[]stateFileRange{{278, 279}, {280, 282}, {280, 284}}, got.remove,
		"files entirely past boundary (incl. boundary-start) must be removed")
}

// TestPlan_MidStepBoundary_AlignedNameStraddles reproduces the leg P
// v6 iter 4 mode_b layout that landed corrupt per-step regen files on
// disk. Target lastTxNum was mid-step 302 (rounds up to stepBoundary
// 303), so the file [302, 303) was structurally aligned to boundary
// (ToStep==303) but its content genuinely spanned past the true
// endTxN. Under the pre-fix classifier this got actionRegenInPlace —
// the regen writer overwrote v2.0-<domain>.302-303.kv with content
// that only reflected state as-of the mid-step target, while the
// filename still advertised full-step coverage. That violated the
// mode-C completeness invariant.
//
// After the fix: the aligned-name file at stepBoundary is reclassified
// as a straddler (actionRegenTruncate) when the target is mid-step.
// The regen wires that through boundaryRegenFinalPath → v4 mid-step
// name so the advertised endTxN matches the content.
func TestPlan_MidStepBoundary_AlignedNameStraddles(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 288},
		{288, 296},
		{296, 300},
		{300, 302},
		{302, 303}, // aligned-name but content straddles the mid-step target
	}
	got := planStateFileActions(files, 303, false /* mid-step */)
	require.Equal(t,
		[]stateFileRange{{0, 256}, {256, 288}, {288, 296}, {296, 300}, {300, 302}},
		got.keep, "below-boundary files unaffected")
	require.Equal(t, []stateFileRange{{302, 303}}, got.regen)
	require.Equal(t, []bool{false}, got.inPlace,
		"mid-step target: aligned-name boundary file must regen with truncation (v4 mid-step emit)")
	require.Empty(t, got.remove)
}

// TestPlan_BoundaryAtZero: edge case — stepBoundary=0 means unwind to
// before-any-step. Every file is entirely past.
func TestPlan_BoundaryAtZero(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
	}
	got := planStateFileActions(files, 0, true)
	require.Empty(t, got.keep)
	require.Empty(t, got.regen)
	require.Equal(t, files, got.remove)
}

// TestPlan_BoundaryAboveAllFiles: stepBoundary past every file's
// ToStep. Nothing to do.
func TestPlan_BoundaryAboveAllFiles(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
		{272, 278},
	}
	got := planStateFileActions(files, 9999, true)
	require.Equal(t, files, got.keep)
	require.Empty(t, got.regen)
	require.Empty(t, got.remove)
}

// TestPlan_EmptyInput: defensive — no files at all (early-chain
// domain that hasn't retired yet). No-op.
func TestPlan_EmptyInput(t *testing.T) {
	t.Parallel()
	got := planStateFileActions(nil, 278, true)
	require.Empty(t, got.keep)
	require.Empty(t, got.regen)
	require.Empty(t, got.remove)
}

// overrideActionForDomain: per-file, per-domain override rule that
// applies AFTER classifyStateFileForUnwind. Applies IX-horizon
// policy under --prune.mode=minimal:
//   - Receipt: regen → actionRemove (forward-exec restores; keys re-
//     written every txN).
//   - Commitment: pass through — regen uses compute's captured
//     branches via WriteCommitmentBoundaryFileV4 (mode-C v4 emit),
//     not per-key AsOf, so IX horizon doesn't apply.
//   - Accounts/storage/code: error (silent removal would lose state).
//   - actionKeep / actionRemove: no AsOf, pass through.
func TestOverrideActionForDomain(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		action         stateFileAction
		domain         kv.Domain
		ixCoversTarget bool
		wantAction     stateFileAction
		wantErr        bool
	}{
		// Commitment truncate passes through — mode-C v4 emit uses
		// compute's captured branches, no per-key AsOf, so IX-horizon
		// pass-through applies (see WriteCommitmentBoundaryFileV4).
		{"commitment truncate unchanged (ix covered)", actionRegenTruncate, kv.CommitmentDomain, true, actionRegenTruncate, false},
		{"commitment truncate unchanged (ix pruned)", actionRegenTruncate, kv.CommitmentDomain, false, actionRegenTruncate, false},

		// Commitment aligned in-place stays as regen (anchor replacement).
		{"commitment regen-in-place unchanged (ix covered)", actionRegenInPlace, kv.CommitmentDomain, true, actionRegenInPlace, false},
		{"commitment regen-in-place unchanged (ix pruned)", actionRegenInPlace, kv.CommitmentDomain, false, actionRegenInPlace, false},

		// Commitment keep/remove pass through.
		{"commitment keep unchanged", actionKeep, kv.CommitmentDomain, true, actionKeep, false},
		{"commitment remove unchanged", actionRemove, kv.CommitmentDomain, false, actionRemove, false},

		// IX covers target: non-commitment pass-through.
		{"covered: receipt regen-in-place", actionRegenInPlace, kv.ReceiptDomain, true, actionRegenInPlace, false},
		{"covered: accounts regen-truncate", actionRegenTruncate, kv.AccountsDomain, true, actionRegenTruncate, false},
		{"covered: receipt keep", actionKeep, kv.ReceiptDomain, true, actionKeep, false},
		{"covered: storage remove", actionRemove, kv.StorageDomain, true, actionRemove, false},

		// IX doesn't cover: receipt regen actions become remove.
		{"pruned: receipt regen-in-place → remove", actionRegenInPlace, kv.ReceiptDomain, false, actionRemove, false},
		{"pruned: receipt regen-truncate → remove", actionRegenTruncate, kv.ReceiptDomain, false, actionRemove, false},

		// IX doesn't cover: receipt keep/remove pass through (no AsOf needed).
		{"pruned: receipt keep unchanged", actionKeep, kv.ReceiptDomain, false, actionKeep, false},
		{"pruned: receipt remove unchanged", actionRemove, kv.ReceiptDomain, false, actionRemove, false},

		// IX doesn't cover: non-receipt/non-commitment regen actions error.
		{"pruned: accounts regen-in-place errors", actionRegenInPlace, kv.AccountsDomain, false, 0, true},
		{"pruned: storage regen-truncate errors", actionRegenTruncate, kv.StorageDomain, false, 0, true},
		{"pruned: code regen-in-place errors", actionRegenInPlace, kv.CodeDomain, false, 0, true},

		// IX doesn't cover: non-receipt keep/remove pass through (no AsOf needed).
		{"pruned: accounts keep unchanged", actionKeep, kv.AccountsDomain, false, actionKeep, false},
		{"pruned: storage remove unchanged", actionRemove, kv.StorageDomain, false, actionRemove, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := overrideActionForDomain(tc.action, tc.domain, tc.ixCoversTarget)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantAction, got)
		})
	}
}
