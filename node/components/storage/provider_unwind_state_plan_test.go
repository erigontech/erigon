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
// (FromStep, ToStep, stepBoundary) tuple and the expected action.
// Boundary-value coverage at each of the four action regions.
func TestClassifyStateFileForUnwind(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		from, to uint64
		boundary uint64
		want     stateFileAction
	}{
		// === actionKeep: ToStep <= stepBoundary - 1 (file entirely below boundary)
		{"keep — well below boundary", 0, 256, 278, actionKeep},
		{"keep — ToStep one below boundary", 272, 277, 278, actionKeep},
		{"keep — equal-step file deep below", 100, 101, 278, actionKeep},

		// === actionRegenInPlace: ToStep == stepBoundary (aligned at boundary)
		{"in-place — ToStep equals boundary, narrow file", 277, 278, 278, actionRegenInPlace},
		{"in-place — ToStep equals boundary, broad file", 272, 278, 278, actionRegenInPlace},
		{"in-place — boundary at 0 + simple aligned file", 0, 1, 1, actionRegenInPlace},

		// === actionRegenTruncate: FromStep < stepBoundary < ToStep (straddler)
		{"truncate — broad straddler crosses boundary", 272, 280, 278, actionRegenTruncate},
		{"truncate — narrow straddler", 277, 280, 278, actionRegenTruncate},
		{"truncate — boundary near range start", 272, 273, 272, actionRemove}, // F == boundary → remove, not truncate

		// === actionRemove: FromStep >= stepBoundary (entirely past)
		{"remove — entirely past boundary", 280, 284, 278, actionRemove},
		{"remove — starts at boundary step", 278, 279, 278, actionRemove},
		{"remove — far past boundary", 500, 600, 278, actionRemove},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyStateFileForUnwind(stateFileRange{tc.from, tc.to}, tc.boundary)
			require.Equal(t, tc.want, got,
				"file [%d, %d) at stepBoundary=%d", tc.from, tc.to, tc.boundary)
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
	got := planStateFileActions(files, 278)
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
	got := planStateFileActions(files, 278)
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
	got := planStateFileActions(files, 278)
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
	got := planStateFileActions(files, 278)
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
	got := planStateFileActions(files, 278)
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
	got := planStateFileActions(files, 278)

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

// TestPlan_BoundaryAtZero: edge case — stepBoundary=0 means unwind to
// before-any-step. Every file is entirely past.
func TestPlan_BoundaryAtZero(t *testing.T) {
	t.Parallel()
	files := []stateFileRange{
		{0, 256},
		{256, 272},
	}
	got := planStateFileActions(files, 0)
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
	got := planStateFileActions(files, 9999)
	require.Equal(t, files, got.keep)
	require.Empty(t, got.regen)
	require.Empty(t, got.remove)
}

// TestPlan_EmptyInput: defensive — no files at all (early-chain
// domain that hasn't retired yet). No-op.
func TestPlan_EmptyInput(t *testing.T) {
	t.Parallel()
	got := planStateFileActions(nil, 278)
	require.Empty(t, got.keep)
	require.Empty(t, got.regen)
	require.Empty(t, got.remove)
}

// overrideActionForIXHorizon: per-file, per-domain override rule that
// applies AFTER classifyStateFileForUnwind. When the domain's history
// index (IX) has been pruned past the unwind target's txN, regen's
// per-key AsOf lookup cannot answer — the operator asked to unwind
// past the retained-history horizon under --prune.mode=minimal.
//
// Receipt domain: safe to resolve by remove-and-rebuild. Receipt keys
// (cumGas, cumBlobGas, logIdx) are re-written on every txN, so
// forward-exec from target restores every value. The pre-unwind
// boundary .kv is stale (its latest values reflect a txN past the
// target); removing it lets retire produce a fresh .kv from the
// re-executed MDBX rows once forward-exec has progressed.
//
// Accounts / storage / code: return an error. Silent removal would
// lose state for keys last written pre-target and never touched
// since — forward-exec from target cannot resurrect that state.
//
// Commitment: passes through unchanged. Commitment regen uses an
// encoded anchor (not per-key AsOf) so IX horizon doesn't apply.
//
// actionKeep / actionRemove: no AsOf lookup performed, always pass
// through regardless of IX coverage.
func TestOverrideActionForIXHorizon(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		action         stateFileAction
		domain         kv.Domain
		ixCoversTarget bool
		wantAction     stateFileAction
		wantErr        bool
	}{
		// IX covers target: pass-through regardless of action/domain.
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

		// IX doesn't cover: non-receipt regen actions error.
		{"pruned: accounts regen-in-place errors", actionRegenInPlace, kv.AccountsDomain, false, 0, true},
		{"pruned: storage regen-truncate errors", actionRegenTruncate, kv.StorageDomain, false, 0, true},
		{"pruned: code regen-in-place errors", actionRegenInPlace, kv.CodeDomain, false, 0, true},

		// IX doesn't cover: non-receipt keep/remove pass through (no AsOf needed).
		{"pruned: accounts keep unchanged", actionKeep, kv.AccountsDomain, false, actionKeep, false},
		{"pruned: storage remove unchanged", actionRemove, kv.StorageDomain, false, actionRemove, false},

		// Commitment: regen uses encoded anchor, not per-key AsOf.
		{"pruned: commitment regen-in-place unchanged", actionRegenInPlace, kv.CommitmentDomain, false, actionRegenInPlace, false},
		{"pruned: commitment regen-truncate unchanged", actionRegenTruncate, kv.CommitmentDomain, false, actionRegenTruncate, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := overrideActionForIXHorizon(tc.action, tc.domain, tc.ixCoversTarget)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantAction, got)
		})
	}
}
