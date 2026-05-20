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

package state_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// pickLargestCommitmentFile returns the commitment file with the largest tx
// range (and so the most pairs) from the aggregator. Tests want this so they
// have enough non-state branches to drive the detection samples and exercise
// every code path inside convertCommitmentFile.
func pickLargestCommitmentFile(t *testing.T, at *state.AggregatorRoTx) kv.VisibleFile {
	t.Helper()
	files := at.Files(kv.CommitmentDomain)
	require.NotEmpty(t, files, "no commitment files present — testDbAggregatorWithFiles produced no output?")
	var pick kv.VisibleFile
	var bestSpan uint64
	for _, f := range files {
		span := f.EndRootNum() - f.StartRootNum()
		if span > bestSpan {
			bestSpan = span
			pick = f
		}
	}
	require.NotNil(t, pick, "no commitment file has a positive tx-range span")
	return pick
}

// readKVFile returns every (k, v) pair in the supplied .kv file, decompressed.
func readKVFile(t *testing.T, agg *state.Aggregator, path string) ([][]byte, [][]byte) {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	r := seg.NewReader(d.MakeGetter(), agg.Cfg(kv.CommitmentDomain).Compression)
	r.Reset(0)
	var keys, vals [][]byte
	for r.HasNext() {
		k, _ := r.Next(nil)
		require.True(t, r.HasNext(), "value missing for key in %s", path)
		v, _ := r.Next(nil)
		keys = append(keys, append([]byte(nil), k...))
		vals = append(vals, append([]byte(nil), v...))
	}
	return keys, vals
}

// readFileBytes returns the entire on-disk byte contents of a file.
func readFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	b, err := io.ReadAll(f)
	require.NoError(t, err)
	return b
}

// TestConvertCommitmentFile_AlreadyTarget verifies the no-op short-circuit:
// when the source file is already in the requested encoding, convertCommitmentFile
// returns ErrSkip without producing any output. The orchestrator counts these
// files as skipped, not converted.
func TestConvertCommitmentFile_AlreadyTarget(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, _ := testDbAggregatorWithFiles(t, cfg)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	at := state.AggTx(rwTx)
	file := pickLargestCommitmentFile(t, at)

	dstDir := t.TempDir()
	// Source is V1+unsqueezed (disableCommitmentBranchTransform=true), target same.
	opts := state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: false}
	_, _, _, convErr := state.ConvertCommitmentFileForTest(t.Context(), at, file, dstDir, opts, 1, 1, uint64(0), uint64(0), log.New())
	require.ErrorIs(t, convErr, state.ErrSkipForTest, "no-op axes must return errSkip")

	// No output should have been produced.
	entries, err := os.ReadDir(dstDir)
	require.NoError(t, err)
	require.Empty(t, entries, "errSkip must not write any files to dstDir")
}

// TestConvertCommitmentFile_V1ToV2_KeysOnly converts the key axis from V1 to V2
// (squeeze axis unchanged). Asserts:
//   - the new .kv (plus the commitment-domain accessor index) lands in dstDir
//   - the aggregator's view of snapshots/domain/ is unaffected (no integration)
//   - every non-state key in the output decodes canonically via DecodeKeyV2
//   - the key count is preserved
func TestConvertCommitmentFile_V1ToV2_KeysOnly(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	at := state.AggTx(rwTx)
	file := pickLargestCommitmentFile(t, at)

	// Snapshot the original file's pairs for comparison.
	origKeys, origVals := readKVFile(t, agg, file.Fullpath())
	require.NotEmpty(t, origKeys, "test fixture must produce non-empty commitment file")

	dstDir := t.TempDir()
	opts := state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: true}
	_, _, _, convErr := state.ConvertCommitmentFileForTest(t.Context(), at, file, dstDir, opts, 1, 1, uint64(0), uint64(0), log.New())
	require.NoError(t, convErr)

	// Output .kv must land in dstDir.
	newKVPath := filepath.Join(dstDir, filepath.Base(file.Fullpath()))
	_, statErr := os.Stat(newKVPath)
	if errors.Is(statErr, os.ErrNotExist) {
		// File version prefixes may differ — locate by suffix instead.
		newKVPath = findKVInDir(t, dstDir)
	}
	_, err = os.Stat(newKVPath)
	require.NoError(t, err, "expected new .kv in %s", dstDir)

	// Per-domain accessor (commitment uses HashMap → .kvi).
	matches, err := filepath.Glob(filepath.Join(dstDir, "*.kvi"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "expected at least one .kvi accessor in %s", dstDir)

	// Aggregator view of snapshots/domain/ must be untouched.
	for _, vf := range at.Files(kv.CommitmentDomain) {
		require.NotEqual(t, dstDir, filepath.Dir(vf.Fullpath()),
			"aggregator view must not include dstDir files (integrate=false)")
	}

	// Every non-state key in the new file must decode canonically as V2.
	newKeys, newVals := readKVFile(t, agg, newKVPath)
	require.Equal(t, len(origKeys), len(newKeys), "pair count must be preserved")
	require.Equal(t, len(origVals), len(newVals))

	stateCount := 0
	for _, k := range newKeys {
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			stateCount++
			continue
		}
		_, err := nibbles.DecodeKeyV2(k)
		require.NoErrorf(t, err, "non-state key %x in new file must be canonical V2", k)
	}

	// The unsqueezed→unsqueezed value axis is a pass-through; values must be
	// byte-identical to the source (only the surrounding key order changed).
	// Compare on the multiset of (sorted-by-orig-key) values vs (sorted-by-new-
	// key) values — but the canonical comparison is set-based since key order
	// across axes is intentionally different. Use a multiset equality check.
	origVSet := byteMultiset(origVals)
	newVSet := byteMultiset(newVals)
	require.Equal(t, origVSet, newVSet, "value-axis pass-through must preserve every value byte-for-byte")
}

// TestConvertCommitmentFile_V1ToV1_Squeeze converts only the value axis from
// unsqueezed to squeezed (key axis stays V1). Asserts the new file contains
// short (sub-10-byte) plain-key references — the squeeze marker the read-side
// detector keys off of.
func TestConvertCommitmentFile_V1ToV1_Squeeze(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Build fixtures with ReplaceKeysInValues=false so the on-disk files start
	// unsqueezed. Then flip the config flag to true before running the converter
	// so commitmentValTransformDomain's inner guard lets the squeeze fire.
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	at := state.AggTx(rwTx)
	file := pickLargestCommitmentFile(t, at)

	dstDir := t.TempDir()
	opts := state.ConvertOpts{TargetSqueeze: true, TargetNibblesV2: false}
	_, _, _, convErr := state.ConvertCommitmentFileForTest(t.Context(), at, file, dstDir, opts, 1, 1, uint64(0), uint64(0), log.New())
	require.NoError(t, convErr)

	newKVPath := findKVInDir(t, dstDir)
	require.NotEmpty(t, newKVPath)

	// Squeezed values contain at least one embedded plain-key field shorter than
	// binary.MaxVarintLen64 (10 bytes) — the marker the read-side detector
	// (detectSqueezeState) keys off. Walk every non-state branch in the new
	// file and assert at least one such short field exists.
	newKeys, newVals := readKVFile(t, agg, newKVPath)
	require.NotEmpty(t, newVals)
	foundShort := false
	for i, v := range newVals {
		if bytes.Equal(newKeys[i], commitmentdb.KeyCommitmentState) || len(v) == 0 {
			continue
		}
		_, perr := commitment.BranchData(v).ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
			if len(key) < binary.MaxVarintLen64 {
				foundShort = true
			}
			return nil, nil
		})
		require.NoError(t, perr)
		if foundShort {
			break
		}
	}
	require.True(t, foundShort,
		"converted file must contain at least one squeezed (<10B) plain-key field")
}

// TestConvertCommitmentFile_RoundTrip_V1V2 verifies that V1 → V2 → V1 returns
// to byte-equal .kv contents on the squeeze-unchanged path. The round-trip is
// the load-bearing correctness invariant for the key axis.
//
// Mechanically: take the V1+unsqueezed dataset produced by testDbAggregatorWithFiles,
// convert one commitment file to V2+unsqueezed in tmpDir1, swap that file into
// snapshots/domain/, agg.OpenFolder() to refresh, convert back to V1+unsqueezed
// in tmpDir2, and compare tmpDir2/<file>.kv bytes against a snapshot taken
// before the first conversion.
func TestConvertCommitmentFile_RoundTrip_V1V2(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)

	// First pass: V1 → V2. The aggregator needs to drop its mmap handles on
	// the V1 file before we swap in the V2 version, so we rollback explicitly
	// after the conversion (not via defer); t.Cleanup still guards
	// early-exit/panic cases. The rollback fn satisfies func() — ruleguard's
	// "right after the error check" rule.
	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback) //nolint:gocritic // rollback is invoked explicitly below before the file swap
	at := state.AggTx(rwTx)
	file := pickLargestCommitmentFile(t, at)

	origPath := file.Fullpath()
	origBytes := readFileBytes(t, origPath)
	origKVName := filepath.Base(origPath)

	tmpDir1 := t.TempDir()
	_, _, _, err = state.ConvertCommitmentFileForTest(t.Context(), at,
		file, tmpDir1, state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: true},
		1, 1, uint64(0), uint64(0), log.New())
	require.NoError(t, err)
	rwTx.Rollback()

	// Swap: replace the V1 .kv (+ its .kvi) in SnapDomain with the V2 file we
	// just produced in tmpDir1. The orchestrator's Phase 3+4 do the same thing
	// — here we inline it so we can convert a second time without writing the
	// full orchestrator yet.
	snapDir := filepath.Dir(origPath)
	swapInto(t, tmpDir1, snapDir)
	// ReloadFiles (close dirty files, then re-scan) is required so the
	// aggregator drops its mmap handles on the old V1 file and picks up the
	// V2 file we just swapped in. OpenFolder alone keeps the cached entries.
	require.NoError(t, agg.ReloadFiles())

	// Second pass: V2 → V1 on the swapped-in file.
	rwTx2, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx2.Rollback()
	at2 := state.AggTx(rwTx2)

	// Re-pick by step range: filename may have a different version prefix after
	// the round-trip, so finding by basename is unsafe. The largest-span file
	// is stable across the swap.
	file2 := pickLargestCommitmentFile(t, at2)

	tmpDir2 := t.TempDir()
	_, _, _, err = state.ConvertCommitmentFileForTest(t.Context(), at2,
		file2, tmpDir2, state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: false},
		1, 1, uint64(0), uint64(0), log.New())
	require.NoError(t, err)

	finalPath := filepath.Join(tmpDir2, origKVName)
	if _, statErr := os.Stat(finalPath); errors.Is(statErr, os.ErrNotExist) {
		finalPath = findKVInDir(t, tmpDir2)
	}
	finalBytes := readFileBytes(t, finalPath)
	require.True(t, bytes.Equal(origBytes, finalBytes),
		"V1→V2→V1 round-trip must produce byte-equal .kv (orig %d bytes, final %d bytes)",
		len(origBytes), len(finalBytes))
}

// swapInto moves every regular file in srcDir into dstDir, overwriting any
// existing file with the same basename. Used by the round-trip test to splice
// the converter's output back into the aggregator's view (as the orchestrator's
// Phase 3+4 does in production).
func swapInto(t *testing.T, srcDir, dstDir string) {
	t.Helper()
	entries, err := os.ReadDir(srcDir)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		src := filepath.Join(srcDir, e.Name())
		dst := filepath.Join(dstDir, e.Name())
		// Remove any existing target so Rename succeeds across filesystems.
		_ = dir.RemoveFile(dst)
		require.NoError(t, os.Rename(src, dst), "rename %s -> %s", src, dst)
	}
}

// findKVInDir returns the path of the first *.kv file in dir, or empty string
// if none exists. Helper for tests that don't know the exact file-version
// prefix the converter applied.
func findKVInDir(t *testing.T, dir string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.kv"))
	require.NoError(t, err)
	if len(matches) == 0 {
		return ""
	}
	return matches[0]
}

// byteMultiset encodes a slice of byte slices as a map[string]int so two
// multisets can be compared by value, independent of order. The values in a
// commitment .kv file are unique by construction (no duplicate branches at
// the same key), but we still use a multiset to avoid sneaking in an
// uniqueness assumption.
func byteMultiset(in [][]byte) map[string]int {
	out := make(map[string]int, len(in))
	for _, b := range in {
		out[string(b)]++
	}
	return out
}

// snapshotCommitmentFiles records the (basename → byte content) of every file
// in SnapDomain whose name matches a commitment file (or commitment accessor /
// torrent sibling). Used by tests to assert the on-disk state moved
// correctly during the orchestrator's backup/promote phases.
func snapshotCommitmentFiles(t *testing.T, snapDir string) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	matches, err := filepath.Glob(filepath.Join(snapDir, "*-commitment.*"))
	require.NoError(t, err)
	for _, p := range matches {
		out[filepath.Base(p)] = readFileBytes(t, p)
	}
	return out
}

// TestConvertCommitmentFiles_FullFlow runs the 5-phase orchestrator on a real
// aggregator built by testDbAggregatorWithFiles. It verifies the load-bearing
// invariants:
//
//   - Originals land in snapshots/backup/domains/ (move, not copy).
//   - snapshots/rebuild/ is gone after Phase 4.
//   - snapshots/domain/ contains a new commitment .kv and per-config accessor
//     for every converted file.
//   - The aggregator picks up the new files after Phase 5's ReloadFiles, and
//     the commitment root computed against the reloaded files matches the
//     root the original files produced.
func TestConvertCommitmentFiles_FullFlow(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)
	// commitmentValTransformDomain short-circuits to pass-through when the live
	// runtime flag is false. Flip it on so TargetSqueeze:true actually fires —
	// otherwise the orchestrator silently no-ops on the squeeze axis and the
	// "root unchanged" assertion below holds trivially.
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)

	// Compute the commitment root via the pre-conversion files.
	rootBefore := computeCommitmentRoot(t, db)

	snapDir := agg.Dirs().SnapDomain
	origFiles := snapshotCommitmentFiles(t, snapDir)
	require.NotEmpty(t, origFiles, "must have commitment files before conversion")

	// Open a read-only view for the orchestrator. The orchestrator's Phase 5
	// calls ReloadFiles which invalidates this RoTx; the explicit Rollback
	// below releases it. The Cleanup is a safety net for early-exit/panic.
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback) //nolint:gocritic // explicit Rollback below; this is the panic safety net
	at := state.AggTx(rwTx)

	// Value-axis only: V1+unsqueezed → V1+squeezed. Keeps the test asserting
	// pure orchestrator mechanics without composing two codecs at once.
	opts := state.ConvertOpts{TargetSqueeze: true, TargetNibblesV2: false}
	convErr := state.ConvertCommitmentFiles(t.Context(), at, opts, log.New())
	rwTx.Rollback() // safe: ReloadFiles already invalidated at internally
	require.NoError(t, convErr)

	// Verify the squeeze actually fired: at least one promoted file must
	// contain a sub-10B embedded plain-key field. Without this check the
	// "root unchanged" assertion below would also pass when the value codec
	// silently no-oped (e.g. ReplaceKeysInValues=false in the fixture).
	newKVsForSqueezeCheck, gErr := filepath.Glob(filepath.Join(snapDir, "*-commitment.*.kv"))
	require.NoError(t, gErr)
	require.NotEmpty(t, newKVsForSqueezeCheck)
	require.True(t, anyFileHasSqueezedField(t, agg, newKVsForSqueezeCheck),
		"orchestrator with TargetSqueeze=true must produce at least one short plain-key field somewhere")

	// Phase 3 invariant: every backed-up file is a verbatim copy of an
	// original. The fixture intentionally includes unmerged 1-step subset
	// files; those fail ValuesPlainKeyReferencingThresholdReached and are
	// skipped by the converter (left in place under snapshots/domain/), so
	// backup is a subset — not equal — to origFiles.
	backupDir := filepath.Join(agg.Dirs().Snap, "backup", "domains")
	backupFiles := snapshotCommitmentFiles(t, backupDir)
	require.NotEmpty(t, backupFiles, "at least one squeezeable file must be backed up")
	require.LessOrEqual(t, len(backupFiles), len(origFiles))
	for name, content := range backupFiles {
		orig, ok := origFiles[name]
		require.Truef(t, ok, "backup contains %s with no matching original", name)
		require.Truef(t, bytes.Equal(orig, content),
			"backup content mismatch for %s", name)
	}

	// Phase 4 invariant: rebuild dir is gone.
	rebuildDir := filepath.Join(agg.Dirs().Snap, "rebuild", "domain")
	_, statErr := os.Stat(rebuildDir)
	require.Truef(t, errors.Is(statErr, os.ErrNotExist),
		"rebuild dir must be removed; got err=%v", statErr)

	// Phase 4 invariant: snapshots/domain/ has a fresh .kv + .kvi for each
	// converted file. We compare by step-range basename since the new file may
	// have a different version prefix.
	newKVs, err := filepath.Glob(filepath.Join(snapDir, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, newKVs, "snapshots/domain/ must have commitment .kv files after promote")
	for _, p := range newKVs {
		// Each .kv must have a sibling .kvi (commitment domain uses AccessorHashMap).
		kvi := p[:len(p)-3] + ".kvi"
		kvi = swapVersionExt(kvi) // version prefix may differ
		_, err := os.Stat(kvi)
		require.NoErrorf(t, err, "expected sibling .kvi for %s", p)
	}

	// Phase 5 invariant: re-computing the root via the reloaded aggregator
	// matches the pre-conversion root.
	rootAfter := computeCommitmentRoot(t, db)
	require.Equal(t, rootBefore, rootAfter,
		"commitment root must be unchanged across squeeze conversion")
}

// anyFileHasSqueezedField returns true if any non-state branch value in any of
// the supplied .kv files contains an embedded plain-key reference shorter than
// binary.MaxVarintLen64 (10 bytes) — the on-disk marker the read side keys off
// to detect squeezed values.
func anyFileHasSqueezedField(t *testing.T, agg *state.Aggregator, paths []string) bool {
	t.Helper()
	for _, p := range paths {
		keys, vals := readKVFile(t, agg, p)
		for i, v := range vals {
			if bytes.Equal(keys[i], commitmentdb.KeyCommitmentState) || len(v) == 0 {
				continue
			}
			found := false
			_, perr := commitment.BranchData(v).ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
				if len(key) < binary.MaxVarintLen64 {
					found = true
				}
				return nil, nil
			})
			require.NoError(t, perr)
			if found {
				return true
			}
		}
	}
	return false
}

// computeCommitmentRoot opens a fresh tx and computes the commitment root via
// the same code path used by `integration commitment print`.
func computeCommitmentRoot(t *testing.T, db kv.TemporalRwDB) []byte {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	rh, err := domains.ComputeCommitment(t.Context(), tx, false, 0, 0, "", nil)
	require.NoError(t, err)
	return append([]byte(nil), rh...)
}

// swapVersionExt returns a glob-found sibling at the same step-range; the
// version prefix on accessor files may differ from the .kv prefix.
func swapVersionExt(kviPath string) string {
	dirPart, base := filepath.Split(kviPath)
	idx := strings.Index(base, "-commitment.")
	if idx < 0 {
		return kviPath
	}
	pattern := filepath.Join(dirPart, "*"+base[idx:])
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return kviPath
	}
	return matches[0]
}

// TestConvertCommitmentFiles_RoundTripComposed exercises the load-bearing
// correctness invariant: a forward conversion followed by its inverse must
// land at the starting state. The .kv bytes after V1+unsqueezed → V2+squeezed
// → V1+unsqueezed must be byte-equal to the starting .kv bytes for every
// commitment file.
//
// Mechanically: snapshot snapshots/domain/ at the start, run two conversions
// (clearing the backup dir between them because Phase-1 pre-flight refuses to
// overwrite an existing backup), then compare snapshots/domain/ against the
// pre-conversion snapshot. The composed round-trip also implicitly exercises
// both axes in both directions.
func TestConvertCommitmentFiles_RoundTripComposed(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)
	// Enable ReplaceKeysInValues so the squeeze axis actually fires in both
	// directions; without it the squeeze→unsqueezed leg of the round-trip is
	// a silent no-op and byte equality holds for the wrong reason.
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)

	snapDir := agg.Dirs().SnapDomain
	origFiles := snapshotCommitmentFiles(t, snapDir)
	require.NotEmpty(t, origFiles)

	// Forward: V1+unsqueezed → V2+squeezed.
	runOrchestrator(t, db, state.ConvertOpts{TargetSqueeze: true, TargetNibblesV2: true})

	// Confirm the squeeze leg actually fired before re-running in reverse;
	// otherwise the byte-equal assertion below would be a tautology.
	postFwd, gErr := filepath.Glob(filepath.Join(snapDir, "*-commitment.*.kv"))
	require.NoError(t, gErr)
	require.True(t, anyFileHasSqueezedField(t, agg, postFwd),
		"forward leg must produce at least one squeezed plain-key field")

	// Clear backup between passes; Phase 1 pre-flight refuses to overwrite.
	backupDir := filepath.Join(agg.Dirs().Snap, "backup", "domains")
	require.NoError(t, dir.RemoveAll(backupDir))

	// Reverse: V2+squeezed → V1+unsqueezed.
	runOrchestrator(t, db, state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: false})

	// Snapshot final state and compare byte-for-byte against the pre-conversion
	// snapshot. Both sets must list the same basenames; each pair must match.
	finalFiles := snapshotCommitmentFiles(t, snapDir)
	require.Equal(t, len(origFiles), len(finalFiles),
		"final commitment file count must match starting count")
	for name, content := range origFiles {
		got, ok := finalFiles[name]
		require.Truef(t, ok, "post-roundtrip missing file %s", name)
		require.Truef(t, bytes.Equal(content, got),
			"post-roundtrip mismatch for %s (orig %d bytes, final %d bytes)",
			name, len(content), len(got))
	}
}

// runOrchestrator opens a fresh RwTx, runs ConvertCommitmentFiles, and closes
// the tx. The RwTx is held only for the duration of the call; the explicit
// Rollback releases it. The Cleanup is the panic-safety net required by the
// project's ruleguard rule for transactions.
func runOrchestrator(t *testing.T, db kv.TemporalRwDB, opts state.ConvertOpts) {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback) //nolint:gocritic // explicit Rollback below; this is the panic safety net
	at := state.AggTx(tx)
	convErr := state.ConvertCommitmentFiles(t.Context(), at, opts, log.New())
	tx.Rollback()
	require.NoError(t, convErr)
}

// TestConvertCommitmentFiles_AllAlreadyTarget covers the no-op path. When
// every file is already in the requested state, the orchestrator emits no
// backup, no rebuild dir, and modifies nothing on disk.
func TestConvertCommitmentFiles_AllAlreadyTarget(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)

	snapDir := agg.Dirs().SnapDomain
	before := snapshotCommitmentFiles(t, snapDir)
	require.NotEmpty(t, before)

	// Source is V1+unsqueezed; request the same → every file skips.
	runOrchestrator(t, db, state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: false})

	// snapshots/domain/ untouched.
	after := snapshotCommitmentFiles(t, snapDir)
	require.Equal(t, len(before), len(after))
	for name, content := range before {
		got, ok := after[name]
		require.True(t, ok)
		require.True(t, bytes.Equal(content, got), "file %s changed despite no-op opts", name)
	}

	// No backup dir, no rebuild dir.
	backupDir := filepath.Join(agg.Dirs().Snap, "backup", "domains")
	rebuildDir := filepath.Join(agg.Dirs().Snap, "rebuild", "domain")
	_, err := os.Stat(backupDir)
	require.Truef(t, errors.Is(err, os.ErrNotExist), "backup dir must not exist (err=%v)", err)
	_, err = os.Stat(rebuildDir)
	require.Truef(t, errors.Is(err, os.ErrNotExist), "rebuild dir must not exist (err=%v)", err)
}

// TestConvertCommitmentFiles_EmptyDatadir covers the no-files-at-all path:
// the orchestrator returns nil without writing anything when at.Files returns
// an empty list.
func TestConvertCommitmentFiles_EmptyDatadir(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	db, agg := testDbAndAggregatorv3(t, 10)
	_ = agg
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := state.AggTx(tx)
	require.Empty(t, at.Files(kv.CommitmentDomain),
		"setup precondition: no commitment files in empty datadir")

	require.NoError(t, state.ConvertCommitmentFiles(t.Context(), at,
		state.ConvertOpts{TargetSqueeze: true, TargetNibblesV2: true}, log.New()))

	// Nothing was created.
	_, err = os.Stat(filepath.Join(agg.Dirs().Snap, "backup"))
	require.Truef(t, errors.Is(err, os.ErrNotExist), "backup must not exist (err=%v)", err)
	_, err = os.Stat(filepath.Join(agg.Dirs().Snap, "rebuild"))
	require.Truef(t, errors.Is(err, os.ErrNotExist), "rebuild must not exist (err=%v)", err)
}

// TestConvertCommitmentFiles_BackupExists covers the pre-flight refusal path:
// if snapshots/backup/domains/ already has contents (from a prior conversion
// the user hasn't acknowledged), the orchestrator must refuse to start so it
// can't silently overwrite a recovery point.
func TestConvertCommitmentFiles_BackupExists(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)

	// Pre-populate backup.
	backupDir := filepath.Join(agg.Dirs().Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "decoy.kv"), []byte("x"), 0o644))

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := state.AggTx(tx)

	convErr := state.ConvertCommitmentFiles(t.Context(), at,
		state.ConvertOpts{TargetSqueeze: true, TargetNibblesV2: true}, log.New())
	require.Error(t, convErr, "must refuse to run when backup is non-empty")
	require.Contains(t, convErr.Error(), backupDir,
		"error must name the offending backup path")
}

// TestConvertCommitmentFiles_LeftoverRebuild covers the crash-recovery path:
// if a prior crashed run left junk under snapshots/rebuild/domain/, the
// orchestrator wipes it on pre-flight and proceeds normally.
func TestConvertCommitmentFiles_LeftoverRebuild(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfg)

	// Pre-populate rebuild dir with junk to simulate a crashed Phase 1.
	rebuildDir := filepath.Join(agg.Dirs().Snap, "rebuild", "domain")
	require.NoError(t, os.MkdirAll(rebuildDir, 0o755))
	junk := filepath.Join(rebuildDir, "stale.kv")
	require.NoError(t, os.WriteFile(junk, []byte("junk"), 0o644))

	runOrchestrator(t, db, state.ConvertOpts{TargetSqueeze: true, TargetNibblesV2: false})

	// Junk gone; conversion completed.
	_, err := os.Stat(junk)
	require.Truef(t, errors.Is(err, os.ErrNotExist),
		"stale rebuild file must be wiped (err=%v)", err)
	backupDir := filepath.Join(agg.Dirs().Snap, "backup", "domains")
	_, err = os.Stat(backupDir)
	require.NoError(t, err, "backup dir must exist after successful run")
}

// listVisibleCommitmentKVs returns the basenames of every visible commitment
// .kv file in the aggregator's view, in the order at.Files presents them.
// Used by the resume integration test to know how many input shards Phase 1
// will touch up-front.
func listVisibleCommitmentKVs(t *testing.T, db kv.TemporalRwDB) []kv.VisibleFile {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := state.AggTx(tx)
	all := at.Files(kv.CommitmentDomain)
	out := make([]kv.VisibleFile, 0, len(all))
	for _, f := range all {
		if strings.HasSuffix(f.Fullpath(), ".kv") {
			out = append(out, f)
		}
	}
	return out
}

// TestConvertCommitmentFiles_ContinueResumes drives the resume flow end-to-end.
// A reference fixture runs the orchestrator uninterrupted; a parallel fixture
// (same deterministic seed → byte-identical inputs) cancels mid-Phase-1 after
// the first .kv lands in rebuildDir, then re-runs with Continue=true. The
// resumed snapshots/domain/ must match the reference byte-for-byte and the
// commitment root must be unchanged from the pre-conversion value.
//
// Cancellation is driven by the convertPhase1AfterFileHook test hook rather
// than by counting log lines (which is fragile across line-number /
// timestamp changes — see the plan's "Decision now" note in Task 5).
func TestConvertCommitmentFiles_ContinueResumes(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}

	// Pure key-axis conversion (V1→V2). The squeeze axis stays off so the
	// sub-threshold short-circuit at convertCommitmentFile cannot silently
	// downgrade individual files to errSkip — every visible .kv must drive a
	// real Phase 1 conversion, otherwise the hook-count assertion below would
	// fire spuriously.
	opts := state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: true}

	// Reference fixture: deterministic seed (newRnd(0) in the fixture builder)
	// → identical input bytes to the resume fixture. A full uninterrupted run
	// produces the snapshots/domain/ contents we compare against.
	refDB, refAgg := testDbAggregatorWithFiles(t, cfg)
	runOrchestrator(t, refDB, opts)
	referenceSnap := snapshotCommitmentFiles(t, refAgg.Dirs().SnapDomain)
	require.NotEmpty(t, referenceSnap, "reference run must produce commitment files in snapDomain")
	referenceRoot := computeCommitmentRoot(t, refDB)

	// Resume fixture: same config, same seed.
	db, agg := testDbAggregatorWithFiles(t, cfg)
	snapDir := agg.Dirs().SnapDomain
	rebuildDir := filepath.Join(agg.Dirs().Snap, "rebuild", "domain")
	backupDir := filepath.Join(agg.Dirs().Snap, "backup", "domains")

	// Snapshot the pre-conversion state so we can assert Phase 1 cancellation
	// left snapDomain untouched (Phases 2-5 never ran in the first attempt).
	originalSnap := snapshotCommitmentFiles(t, snapDir)
	require.NotEmpty(t, originalSnap)
	rootBefore := computeCommitmentRoot(t, db)
	require.Equal(t, referenceRoot, rootBefore,
		"determinism precondition: identical seed must produce identical pre-conversion root")

	inputFiles := listVisibleCommitmentKVs(t, db)
	require.GreaterOrEqual(t, len(inputFiles), 2,
		"resume test needs >= 2 commitment .kv files to exercise the suffix path")
	firstFileStepFrom := inputFiles[0].StartRootNum() / agg.StepSize()
	firstFileStepTo := inputFiles[0].EndRootNum() / agg.StepSize()
	inputCount := len(inputFiles)

	// First run: cancel after file 0 completes. The hook fires AFTER the file's
	// .kv (and accessor siblings, all tmp-then-rename) land in rebuildDir, so
	// the partial state captured here is one complete shard.
	t.Cleanup(func() { state.SetConvertPhase1AfterFileHookForTest(nil) })
	ctx1, cancel1 := context.WithCancel(t.Context())
	firstRunCount := 0
	state.SetConvertPhase1AfterFileHookForTest(func(idx int) {
		firstRunCount++
		if idx == 0 {
			cancel1()
		}
	})

	tx1, err := db.BeginTemporalRw(ctx1)
	require.NoError(t, err)
	t.Cleanup(tx1.Rollback) //nolint:gocritic // explicit Rollback below; this is the panic safety net
	convErr := state.ConvertCommitmentFiles(ctx1, state.AggTx(tx1), opts, log.New())
	tx1.Rollback()
	cancel1()
	require.Error(t, convErr, "first run must abort once the hook cancels ctx")
	require.ErrorIs(t, convErr, context.Canceled)
	require.Equal(t, 1, firstRunCount,
		"hook must fire exactly once before cancel is observed at the next iteration's ctx.Err() check")

	// File 0's complete shard must be present in rebuildDir.
	kvPattern := filepath.Join(rebuildDir, fmt.Sprintf("*-commitment.%d-%d.kv", firstFileStepFrom, firstFileStepTo))
	kvMatches, err := filepath.Glob(kvPattern)
	require.NoError(t, err)
	require.Len(t, kvMatches, 1, "rebuildDir must contain file 0's .kv after the cancel: %s", kvPattern)
	kviPattern := filepath.Join(rebuildDir, fmt.Sprintf("*-commitment.%d-%d.kvi", firstFileStepFrom, firstFileStepTo))
	kviMatches, err := filepath.Glob(kviPattern)
	require.NoError(t, err)
	require.Len(t, kviMatches, 1, "rebuildDir must contain file 0's .kvi sibling after the cancel")

	// No other shards should be present — the ctx.Err() check at the top of
	// Phase 1's next iteration returns before convertCommitmentFile is invoked
	// again, so no other rebuildDir output races to land.
	allKVMatches, err := filepath.Glob(filepath.Join(rebuildDir, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.Len(t, allKVMatches, 1, "exactly one .kv shard expected in rebuildDir after cancel")

	// snapDomain must be byte-identical to the pre-conversion snapshot — Phase
	// 3 backup and Phase 4 promote never ran.
	postCancelSnap := snapshotCommitmentFiles(t, snapDir)
	require.Equal(t, len(originalSnap), len(postCancelSnap),
		"snapDomain file count must be unchanged after Phase 1 cancel")
	for name, want := range originalSnap {
		got, ok := postCancelSnap[name]
		require.Truef(t, ok, "snapDomain file %s disappeared during Phase 1 cancel", name)
		require.Truef(t, bytes.Equal(want, got), "snapDomain file %s mutated during Phase 1 cancel", name)
	}
	_, statErr := os.Stat(backupDir)
	require.Truef(t, errors.Is(statErr, os.ErrNotExist),
		"backup dir must not exist after Phase 1 cancel (Phase 3 did not run): %v", statErr)

	// Second run: Continue=true, fresh ctx, hook installed only to count calls
	// — no cancellation this time.
	secondRunCount := 0
	state.SetConvertPhase1AfterFileHookForTest(func(idx int) {
		secondRunCount++
	})

	tx2, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx2.Rollback) //nolint:gocritic // explicit Rollback below; this is the panic safety net
	resumeOpts := opts
	resumeOpts.Continue = true
	convErr = state.ConvertCommitmentFiles(t.Context(), state.AggTx(tx2), resumeOpts, log.New())
	tx2.Rollback() // safe: Phase 5 ReloadFiles already invalidated at internally
	require.NoError(t, convErr, "resume run must complete successfully")
	state.SetConvertPhase1AfterFileHookForTest(nil)

	// File 0 was already done from run 1 — Phase 1 only iterates over the
	// pendingFiles suffix, so the hook fires exactly (inputCount - 1) times.
	require.Equal(t, inputCount-1, secondRunCount,
		"resume must process N-1 files (file 0 was carried over from the cancelled run)")

	// Phase 4 invariant: rebuildDir is gone.
	_, statErr = os.Stat(rebuildDir)
	require.Truef(t, errors.Is(statErr, os.ErrNotExist),
		"rebuildDir must be removed after successful resume: %v", statErr)

	// Phase 3 invariant: every original is backed up.
	_, statErr = os.Stat(backupDir)
	require.NoError(t, statErr, "backup dir must exist after successful resume")
	backupSnap := snapshotCommitmentFiles(t, backupDir)
	require.NotEmpty(t, backupSnap)
	for name, content := range backupSnap {
		orig, ok := originalSnap[name]
		require.Truef(t, ok, "backup contains %s with no matching pre-conversion original", name)
		require.Truef(t, bytes.Equal(orig, content), "backup content mismatch for %s", name)
	}

	// snapDomain matches the reference run. The on-disk basename set must be
	// identical; for each .kv file the bytes must match. Accessor siblings
	// (.kvi / .bt / .kvei) are not byte-compared because recsplit uses a
	// per-datadir random salt (see salt-state.txt), so independent fixtures
	// produce different accessor bytes for the same input — accessor existence
	// is asserted via the basename match.
	resumedSnap := snapshotCommitmentFiles(t, snapDir)
	require.Equal(t, len(referenceSnap), len(resumedSnap),
		"resumed snapDomain file count must match reference")
	for name := range referenceSnap {
		_, ok := resumedSnap[name]
		require.Truef(t, ok, "resumed run missing file %s present in reference", name)
	}
	for name, refContent := range referenceSnap {
		if !strings.HasSuffix(name, ".kv") {
			continue
		}
		got := resumedSnap[name]
		require.Truef(t, bytes.Equal(refContent, got),
			"resumed .kv %s differs from reference (ref %d bytes, resumed %d bytes)",
			name, len(refContent), len(got))
	}

	// Commitment root unchanged across the conversion — semantic correctness
	// check independent of file-byte equality.
	rootAfter := computeCommitmentRoot(t, db)
	require.Equal(t, rootBefore, rootAfter,
		"commitment root must survive the cancel/resume cycle")
}

// TestRestoreCommitmentFiles_HappyPath seeds the backup dir with original
// commitment files, seeds snapshots/domain/ with same-named "converted"
// dummies, runs RestoreCommitmentFiles, and asserts the originals overwrite
// the dummies while the backup tree is cleaned up.
func TestRestoreCommitmentFiles_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))

	// Seed backup with originals and snap/domain with converted dummies of the
	// same names — restore must overwrite the dummies with the originals.
	origKV := []byte("ORIG_KV")
	origBT := []byte("ORIG_BT")
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.kv"), origKV, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.bt"), origBT, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.kv"), []byte("CONV_KV"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.bt"), []byte("CONV_BT"), 0o644))

	require.NoError(t, state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New()))

	restoredKV := readFileBytes(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.kv"))
	restoredBT := readFileBytes(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.bt"))
	require.Equal(t, origKV, restoredKV, "kv must hold original backup bytes")
	require.Equal(t, origBT, restoredBT, "bt must hold original backup bytes")

	_, statErr := os.Stat(backupDir)
	require.Truef(t, errors.Is(statErr, os.ErrNotExist),
		"backup dir must be removed after restore (err=%v)", statErr)
	_, statErr = os.Stat(filepath.Join(dirs.Snap, "backup"))
	require.Truef(t, errors.Is(statErr, os.ErrNotExist),
		"empty backup parent must be removed (err=%v)", statErr)
}

// TestRestoreCommitmentFiles_NoBackup confirms RestoreCommitmentFiles refuses
// to run when snapshots/backup/domains/ is missing — the operator must see a
// clear error rather than a silent no-op.
func TestRestoreCommitmentFiles_NoBackup(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)

	err := state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New())
	require.Error(t, err, "must refuse to run without a backup directory")
	require.Contains(t, err.Error(), "no backup",
		"error must mention 'no backup' so the operator understands the cause")
}

// TestRestoreCommitmentFiles_OrphanSweep verifies the orphan-sweep step:
// converted files with a different version prefix (or extra accessor types)
// must be removed before the originals are renamed back into place, otherwise
// the restored datadir would contain cross-version siblings that confuse
// erigon at startup.
func TestRestoreCommitmentFiles_OrphanSweep(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))

	// Originals (v2.0): .kv + .bt under backup.
	origKV := []byte("ORIG_KV")
	origBT := []byte("ORIG_BT")
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.kv"), origKV, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.bt"), origBT, 0o644))

	// Converted (v2.1): different version prefix and a sibling accessor type
	// (.kvei) that does not exist in the backup. Naive rename-overwrite would
	// leave both behind alongside the restored v2.0 originals.
	convKVPath := filepath.Join(dirs.SnapDomain, "v2.1-commitment.0-32.kv")
	convKVEIPath := filepath.Join(dirs.SnapDomain, "v2.1-commitment.0-32.kvei")
	require.NoError(t, os.WriteFile(convKVPath, []byte("CONV_KV"), 0o644))
	require.NoError(t, os.WriteFile(convKVEIPath, []byte("CONV_KVEI"), 0o644))

	require.NoError(t, state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New()))

	// Sweep removed both cross-version artifacts.
	_, statErr := os.Stat(convKVPath)
	require.Truef(t, errors.Is(statErr, os.ErrNotExist),
		"v2.1 .kv orphan must be swept (err=%v)", statErr)
	_, statErr = os.Stat(convKVEIPath)
	require.Truef(t, errors.Is(statErr, os.ErrNotExist),
		"v2.1 .kvei orphan must be swept (err=%v)", statErr)

	// Originals restored with backup content.
	restoredKV := readFileBytes(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.kv"))
	restoredBT := readFileBytes(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.bt"))
	require.Equal(t, origKV, restoredKV)
	require.Equal(t, origBT, restoredBT)
}

// TestRestoreCommitmentFiles_RerunnableAfterPartialFailure simulates a restore
// that crashed after the orphan sweep deleted the converted files and after
// some — but not all — backup files were renamed into snapshots/domain/. A
// retry must complete the operation without deleting the files that the prior
// attempt already moved.
func TestRestoreCommitmentFiles_RerunnableAfterPartialFailure(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))

	// Simulate "after partial failure" state:
	//   - manifest already on disk, listing the full set of 3 backup files;
	//   - .kv and .bt already moved into SnapDomain on the prior attempt;
	//   - only .kvi remains in backup (the rename that crashed).
	origKV := []byte("ORIG_KV")
	origBT := []byte("ORIG_BT")
	origKVI := []byte("ORIG_KVI")
	manifest := "v2.0-commitment.0-32.kv\nv2.0-commitment.0-32.bt\nv2.0-commitment.0-32.kvi"
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, ".restore_manifest"), []byte(manifest), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.kvi"), origKVI, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.kv"), origKV, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.bt"), origBT, 0o644))

	require.NoError(t, state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New()))

	// All three originals present with the expected content — the manifest-aware
	// sweep must not have deleted the .kv/.bt files already in place.
	require.Equal(t, origKV, readFileBytes(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.kv")))
	require.Equal(t, origBT, readFileBytes(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.bt")))
	require.Equal(t, origKVI, readFileBytes(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.kvi")))

	// Backup tree gone after successful completion.
	_, statErr := os.Stat(backupDir)
	require.Truef(t, errors.Is(statErr, os.ErrNotExist),
		"backup dir must be removed after successful retry (err=%v)", statErr)
}

// TestRestoreCommitmentFiles_RejectsForeignFile verifies the restore refuses to
// proceed when the backup directory contains a file whose name does not match
// the commitment step-range pattern. Without this guard a stray file like
// `random.bak` would short-circuit step-range collection silently or, worse,
// a foreign file whose name happens to contain `-commitment.<from>-<to>.`
// could broaden the orphan sweep into unrelated commitment files.
func TestRestoreCommitmentFiles_RejectsForeignFile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))

	// One legitimate backup entry alongside one foreign filename.
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.kv"), []byte("ORIG"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "random.bak"), []byte("X"), 0o644))

	// Sentinel file in SnapDomain to prove no sweep ran before the rejection.
	sentinel := filepath.Join(dirs.SnapDomain, "v2.1-commitment.0-32.kv")
	require.NoError(t, os.WriteFile(sentinel, []byte("SENTINEL"), 0o644))

	err := state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New())
	require.Error(t, err, "must refuse to run when backup contains a non-matching entry")
	require.Contains(t, err.Error(), "random.bak",
		"error must name the offending entry")

	// Sweep must not have fired — sentinel still present, backup still intact.
	_, statErr := os.Stat(sentinel)
	require.NoError(t, statErr, "sweep must not run when validation fails")
	_, statErr = os.Stat(filepath.Join(backupDir, "v2.0-commitment.0-32.kv"))
	require.NoError(t, statErr, "backup must be untouched after rejection")
}

// TestRestoreCommitmentFiles_FailsWhenManifestEntryMissingFromBothSides
// reproduces the case where an existing manifest references a file that is
// absent from both the backup directory and the destination — e.g. a crash
// produced a truncated manifest with extra entries an operator was about to
// rebuild, or a backup file was deleted out-of-band between attempts. Without
// the cross-check, restore would silently report success while a file is
// missing; the operator would only discover it after erigon failed to start.
func TestRestoreCommitmentFiles_FailsWhenManifestEntryMissingFromBothSides(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))

	// Manifest references three files. Only .kv exists in backup; .bt was moved
	// to SnapDomain on a prior attempt (legitimate retry state); .kvi exists in
	// neither location — that's the missing-data case we must detect.
	manifest := "v2.0-commitment.0-32.kv\nv2.0-commitment.0-32.bt\nv2.0-commitment.0-32.kvi"
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, ".restore_manifest"), []byte(manifest), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.kv"), []byte("ORIG_KV"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.bt"), []byte("ORIG_BT"), 0o644))

	err := state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New())
	require.Error(t, err, "must fail when a manifest entry is in neither backup nor destination")
	require.Contains(t, err.Error(), "v2.0-commitment.0-32.kvi",
		"error must name the missing file")
}

// TestRestoreCommitmentFiles_AtomicManifestIgnoresStaleTmp reproduces a crash
// during the very first manifest write: the .restore_manifest.tmp temp file is
// left behind without a corresponding .restore_manifest. The next restore must
// not treat the stale .tmp as a backup entry (it doesn't match the commitment
// pattern; without the explicit filter it would trigger the foreign-file
// guard). Restore must complete successfully against the real backup entries.
func TestRestoreCommitmentFiles_AtomicManifestIgnoresStaleTmp(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))

	origKV := []byte("ORIG_KV")
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.kv"), origKV, 0o644))
	// Stale tmp from a crashed manifest write — must be ignored, not parsed.
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, ".restore_manifest.tmp"), []byte("garbage"), 0o644))

	require.NoError(t, state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New()))

	restoredKV := readFileBytes(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.kv"))
	require.Equal(t, origKV, restoredKV)
}

// TestRestoreCommitmentFiles_RejectsPathTraversalInManifest verifies the
// load-manifest path refuses entries that contain path components. The
// step-range regex is not anchored, so a name like
// `../../etc/passwd-commitment.0-32.kv` would otherwise pass shape validation
// and let filepath.Join escape backupDir / snapDomain on rename.
func TestRestoreCommitmentFiles_RejectsPathTraversalInManifest(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))

	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "v2.0-commitment.0-32.kv"), []byte("ORIG"), 0o644))
	manifest := "../../etc/passwd-commitment.0-32.kv"
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, ".restore_manifest"), []byte(manifest), 0o644))

	err := state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New())
	require.Error(t, err, "must reject manifest entries with path components")
	require.Contains(t, err.Error(), "plain filename",
		"error must call out the basename requirement")
}

// TestRestoreCommitmentFiles_RejectsEmptyManifest verifies that an existing
// .restore_manifest file with no usable entries surfaces as an error rather
// than silently reporting "0 files restored". Without this guard, an operator
// who manually empties or truncates the manifest mid-debug would see a
// success log while their backup files remain orphaned in backup/domains/.
func TestRestoreCommitmentFiles_RejectsEmptyManifest(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))

	origKV := []byte("ORIG_KV")
	backupPath := filepath.Join(backupDir, "v2.0-commitment.0-32.kv")
	require.NoError(t, os.WriteFile(backupPath, origKV, 0o644))
	// Zero-byte manifest — simulates hand-truncation or a partial editor write.
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, ".restore_manifest"), nil, 0o644))

	err := state.RestoreCommitmentFiles(t.Context(), agg.Dirs(), log.New())
	require.Error(t, err, "must refuse to proceed on an empty manifest")
	require.Contains(t, err.Error(), "manifest")
	require.Contains(t, err.Error(), "empty")

	// The real backup file must remain untouched so the operator can recover
	// after deleting the empty manifest.
	require.Equal(t, origKV, readFileBytes(t, backupPath),
		"backup file must not be moved when manifest validation fails")
	require.NoFileExists(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-32.kv"))
}
