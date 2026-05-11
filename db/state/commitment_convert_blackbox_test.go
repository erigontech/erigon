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
	_, _, convErr := state.ConvertCommitmentFileForTest(t.Context(), at, file, dstDir, opts, "", log.New())
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
	_, _, convErr := state.ConvertCommitmentFileForTest(t.Context(), at, file, dstDir, opts, "(1/1 files)", log.New())
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
	_, _, convErr := state.ConvertCommitmentFileForTest(t.Context(), at, file, dstDir, opts, "(1/1)", log.New())
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
	_, _, err = state.ConvertCommitmentFileForTest(t.Context(), at,
		file, tmpDir1, state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: true},
		"(rt fwd)", log.New())
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
	_, _, err = state.ConvertCommitmentFileForTest(t.Context(), at2,
		file2, tmpDir2, state.ConvertOpts{TargetSqueeze: false, TargetNibblesV2: false},
		"(rt back)", log.New())
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

	// Phase 3 invariant: every original commitment-named file is now in backup.
	backupDir := filepath.Join(agg.Dirs().Snap, "backup", "domains")
	backupFiles := snapshotCommitmentFiles(t, backupDir)
	require.Equal(t, len(origFiles), len(backupFiles),
		"backup must contain every original commitment file")
	for name, content := range origFiles {
		got, ok := backupFiles[name]
		require.True(t, ok, "missing %s in backup", name)
		require.True(t, bytes.Equal(content, got),
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

	snapDir := agg.Dirs().SnapDomain
	origFiles := snapshotCommitmentFiles(t, snapDir)
	require.NotEmpty(t, origFiles)

	// Forward: V1+unsqueezed → V2+squeezed.
	runOrchestrator(t, db, state.ConvertOpts{TargetSqueeze: true, TargetNibblesV2: true})

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
