package state

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

func TestStepRange(t *testing.T) {
	t.Parallel()
	stepSize := uint64(4)

	t.Run("simple range", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 1,
			endTxNum:   10,
		}

		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(0), startStep)
		require.Equal(t, kv.Step(2), endStep)
		require.Equal(t, uint64(2), f.StepCount(4))
	})

	t.Run("inner boundaries", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 4,
			endTxNum:   7,
		}

		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(1), startStep)
		require.Equal(t, kv.Step(1), endStep)
		require.Equal(t, uint64(0), f.StepCount(stepSize))
	})

	t.Run("outer boundaries", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 3,
			endTxNum:   8,
		}
		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(0), startStep)
		require.Equal(t, kv.Step(2), endStep)
		require.Equal(t, uint64(2), f.StepCount(stepSize))
	})
}

func TestFileItemWithMissedAccessor(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	// filesItem
	f1 := &FilesItem{
		startTxNum: 1,
		endTxNum:   10,
	}
	f2 := &FilesItem{
		startTxNum: 11,
		endTxNum:   20,
	}
	f3 := &FilesItem{
		startTxNum: 31,
		endTxNum:   40,
	}
	aggStep := uint64(10)

	df := newDirtyFiles()
	df.Set(f1)
	df.Set(f2)
	df.Set(f3)

	accessorForStep := func(fromStep, toStep kv.Step) []string {
		return []string{
			filepath.Join(tmp, fmt.Sprintf("testacc_%d_%d.bin", fromStep, toStep)),
			filepath.Join(tmp, fmt.Sprintf("testacc2_%d_%d.bin", fromStep, toStep)),
		}
	}
	accessorFor := func(item *FilesItem) []string {
		fromStep, toStep := item.StepRange(aggStep)
		return accessorForStep(fromStep, toStep)
	}

	// create accesssor files for f1, f2
	for _, fname := range accessorForStep(f1.StepRange(aggStep)) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer dir.RemoveFile(fname)
	}

	for _, fname := range accessorForStep(f2.StepRange(aggStep)) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer dir.RemoveFile(fname)
	}

	fileItems := fileItemsWithMissedAccessors(df.Items(), accessorFor)
	require.Len(t, fileItems, 1)
	require.Equal(t, f3, fileItems[0])
}

func TestVisibleFileVersion(t *testing.T) {
	t.Parallel()
	vf := visibleFile{src: &FilesItem{version: version.V2_1}}
	require.Equal(t, version.V2_1, vf.Version())
}

// openDirtyFiles must accept a mixed-version file set (v1.0/v2.0/v2.1) without tripping the
// MustSupport version-acceptance check, opening one dirty item per range from disk.
func TestOpenDirtyFilesAcceptsMixedVersions(t *testing.T) {
	t.Parallel()
	logger := log.New()
	_, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, logger)
	// Bump this (accounts) domain's read ceiling to v2.1 so the mixed-version fixtures open without
	// tripping MustSupport; unrelated to commitment-domain versioning.
	d.FileVersion.DataKV = version.Versions{Current: version.V2_1, MinSupported: version.V1_0}

	tmp := t.TempDir()
	cases := []struct {
		name string
		rng  string
	}{
		{"v1.0-accounts.0-1.kv", "0-1"},
		{"v2.0-accounts.1-2.kv", "1-2"},
		{"v2.1-accounts.2-3.kv", "2-3"},
	}
	fileNames := make([]string, 0, len(cases))
	for _, c := range cases {
		writeTestKVFile(t, filepath.Join(d.dirs.SnapDomain, c.name), tmp, logger)
		fileNames = append(fileNames, c.name)
	}

	d.scanDirtyFiles(fileNames)
	require.NoError(t, d.openDirtyFiles(t.Context(), fileNames))

	opened := make(map[string]bool)
	d.dirtyFiles.Scan(func(it *FilesItem) bool {
		from, to := it.StepRange(d.stepSize)
		require.NotNil(t, it.decompressor, "dirty file %d-%d must be opened", from, to)
		opened[fmt.Sprintf("%d-%d", from, to)] = true
		return true
	})

	for _, c := range cases {
		require.True(t, opened[c.rng], "range %s must open from %s", c.rng, c.name)
	}
}

// Two same-range commitment files of different versions collapse to one dirty item
// resolved to the highest version (v2.1); the lower v2.0 twin is left on disk, never opened.
func TestOpenDirtyFilesSameRangePrefersNewestVersion(t *testing.T) {
	t.Parallel()
	for _, order := range [][]string{
		{"v2.0-commitment.0-2.kv", "v2.1-commitment.0-2.kv"},
		{"v2.1-commitment.0-2.kv", "v2.0-commitment.0-2.kv"},
	} {
		t.Run(strings.Join(order, ","), func(t *testing.T) {
			logger := log.New()
			_, d := testDbAndDomainOfStep(t, statecfg.Schema.CommitmentDomain, 16, logger)
			tmp := t.TempDir()
			for _, name := range order {
				writeTestKVFile(t, filepath.Join(d.dirs.SnapDomain, name), tmp, logger)
			}

			d.scanDirtyFiles(order)
			require.NoError(t, d.openDirtyFiles(t.Context(), order))

			require.Equal(t, 1, d.dirtyFiles.Len(), "same-range duplicate must collapse to one dirty file")
			var opened *FilesItem
			d.dirtyFiles.Scan(func(it *FilesItem) bool { opened = it; return true })
			require.NotNil(t, opened)
			require.Equal(t, "v2.1-commitment.0-2.kv", filepath.Base(opened.decompressor.FilePath()), "newest version wins")

			_, err := os.Stat(filepath.Join(d.dirs.SnapDomain, "v2.0-commitment.0-2.kv"))
			require.NoError(t, err, "lower-version twin stays on disk, just unopened")
		})
	}
}

// TestFilterDirtyFiles_LegacyStepIndexedNaming pins the pre-v4.0 file-
// naming convention where the filename encodes step indices and the
// scan multiplies by stepSize to recover the txnum range. Anchor for
// the current behaviour so the v4 dispatch below is a strict addition
// rather than a replacement.
func TestFilterDirtyFiles_LegacyStepIndexedNaming(t *testing.T) {
	t.Parallel()
	stepSize := uint64(1000)
	logger := log.New()
	names := []string{
		"v1.0-accounts.0-256.kv",
		"v2.0-accounts.256-288.kv",
		"v2.2-accounts.288-289.kv",
	}
	got := filterDirtyFiles(names, stepSize, "accounts", "kv", logger)
	require.Len(t, got, 3)
	require.Equal(t, uint64(0), got[0].startTxNum)
	require.Equal(t, uint64(256_000), got[0].endTxNum)
	require.Equal(t, uint64(256_000), got[1].startTxNum)
	require.Equal(t, uint64(288_000), got[1].endTxNum)
	require.Equal(t, uint64(288_000), got[2].startTxNum)
	require.Equal(t, uint64(289_000), got[2].endTxNum)
}

// TestFilterDirtyFiles_V4RawTxnumNaming pins the v4.0+ convention: the
// filename encodes raw exclusive txnums directly. Without dispatch on
// TxNumNamingPivot the scan would treat the numbers as step indices and
// re-multiply by stepSize, producing wildly wrong ranges (e.g.
// startTxNum=256_000_000_000 for a v4 file that actually covers
// [256_000_000, 288_000_000)). This is the load-bearing correctness
// gate for mode-C v4-emit — without it every v4 file on disk gets
// silently mis-registered.
func TestFilterDirtyFiles_V4RawTxnumNaming(t *testing.T) {
	t.Parallel()
	stepSize := uint64(1000)
	logger := log.New()
	names := []string{
		"v4.0-accounts.256000-289250.kv", // covers [256000, 289250) — mid-step end
		"v4.0-accounts.0-1000.kv",        // covers [0, 1000) — one full step
	}
	got := filterDirtyFiles(names, stepSize, "accounts", "kv", logger)
	require.Len(t, got, 2)
	require.Equal(t, uint64(256_000), got[0].startTxNum)
	require.Equal(t, uint64(289_250), got[0].endTxNum,
		"v4.0 endTxNum must be read raw (mid-step 289_250), not re-multiplied to 289_250_000")
	require.Equal(t, uint64(0), got[1].startTxNum)
	require.Equal(t, uint64(1000), got[1].endTxNum)
}

// TestFilterDirtyFiles_MixedVersionsInSameScan pins that a directory
// holding BOTH legacy step-indexed and v4 raw-txnum files (which is the
// on-disk state during mode-C's v4 window) scans each with the correct
// convention.
func TestFilterDirtyFiles_MixedVersionsInSameScan(t *testing.T) {
	t.Parallel()
	stepSize := uint64(1000)
	logger := log.New()
	names := []string{
		"v2.0-accounts.256-288.kv",       // legacy: step indices → [256_000, 288_000)
		"v4.0-accounts.288000-289250.kv", // v4: raw txnums → [288_000, 289_250)
	}
	got := filterDirtyFiles(names, stepSize, "accounts", "kv", logger)
	require.Len(t, got, 2)
	require.Equal(t, uint64(256_000), got[0].startTxNum)
	require.Equal(t, uint64(288_000), got[0].endTxNum)
	require.Equal(t, uint64(288_000), got[1].startTxNum)
	require.Equal(t, uint64(289_250), got[1].endTxNum,
		"v4 file in mixed scan retains mid-step endTxNum")
}

// TestLastFullyCoveredStep pins the step-boundary formula that
// separates fully-covered from partially-covered steps. Called out
// because the mid-step case (mode-C v4 file, endTxN mid-step) is the
// specific correctness anchor for canPrune/StepsInFiles.
func TestLastFullyCoveredStep(t *testing.T) {
	t.Parallel()
	const ss = uint64(1000)

	require.Equal(t, kv.Step(0), lastFullyCoveredStep(0, ss), "endTxN=0: no data, no full step")
	require.Equal(t, kv.Step(0), lastFullyCoveredStep(500, ss), "endTxN < ss: no full step")
	require.Equal(t, kv.Step(0), lastFullyCoveredStep(999, ss), "endTxN one below boundary: still no full step")
	require.Equal(t, kv.Step(0), lastFullyCoveredStep(1000, ss), "endTxN==ss: step 0 fully covered")
	require.Equal(t, kv.Step(0), lastFullyCoveredStep(1500, ss), "endTxN mid-step-1: step 0 fully covered, step 1 partial")
	require.Equal(t, kv.Step(1), lastFullyCoveredStep(2000, ss), "endTxN==2*ss: step 1 fully covered")
	require.Equal(t, kv.Step(1), lastFullyCoveredStep(2999, ss), "endTxN one below 3*ss: step 1 still last full")
	require.Equal(t, kv.Step(100), lastFullyCoveredStep(101_000_000, ss*1000), "aligned 101M with ss=1M: last full = step 100")
	require.Equal(t, kv.Step(100), lastFullyCoveredStep(101_250_000, ss*1000), "mid-step 101.25M with ss=1M: last full = step 100 (step 101 partial)")
}

// TestFilterDirtyFiles_LowMajorParsesAsLegacy pins that any major
// version below TxNumNamingPivot's takes the step-multiplied branch.
func TestFilterDirtyFiles_LowMajorParsesAsLegacy(t *testing.T) {
	t.Parallel()
	stepSize := uint64(1000)
	logger := log.New()
	got := filterDirtyFiles([]string{"v0.0-accounts.5-6.kv"}, stepSize, "accounts", "kv", logger)
	require.Len(t, got, 1)
	require.Equal(t, uint64(5_000), got[0].startTxNum)
	require.Equal(t, uint64(6_000), got[0].endTxNum)
	_ = version.V4_0
}

// TestFilesItem_IsRawTxN_TwoEdges pins that a v4 file with EITHER a
// non-aligned startTxN OR a non-aligned endTxN is classified as v4.
// The two-v4-then-merge lifecycle emits v4 #1 (aligned start, mid-step
// end) and v4 #2 (mid-step start, aligned end); both must be caught by
// the predicate so the merge scheduler, retire, and read-side mask
// paths treat them uniformly.
func TestFilesItem_IsRawTxN_TwoEdges(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(1000)

	// Standard step-aligned file — NOT v4.
	require.False(t, (&FilesItem{startTxNum: 3000, endTxNum: 4000}).IsRawTxN(stepSize),
		"aligned [3000, 4000) at stepSize=1000 is a step-form file, not v4")

	// v4 #1 — aligned start, mid-step end (unwind emission).
	require.True(t, (&FilesItem{startTxNum: 3000, endTxNum: 3512}).IsRawTxN(stepSize),
		"v4 #1 [3000, 3512) has non-aligned end — must classify as v4")

	// v4 #2 — mid-step start, aligned end (retire tail emission).
	require.True(t, (&FilesItem{startTxNum: 3512, endTxNum: 4000}).IsRawTxN(stepSize),
		"v4 #2 [3512, 4000) has non-aligned start — must classify as v4")

	// Both edges non-aligned — still v4 (e.g. inner span produced by a
	// hypothetical mid-merge state).
	require.True(t, (&FilesItem{startTxNum: 3100, endTxNum: 3900}).IsRawTxN(stepSize),
		"both-non-aligned [3100, 3900) must classify as v4")
}

// TestDirtyFiles_V4OneEndTxNForStep_LocatesUnwindTail pins the primitive
// retire's dest-path helpers use to construct the complementary v4 #2
// path. Given a v4 #1 [step*ss, cut) in dirtyFiles, must return cut.
func TestDirtyFiles_V4OneEndTxNForStep_LocatesUnwindTail(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(1000)
	df := newDirtyFiles()

	df.Set(&FilesItem{startTxNum: 3000, endTxNum: 3512}) // v4 #1
	df.Set(&FilesItem{startTxNum: 2000, endTxNum: 3000}) // aligned neighbor

	endTxN, ok := df.V4OneEndTxNForStep(stepSize, kv.Step(3))
	require.True(t, ok, "v4 #1 present at step 3 must be found")
	require.Equal(t, uint64(3512), endTxN, "endTxN of v4 #1 must be returned so v4 #2 tail can start there")
}

// TestDirtyFiles_V4OneEndTxNForStep_NoneReturnsFalse pins the absence
// path: standard step-aligned files must not be misclassified as v4 #1.
func TestDirtyFiles_V4OneEndTxNForStep_NoneReturnsFalse(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(1000)
	df := newDirtyFiles()

	df.Set(&FilesItem{startTxNum: 3000, endTxNum: 4000}) // aligned, not v4

	_, ok := df.V4OneEndTxNForStep(stepSize, kv.Step(3))
	require.False(t, ok, "aligned file must not match v4 #1")
}

// TestDirtyFiles_V4PairForStep_TilesStep pins the merge-scheduler's
// pair-detection primitive: a v4 #1 [step*ss, cut) and v4 #2 [cut,
// (step+1)*ss) together tile the step and must be returned as a
// mergeable pair.
func TestDirtyFiles_V4PairForStep_TilesStep(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(1000)
	df := newDirtyFiles()

	v41 := &FilesItem{startTxNum: 3000, endTxNum: 3512}
	v42 := &FilesItem{startTxNum: 3512, endTxNum: 4000}
	df.Set(v41)
	df.Set(v42)

	got1, got2, ok := df.V4PairForStep(stepSize, kv.Step(3))
	require.True(t, ok, "v4 #1 and v4 #2 together tile step 3 [3000, 4000); expected V4PairForStep ok=true")
	require.Same(t, v41, got1, "v4 #1 pointer mismatch")
	require.Same(t, v42, got2, "v4 #2 pointer mismatch")
}

// TestDirtyFiles_V4PairForStep_PartialReturnsFalse: only v4 #1 present
// (v4 #2 hasn't been emitted by retire yet) — no pair. Prevents the
// merge scheduler from proposing an aligned merge before the tail
// exists.
func TestDirtyFiles_V4PairForStep_PartialReturnsFalse(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(1000)
	df := newDirtyFiles()

	// v4 #1 alone.
	df.Set(&FilesItem{startTxNum: 3000, endTxNum: 3512})

	_, _, ok := df.V4PairForStep(stepSize, kv.Step(3))
	require.False(t, ok, "v4 #1 alone must not be reported as a mergeable pair")
}

// TestDirtyFiles_V4PairForStep_RejectsMismatchedBoundary pins invariant
// #4 from the plan: v4 #1 ending at txN=X and v4 #2 starting at txN=X+G
// (gap G > 0) MUST NOT be reported as a pair — the union would leave
// a hole in the merged output.
func TestDirtyFiles_V4PairForStep_RejectsMismatchedBoundary(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(1000)
	df := newDirtyFiles()

	// v4 #1 ends at 3512, v4 #2 starts at 3513 — 1-txN gap.
	df.Set(&FilesItem{startTxNum: 3000, endTxNum: 3512})
	df.Set(&FilesItem{startTxNum: 3513, endTxNum: 4000})

	_, _, ok := df.V4PairForStep(stepSize, kv.Step(3))
	require.False(t, ok, "gap between v4 #1 end and v4 #2 start must reject pair")
}

// TestDirtyFiles_V4PairForStep_IgnoresOtherSteps: v4s for a different
// step shouldn't count toward this step's pair.
func TestDirtyFiles_V4PairForStep_IgnoresOtherSteps(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(1000)
	df := newDirtyFiles()

	// Step 3 has a v4 #1 but no v4 #2. Step 5 has a complete v4 pair.
	df.Set(&FilesItem{startTxNum: 3000, endTxNum: 3512}) // step 3 v4 #1
	df.Set(&FilesItem{startTxNum: 5000, endTxNum: 5512}) // step 5 v4 #1
	df.Set(&FilesItem{startTxNum: 5512, endTxNum: 6000}) // step 5 v4 #2

	_, _, ok3 := df.V4PairForStep(stepSize, kv.Step(3))
	require.False(t, ok3, "step 3 lacks a v4 #2 — no pair")

	v41, v42, ok5 := df.V4PairForStep(stepSize, kv.Step(5))
	require.True(t, ok5, "step 5 has a complete v4 pair")
	require.Equal(t, uint64(5000), v41.startTxNum)
	require.Equal(t, uint64(5512), v41.endTxNum)
	require.Equal(t, uint64(5512), v42.startTxNum)
	require.Equal(t, uint64(6000), v42.endTxNum)
}

func writeTestKVFile(t *testing.T, path, tmp string, logger log.Logger) {
	t.Helper()
	comp, err := seg.NewCompressor(t.Context(), "test", path, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer comp.Close()
	require.NoError(t, comp.AddWord([]byte("k")))
	require.NoError(t, comp.AddWord([]byte("v")))
	require.NoError(t, comp.Compress())
}

// TestOpenDirtyFilesReopensV4RawTxNFile pins the restart-safety
// invariant for mode-C v4 emit: a v4-named .kv file with mid-step
// endTxN written pre-restart must be successfully re-opened on the
// next process start. The bug this guards: openDirtyFiles used to
// call kvFileNameMask(fromStep, toStep) which produces "*-<domain>.
// <step>-<step>.kv" — never matches a v4 file whose actual name is
// v4.0-<domain>.<startTxN>-<endTxN>.kv, so the item ended up in
// invalidFileItems and the file was silently orphaned (Post-mortem:
// GetLatest for keys in the v4 file's range would fall through to
// the older step-aligned file it truncated — pre-unwind stale state).
func TestOpenDirtyFilesReopensV4RawTxNFile(t *testing.T) {
	t.Parallel()
	logger := log.New()
	const stepSize = uint64(16)
	_, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, stepSize, logger)

	// Mode-C boundary emit shape: fromTxN step-aligned (272*16),
	// endTxN mid-step (4360 lands inside step 272..273 which spans
	// [4352, 4368)). The prior bug: StepRange((272*16, 4360)) = (272,
	// 272), kvFileNameMask produced "*-accounts.272-272.kv" — never
	// matches the actual "v4.0-accounts.4352-4360.kv" filename.
	const fromTxN = uint64(272 * 16)
	const endTxN = uint64(4360)
	require.NotEqual(t, uint64(0), endTxN%stepSize, "test premise: endTxN must be mid-step to exercise the v4 dispatch")

	v4Name := "v4.0-accounts." + fmt.Sprintf("%d-%d", fromTxN, endTxN) + ".kv"
	writeTestKVFile(t, filepath.Join(d.dirs.SnapDomain, v4Name), t.TempDir(), logger)

	d.scanDirtyFiles([]string{v4Name})
	require.NoError(t, d.openDirtyFiles(t.Context(), []string{v4Name}))

	var opened *FilesItem
	d.dirtyFiles.Scan(func(it *FilesItem) bool { opened = it; return true })
	require.NotNil(t, opened, "v4 raw-txN file must survive scanDirtyFiles + openDirtyFiles as a live dirty item")
	require.NotNil(t, opened.decompressor, "v4 file's decompressor must be opened (kvFileNameMaskForItem picked the raw-txN mask)")
	require.Equal(t, fromTxN, opened.startTxNum)
	require.Equal(t, endTxN, opened.endTxNum)
	require.Equal(t, v4Name, filepath.Base(opened.decompressor.FilePath()))
}
