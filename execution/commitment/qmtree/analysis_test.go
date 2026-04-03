package qmtree_test

// Format-size analysis and integration benchmarks for qmtree proof structures.
//
// Two kinds of tests live here:
//
//   1. Pure format-size analysis (no DB, no network):
//      Computes proof sizes as a function of tree depth for representative
//      configurations (hoodi at various block heights, mainnet scale).
//      Run with: go test ./execution/commitment/qmtree/ -run TestFormatSizes -v
//
//   2. Integration benchmarks (requires real datadir):
//      Set QMTREE_DATADIR to a directory containing a synced qmtree dataset
//      (produced by `integration stage_exec_replay --experimental.qmtree`).
//      Run with: QMTREE_DATADIR=/path/to/snap go test ./execution/commitment/qmtree/ -run TestBench -v
//
// Output files are written to $TMPDIR/qmtree-analysis/ (or a path specified by
// QMTREE_OUT env var) as CSV files that can be imported into a spreadsheet.

import (
	"encoding/csv"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/qmtree"
)

// ---------------------------------------------------------------------------
// Section 1: Pure format-size analysis (mathematical, no DB required)
// ---------------------------------------------------------------------------

// upperLevels returns the number of UpperPath nodes in a ProofPath for a tree
// whose highest twig ID is youngestTwigId.
func upperLevels(youngestTwigId uint64) int {
	if youngestTwigId == 0 {
		return 0
	}
	// maxLevel = FIRST_LEVEL_ABOVE_TWIG + 63 - LeadingZeros64(youngestTwigId)
	// U = maxLevel - FIRST_LEVEL_ABOVE_TWIG
	return 63 - bits.LeadingZeros64(youngestTwigId) + 1
}

// proofPathBytes returns the byte length of ProofPath.ToBytes() for a tree of
// given depth (expressed as youngestTwigId).
// Formula: 8 + (U + OTHER_NODE_COUNT) * 32  where OTHER_NODE_COUNT = 13
func proofPathBytes(youngestTwigId uint64) int {
	const otherNodeCount = 13 // selfHash + 11 intraTwig + root
	u := upperLevels(youngestTwigId)
	return 8 + (u+otherNodeCount)*32
}

// callProofBytes returns the byte size of a QMCallProof with M accessed leaves
// across T unique twigs, for a tree with youngestTwigId.
func callProofBytes(M, T int, youngestTwigId uint64) int {
	u := upperLevels(youngestTwigId)
	root := 32
	perTwig := 8 + u*32
	perLeaf := 8 + 4*32 + 32 + 11*32 // txNum + leafData(4×32) + selfHash + 11 intraPeers
	return root + T*perTwig + M*perLeaf
}

// individualWitnessBytes returns the total byte size for M individual witnesses.
func individualWitnessBytes(M int, youngestTwigId uint64) int {
	return M * proofPathBytes(youngestTwigId)
}

// TestFormatSizes_ProofPath prints a table of ProofPath.ToBytes() sizes at
// various tree depths, matching the sizes of hoodi and mainnet at different
// sync points.
func TestFormatSizes_ProofPath(t *testing.T) {
	type row struct {
		label         string
		approxLeaves  uint64
		approxBlocks  string
		youngestTwigId uint64
	}

	rows := []row{
		{"Genesis (1 leaf)", 1, "0", 0},
		{"1 twig full (2048)", 2048, "~160 blks", 0},
		{"2 twigs", 4096, "~320 blks", 1},
		{"Hoodi 1k blocks", 11_851, "1,000", 5},
		{"Hoodi 2k blocks", 19_090, "2,000", 9},
		{"Hoodi 10k blocks", 95_000, "10,000", 46},
		{"Hoodi 100k blocks", 1_261_568, "100,000", 615},
		{"Hoodi 200k blocks", 2_400_000, "200,000", 1171},
		{"Mainnet 1M blocks", 1_200_000_000, "1,000,000 (est)", 585_937},
		{"Mainnet 22M blocks (tip)", 2_200_000_000, "22,000,000 (est)", 1_074_218},
	}

	t.Logf("%-36s  %10s  %6s  %10s", "Scenario", "Leaves", "U", "ProofPath (B)")
	t.Logf("%s", "-----------------------------------------------------------------------")
	for _, r := range rows {
		u := upperLevels(r.youngestTwigId)
		sz := proofPathBytes(r.youngestTwigId)
		t.Logf("%-36s  %10d  %6d  %10d", r.label, r.approxLeaves, u, sz)
	}

	// Write CSV output.
	outDir := outputDir(t)
	f := createCSV(t, filepath.Join(outDir, "proof-path-sizes.csv"))
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"scenario", "approx_leaves", "approx_blocks", "youngestTwigId", "upper_levels_U", "proof_path_bytes"})
	for _, r := range rows {
		u := upperLevels(r.youngestTwigId)
		sz := proofPathBytes(r.youngestTwigId)
		_ = w.Write([]string{r.label, str(r.approxLeaves), r.approxBlocks, str(r.youngestTwigId), strconv.Itoa(u), strconv.Itoa(sz)})
	}
	w.Flush()
	t.Logf("CSV written to %s", f.Name())
}

// TestFormatSizes_CallProof compares individual-witness vs compact-proof sizes
// for representative call access patterns at hoodi and mainnet scale.
func TestFormatSizes_CallProof(t *testing.T) {
	type scenario struct {
		label          string
		youngestTwigId uint64
		M              int // total leaves
		T              int // unique twigs (T <= M)
	}

	scenarios := []scenario{
		// Hoodi 100k blocks (~615 twigs, U≈10)
		{"hoodi: 1 leaf, 1 twig", 615, 1, 1},
		{"hoodi: 5 leaves, 1 twig", 615, 5, 1},
		{"hoodi: 10 leaves, 2 twigs", 615, 10, 2},
		{"hoodi: 20 leaves, 5 twigs", 615, 20, 5},
		{"hoodi: 20 leaves, 20 twigs", 615, 20, 20},
		{"hoodi: 50 leaves, 10 twigs", 615, 50, 10},
		{"hoodi: 100 leaves, 25 twigs", 615, 100, 25},
		// Mainnet tip (~1.07M twigs, U≈20)
		{"mainnet: 1 leaf, 1 twig", 1_074_218, 1, 1},
		{"mainnet: 20 leaves, 5 twigs", 1_074_218, 20, 5},
		{"mainnet: 20 leaves, 20 twigs", 1_074_218, 20, 20},
		{"mainnet: 100 leaves, 25 twigs", 1_074_218, 100, 25},
	}

	t.Logf("%-38s  %4s  %4s  %6s  %14s  %14s  %7s",
		"Scenario", "M", "T", "U", "Individual (B)", "Compact (B)", "Saving")
	t.Logf("%s", strings.Repeat("-", 90))
	for _, s := range scenarios {
		u := upperLevels(s.youngestTwigId)
		indiv := individualWitnessBytes(s.M, s.youngestTwigId)
		compact := callProofBytes(s.M, s.T, s.youngestTwigId)
		saving := 100.0 * (1.0 - float64(compact)/float64(indiv))
		t.Logf("%-38s  %4d  %4d  %6d  %14d  %14d  %6.0f%%",
			s.label, s.M, s.T, u, indiv, compact, saving)
	}

	outDir := outputDir(t)
	f := createCSV(t, filepath.Join(outDir, "call-proof-sizes.csv"))
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"scenario", "M_leaves", "T_twigs", "U_upper_levels",
		"individual_witness_bytes", "compact_proof_bytes", "saving_pct"})
	for _, s := range scenarios {
		u := upperLevels(s.youngestTwigId)
		indiv := individualWitnessBytes(s.M, s.youngestTwigId)
		compact := callProofBytes(s.M, s.T, s.youngestTwigId)
		saving := 100.0 * (1.0 - float64(compact)/float64(indiv))
		_ = w.Write([]string{s.label, strconv.Itoa(s.M), strconv.Itoa(s.T),
			strconv.Itoa(u), strconv.Itoa(indiv), strconv.Itoa(compact),
			fmt.Sprintf("%.1f", saving)})
	}
	w.Flush()
	t.Logf("CSV written to %s", f.Name())
}

// ---------------------------------------------------------------------------
// Section 2: Integration benchmarks (require QMTREE_DATADIR)
// ---------------------------------------------------------------------------

// TestBench_GetWitness benchmarks tracker.GetWitness over N sampled serial
// numbers and reports timing + actual proof sizes.
//
// Set QMTREE_DATADIR to a directory containing a synced qmtree dataset.
// Set QMTREE_SAMPLES=N to control sample count (default 1000).
func TestBench_GetWitness(t *testing.T) {
	snapDir := os.Getenv("QMTREE_DATADIR")
	if snapDir == "" {
		t.Skip("QMTREE_DATADIR not set — skipping integration benchmark")
	}

	const stepSize = 1_562_500
	tracker, err := qmtree.NewTracker(snapDir, uint64(stepSize))
	require.NoError(t, err, "open tracker from %s", snapDir)

	N := tracker.NextTxNum
	if N == 0 {
		t.Skip("tracker is empty — run stage_exec_replay first")
	}

	samples := sampleCount(t)
	step := max64(1, N/uint64(samples))

	t.Logf("tracker has %d leaves (~%d twigs), sampling every %d",
		N, N>>qmtree.TWIG_SHIFT, step)

	type result struct {
		txNum     uint64
		proofSize int
		dur       time.Duration
	}
	results := make([]result, 0, samples)

	for sn := uint64(0); sn < N; sn += step {
		start := time.Now()
		w, err := tracker.GetWitness(sn)
		dur := time.Since(start)
		if err != nil {
			continue
		}
		pb := w.Proof.ToBytes()
		results = append(results, result{txNum: sn, proofSize: len(pb), dur: dur})
	}

	var totalDur time.Duration
	var totalSize int
	var minSize, maxSize int = 1<<31, 0
	for _, r := range results {
		totalDur += r.dur
		totalSize += r.proofSize
		if r.proofSize < minSize {
			minSize = r.proofSize
		}
		if r.proofSize > maxSize {
			maxSize = r.proofSize
		}
	}
	n := len(results)
	if n == 0 {
		t.Fatal("no results")
	}
	t.Logf("GetWitness over %d samples:", n)
	t.Logf("  avg duration: %v", totalDur/time.Duration(n))
	t.Logf("  min/avg/max proof size: %d / %d / %d bytes", minSize, totalSize/n, maxSize)
	t.Logf("  theoretical size at N=%d twigs: %d bytes",
		N>>qmtree.TWIG_SHIFT, proofPathBytes(N>>qmtree.TWIG_SHIFT-1))

	// Write CSV.
	outDir := outputDir(t)
	f := createCSV(t, filepath.Join(outDir, "get-witness-bench.csv"))
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"serial_num", "proof_bytes", "duration_ns"})
	for _, r := range results {
		_ = w.Write([]string{str(r.txNum), strconv.Itoa(r.proofSize), strconv.FormatInt(r.dur.Nanoseconds(), 10)})
	}
	w.Flush()
	t.Logf("CSV written to %s", f.Name())
}

// TestBench_ProofSizeByTwig aggregates proof sizes by twig ID, showing whether
// proof sizes grow uniformly with tree depth or in steps.
func TestBench_ProofSizeByTwig(t *testing.T) {
	snapDir := os.Getenv("QMTREE_DATADIR")
	if snapDir == "" {
		t.Skip("QMTREE_DATADIR not set")
	}

	const stepSize = 1_562_500
	tracker, err := qmtree.NewTracker(snapDir, uint64(stepSize))
	require.NoError(t, err)

	N := tracker.NextTxNum
	if N == 0 {
		t.Skip("tracker is empty")
	}

	numTwigs := N >> qmtree.TWIG_SHIFT
	t.Logf("tree has %d leaves, %d twigs", N, numTwigs)

	outDir := outputDir(t)
	f := createCSV(t, filepath.Join(outDir, "proof-size-by-twig.csv"))
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"twig_id", "sample_sn", "proof_bytes", "upper_levels_actual"})

	// Sample first leaf in every 8th twig.
	twigStep := max64(1, numTwigs/128)
	for twigId := uint64(0); twigId < numTwigs; twigId += twigStep {
		sn := twigId << qmtree.TWIG_SHIFT
		witness, err := tracker.GetWitness(sn)
		if err != nil {
			continue
		}
		proofBz := witness.Proof.ToBytes()
		uActual := len(witness.Proof.UpperPath)
		_ = w.Write([]string{str(twigId), str(sn), strconv.Itoa(len(proofBz)), strconv.Itoa(uActual)})
	}
	w.Flush()
	t.Logf("CSV written to %s", f.Name())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func outputDir(t *testing.T) string {
	t.Helper()
	d := os.Getenv("QMTREE_OUT")
	if d == "" {
		d = filepath.Join(os.TempDir(), "qmtree-analysis")
	}
	require.NoError(t, os.MkdirAll(d, 0o755))
	return d
}

func createCSV(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	return f
}

func str(n uint64) string { return strconv.FormatUint(n, 10) }

func sampleCount(t *testing.T) int {
	t.Helper()
	if s := os.Getenv("QMTREE_SAMPLES"); s != "" {
		n, err := strconv.Atoi(s)
		require.NoError(t, err)
		return n
	}
	return 1000
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
