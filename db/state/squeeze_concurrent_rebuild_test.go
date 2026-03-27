package state_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
)

// rebuildResult holds the output of a single rebuild pass.
type rebuildResult struct {
	root      []byte
	duration  time.Duration
	fileSizes map[string]int64
}

// envIntOr reads an environment variable as uint64, returning def if unset or unparseable.
func envIntOr(key string, def uint64) uint64 {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}

// collectCommitmentFiles returns a map of filename→size for all commitment .kv files
// in the domain snapshot directory.
func collectCommitmentFiles(dirs datadir.Dirs) map[string]int64 {
	result := make(map[string]int64)
	paths, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	if err != nil {
		return result
	}
	commitStr := kv.CommitmentDomain.String()
	for _, p := range paths {
		name := filepath.Base(p)
		if !strings.Contains(name, commitStr) {
			continue
		}
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		result[name] = info.Size()
	}
	return result
}

// wipeCommitment removes all commitment state from both database tables and snapshot files,
// then rescans the aggregator's file state.
func wipeCommitment(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, dirs datadir.Dirs) {
	t.Helper()

	// Clear commitment tables in the database.
	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	buckets, err := rwTx.ListTables()
	require.NoError(t, err)

	commitStr := kv.CommitmentDomain.String()
	for _, b := range buckets {
		if strings.Contains(strings.ToLower(b), commitStr) {
			err = rwTx.ClearTable(b)
			require.NoError(t, err)
		}
	}
	require.NoError(t, rwTx.Commit())

	// Delete commitment .kv files and their siblings (.kvi, .kvei, .bt).
	paths, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	if err == nil {
		for _, p := range paths {
			if !strings.Contains(filepath.Base(p), commitStr) {
				continue
			}
			_ = dir.RemoveFile(p)
			// Remove sibling index/accessor files.
			base := strings.TrimSuffix(p, ".kv")
			for _, ext := range []string{".kvi", ".kvei", ".bt"} {
				_ = dir.RemoveFile(base + ext) // best-effort, may not exist
			}
		}
	}

	// Rescan file state.
	err = agg.OpenFolder()
	require.NoError(t, err)
}

// logComparison prints a summary comparing baseline, sequential, and concurrent rebuild results.
func logComparison(t *testing.T, baseline, sequential, concurrent rebuildResult, originalSizes map[string]int64) {
	t.Helper()

	totalSize := func(m map[string]int64) int64 {
		var s int64
		for _, v := range m {
			s += v
		}
		return s
	}

	origTotal := totalSize(originalSizes)
	seqTotal := totalSize(sequential.fileSizes)
	concTotal := totalSize(concurrent.fileSizes)

	t.Logf("=== Rebuild Comparison ===")
	t.Logf("Sequential: root=%x time=%s files=%d totalSize=%d",
		sequential.root, sequential.duration, len(sequential.fileSizes), seqTotal)
	t.Logf("Concurrent: root=%x time=%s files=%d totalSize=%d",
		concurrent.root, concurrent.duration, len(concurrent.fileSizes), concTotal)
	t.Logf("Root match: sequential=%v concurrent=%v",
		bytes.Equal(sequential.root, baseline.root),
		bytes.Equal(concurrent.root, baseline.root))

	if sequential.duration > 0 {
		speedup := float64(sequential.duration) / float64(concurrent.duration)
		t.Logf("Speedup: %.2fx", speedup)
	}

	if seqTotal > 0 {
		delta := concTotal - seqTotal
		pct := float64(delta) / float64(seqTotal) * 100
		t.Logf("Size delta (concurrent vs sequential): %d (%.1f%%)", delta, pct)
	}

	// Per-file comparison.
	t.Logf("--- Per-file sizes ---")
	allFiles := make(map[string]struct{})
	for f := range originalSizes {
		allFiles[f] = struct{}{}
	}
	for f := range sequential.fileSizes {
		allFiles[f] = struct{}{}
	}
	for f := range concurrent.fileSizes {
		allFiles[f] = struct{}{}
	}
	for f := range allFiles {
		t.Logf("  %-50s original=%-10d sequential=%-10d concurrent=%-10d",
			f, originalSizes[f], sequential.fileSizes[f], concurrent.fileSizes[f])
	}
	_ = origTotal // used via totalSize above
}

// reopenAggregator closes the current aggregator and reopens it with a fresh temporal DB wrapper.
// Returns the new temporal DB and aggregator.
func reopenAggregator(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, stepSize uint64) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	dirs := agg.Dirs()
	agg.Close()

	newAgg := testAgg(t, db, dirs, stepSize, log.New())
	newDB, err := temporal.New(db, newAgg)
	require.NoError(t, err)

	return newDB, newAgg
}
