package state_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
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

// TestConcurrentRebuildCommitment generates deterministic data, records a baseline state root,
// then rebuilds commitment sequentially (ground truth) and concurrently (primary target),
// comparing roots and logging file sizes and timing.
//
// Environment variables for tuning:
//   - TEST_ACCOUNTS (default 10000), TEST_STEPS (default 5), TEST_SLOTS_PER_ACCT (default 2)
//   - TEST_CODE_ACCOUNTS (default 3000), TEST_STEP_SIZE (default 10)
//   - TEST_DATADIR: optional persistent directory (uses t.TempDir() if empty)
func TestConcurrentRebuildCommitment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent rebuild integration test in short mode")
	}

	// --- Read env parameters ---
	numAccounts := envIntOr("TEST_ACCOUNTS", 10000)
	totalSteps := envIntOr("TEST_STEPS", 5)
	slotsPerAcct := envIntOr("TEST_SLOTS_PER_ACCT", 2)
	numCodeAccounts := envIntOr("TEST_CODE_ACCOUNTS", 3000)
	stepSize := envIntOr("TEST_STEP_SIZE", 10)
	persistentDir := os.Getenv("TEST_DATADIR")

	totalTxs := stepSize * totalSteps
	totalStorage := numAccounts * slotsPerAcct
	totalCode := numCodeAccounts
	totalKeys := numAccounts + totalStorage + totalCode

	t.Logf("=== Phase 1: Data Generation ===")
	t.Logf("Parameters: stepSize=%d totalSteps=%d totalTxs=%d", stepSize, totalSteps, totalTxs)
	t.Logf("Keys: accounts=%d storage=%d code=%d total=%d", numAccounts, totalStorage, totalCode, totalKeys)

	genStart := time.Now()

	// --- Create aggregator and DB ---
	db, agg, dirs := testDbAndAggregatorForLargeData(t, stepSize, persistentDir)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)

	ctx := context.Background()

	// --- Open transaction and shared domains ---
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	// --- Calculate per-tx batch sizes ---
	accPerTx := (numAccounts + totalTxs - 1) / totalTxs
	storPerTx := (totalStorage + totalTxs - 1) / totalTxs
	codePerTx := (totalCode + totalTxs - 1) / totalTxs
	if accPerTx == 0 {
		accPerTx = 1
	}
	if storPerTx == 0 {
		storPerTx = 1
	}
	if codePerTx == 0 {
		codePerTx = 1
	}

	t.Logf("Per-tx batch: accounts=%d storage=%d code=%d", accPerTx, storPerTx, codePerTx)

	var (
		blockNum    uint64
		accIdx      uint64
		storAccIdx  uint64
		storSlotIdx uint64
		codeIdx     uint64
		lastRoot    []byte
	)

	// --- Data generation loop ---
	for txNum := uint64(0); txNum < totalTxs; txNum++ {
		// Write accounts batch
		for i := uint64(0); i < accPerTx && accIdx < numAccounts; i++ {
			addr := makeAccountAddr(accIdx)
			acc := accounts.Account{
				Nonce:       txNum,
				Balance:     *uint256.NewInt(txNum * 1000),
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, nil)
			require.NoError(t, err)
			accIdx++
		}

		// Write storage batch
		for i := uint64(0); i < storPerTx && (storAccIdx*slotsPerAcct+storSlotIdx) < totalStorage; i++ {
			skey := makeStorageKey(storAccIdx, storSlotIdx, slotsPerAcct)
			var val [32]byte
			binary.BigEndian.PutUint64(val[24:], txNum)
			err = domains.DomainPut(kv.StorageDomain, rwTx, skey, val[:], txNum, nil)
			require.NoError(t, err)

			storSlotIdx++
			if storSlotIdx >= slotsPerAcct {
				storSlotIdx = 0
				storAccIdx++
			}
		}

		// Write code batch
		for i := uint64(0); i < codePerTx && codeIdx < numCodeAccounts; i++ {
			addr := makeAccountAddr(codeIdx)
			code := makeCodeValue(codeIdx)
			err = domains.DomainPut(kv.CodeDomain, rwTx, addr, code, txNum, nil)
			require.NoError(t, err)

			// Update account with real code hash
			codeHash := accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(code)))
			acc := accounts.Account{
				Nonce:       txNum,
				Balance:     *uint256.NewInt(txNum * 1000),
				CodeHash:    codeHash,
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			err = domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, nil)
			require.NoError(t, err)
			codeIdx++
		}

		// At step boundary: compute commitment, flush, record block->txNum mapping
		if (txNum+1)%stepSize == 0 {
			step := (txNum + 1) / stepSize
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			require.NotEmpty(t, rh)
			lastRoot = rh
			t.Logf("Step %d/%d (txNum=%d): root=%x", step, totalSteps, txNum, rh)

			err = domains.Flush(ctx, rwTx)
			require.NoError(t, err)

			err = rawdbv3.TxNums.Append(rwTx, blockNum, txNum)
			require.NoError(t, err)
			blockNum++
		}
	}

	// --- Close SharedDomains, commit tx ---
	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	domains.Close()
	require.NoError(t, rwTx.Commit())

	// --- Build snapshot files ---
	t.Logf("Building files for %d txs...", totalTxs)
	err = agg.BuildFiles(totalTxs)
	require.NoError(t, err)

	// --- Record baseline root and original sizes ---
	require.NotEmpty(t, lastRoot, "generation root must be non-empty")
	require.NotEqual(t, empty.RootHash.Bytes(), lastRoot, "generation root must differ from empty trie root")

	originalSizes := collectCommitmentFiles(dirs)

	// Extract root from files — this is the authoritative baseline for rebuild comparison,
	// since RebuildCommitmentFiles operates on file data. The last step may not be frozen
	// into files by BuildFiles, so rootFromFiles may differ from lastRoot (the in-memory
	// generation root). That's expected.
	var baselineRoot []byte
	{
		roTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()

		ac := state.AggTx(roTx)
		stateVal, ok, _, _, err := ac.DebugGetLatestFromFiles(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, math.MaxUint64)
		require.NoError(t, err)
		require.True(t, ok, "commitment state must be present in files")

		rootFromFiles, _, _, err := commitment.HexTrieExtractStateRoot(stateVal)
		require.NoError(t, err)
		require.NotEmpty(t, rootFromFiles)

		baselineRoot = rootFromFiles
		if !bytes.Equal(lastRoot, rootFromFiles) {
			t.Logf("NOTE: generation root=%x differs from file root=%x (last step not frozen — expected)", lastRoot, rootFromFiles)
		}

		roTx.Rollback()
	}

	genDuration := time.Since(genStart)
	t.Logf("Generation complete: root=%x fileRoot=%x time=%s accounts=%d storage=%d code=%d steps=%d totalTxs=%d",
		lastRoot, baselineRoot, genDuration, accIdx, storAccIdx*slotsPerAcct+storSlotIdx, codeIdx, totalSteps, totalTxs)
	t.Logf("Original commitment files: %d files", len(originalSizes))
	for f, sz := range originalSizes {
		t.Logf("  %s: %d bytes", f, sz)
	}

	// Store baseline result for later comparison
	baselineResult := rebuildResult{root: baselineRoot, duration: genDuration, fileSizes: originalSizes}

	// ========== Phase 2: Sequential Rebuild (Ground Truth) ==========
	t.Logf("=== Phase 2: Sequential Rebuild ===")

	// Close aggregator, reopen with fresh state
	db, agg = reopenAggregator(t, db, agg, stepSize)

	// Wipe all commitment state
	wipeCommitment(t, db, agg, dirs)

	// Run sequential rebuild (env var not set → defaults to false → sequential mode)
	seqStart := time.Now()
	seqRoot, err := state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), true)
	require.NoError(t, err)
	seqDuration := time.Since(seqStart)

	// Collect file sizes after rebuild
	seqSizes := collectCommitmentFiles(dirs)

	sequentialResult := rebuildResult{
		root:      seqRoot,
		duration:  seqDuration,
		fileSizes: seqSizes,
	}

	// Hard failure if sequential doesn't match baseline
	require.Equal(t, baselineRoot, sequentialResult.root,
		"sequential rebuild root must match baseline: baseline=%x sequential=%x", baselineRoot, sequentialResult.root)

	t.Logf("Sequential rebuild: root=%x time=%s files=%d",
		sequentialResult.root, sequentialResult.duration, len(sequentialResult.fileSizes))
	for f, sz := range seqSizes {
		t.Logf("  %s: %d bytes", f, sz)
	}

	// Phases 3-4 (concurrent rebuild, comparison) will be added in subsequent tasks.
	_ = baselineResult
	_ = dirs
	_ = agg
	_ = db
	_ = stepSize
}
