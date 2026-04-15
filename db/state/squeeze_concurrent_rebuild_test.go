package state_test

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/erigontech/erigon/db/state/statecfg"
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
func collectCommitmentFiles(t *testing.T, dirs datadir.Dirs) map[string]int64 {
	t.Helper()
	result := make(map[string]int64)
	paths, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	require.NoError(t, err)
	commitStr := kv.CommitmentDomain.String()
	for _, p := range paths {
		name := filepath.Base(p)
		if !strings.Contains(name, commitStr) {
			continue
		}
		info, err := os.Stat(p)
		require.NoError(t, err)
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
	require.NoError(t, err, "listing snapshot domain files")
	for _, p := range paths {
		if !strings.Contains(filepath.Base(p), commitStr) {
			continue
		}
		err = dir.RemoveFile(p)
		require.NoError(t, err, "removing commitment file: %s", p)
		// Remove sibling index/accessor files.
		base := strings.TrimSuffix(p, ".kv")
		for _, ext := range []string{".kvi", ".kvei", ".bt"} {
			_ = dir.RemoveFile(base + ext) // best-effort, may not exist
		}
	}

	// Rescan file state.
	err = agg.OpenFolder()
	require.NoError(t, err)

	// Verify the wipe was complete — no commitment files should remain.
	remaining := collectCommitmentFiles(t, dirs)
	require.Empty(t, remaining, "commitment files must be fully removed after wipe")
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
	t.Logf("Original:   files=%d totalSize=%d", len(originalSizes), origTotal)
	t.Logf("Sequential: root=%x time=%s files=%d totalSize=%d",
		sequential.root, sequential.duration, len(sequential.fileSizes), seqTotal)
	t.Logf("Concurrent: root=%x time=%s files=%d totalSize=%d",
		concurrent.root, concurrent.duration, len(concurrent.fileSizes), concTotal)
	t.Logf("Root match: concurrent_vs_sequential=%v",
		bytes.Equal(concurrent.root, sequential.root))

	if sequential.duration > 0 && concurrent.duration > 0 {
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
//   - TEST_ACCOUNTS (default 10000), TEST_STEPS (default 10), TEST_SLOTS_PER_ACCT (default 2)
//   - TEST_CODE_ACCOUNTS (default 3000), TEST_STEP_SIZE (default 10)
//   - TEST_DATADIR: optional persistent directory (uses t.TempDir() if empty)
func TestConcurrentRebuildCommitment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent rebuild integration test in short mode")
	}

	// Save and restore the global concurrent commitment flag.
	origConcurrent := statecfg.ExperimentalConcurrentCommitment
	t.Cleanup(func() { statecfg.ExperimentalConcurrentCommitment = origConcurrent })
	statecfg.ExperimentalConcurrentCommitment = false

	// --- Read env parameters ---
	// Default TEST_STEPS=10 ensures that after BuildFiles + merge the account domain
	// has 2+ files (e.g. 0-8 + 8-9). This is required so the rebuild loop processes
	// multiple ranges — the first range runs sequentially (empty trie after wipe) and
	// subsequent ranges exercise the concurrent ParallelHashSort path.
	numAccounts := envIntOr("TEST_ACCOUNTS", 10000)
	totalSteps := envIntOr("TEST_STEPS", 10)
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

		// Write code batch (initial deploy)
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

		// Mutate code for existing accounts (simulates contract upgrades).
		// Each tx updates a few already-deployed contracts with new bytecode,
		// producing code domain deltas across multiple steps.
		if codeIdx > 0 {
			mutationsPerTx := max(codePerTx/3, 1)
			for i := uint64(0); i < mutationsPerTx; i++ {
				// Pick an already-deployed contract deterministically
				targetIdx := (txNum*mutationsPerTx + i) % codeIdx
				addr := makeAccountAddr(targetIdx)
				// Generate evolved code: different from original by mixing in txNum
				code := makeCodeValue(targetIdx + txNum*numCodeAccounts)
				err = domains.DomainPut(kv.CodeDomain, rwTx, addr, code, txNum, nil)
				require.NoError(t, err)

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
			}
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

	// --- Record original sizes and generation root ---
	require.NotEmpty(t, lastRoot, "generation root must be non-empty")
	require.NotEqual(t, empty.RootHash.Bytes(), lastRoot, "generation root must differ from empty trie root")

	originalSizes := collectCommitmentFiles(t, dirs)

	genDuration := time.Since(genStart)
	t.Logf("Generation complete: root=%x time=%s accounts=%d storage=%d code=%d steps=%d totalTxs=%d",
		lastRoot, genDuration, accIdx, storAccIdx*slotsPerAcct+storSlotIdx, codeIdx, totalSteps, totalTxs)
	t.Logf("Original commitment files: %d files", len(originalSizes))
	for f, sz := range originalSizes {
		t.Logf("  %s: %d bytes", f, sz)
	}

	// Store generation result for the comparison report
	generationResult := rebuildResult{root: lastRoot, duration: genDuration, fileSizes: originalSizes}

	// ========== Phase 2: Sequential Rebuild (Ground Truth) ==========
	// The sequential rebuild is the ground truth. RebuildCommitmentFiles reads "latest from files"
	// which may differ from the incremental per-step commitment during generation. That's expected —
	// the rebuild root is authoritative for comparing sequential vs concurrent.
	t.Logf("=== Phase 2: Sequential Rebuild ===")

	// Ensure sequential mode.
	statecfg.ExperimentalConcurrentCommitment = false

	// Close aggregator, reopen with fresh state
	db, agg = reopenAggregator(t, db, agg, stepSize)

	// Wipe all commitment state
	wipeCommitment(t, db, agg, dirs)

	// Run sequential rebuild
	seqStart := time.Now()
	seqRoot, err := state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), true)
	require.NoError(t, err)
	seqDuration := time.Since(seqStart)

	// Collect file sizes after rebuild — must be non-empty (proves rebuild actually produced files)
	seqSizes := collectCommitmentFiles(t, dirs)
	require.NotEmpty(t, seqSizes, "sequential rebuild must produce commitment files")

	sequentialResult := rebuildResult{
		root:      seqRoot,
		duration:  seqDuration,
		fileSizes: seqSizes,
	}
	require.NotEmpty(t, sequentialResult.root, "sequential rebuild root must be non-empty")
	require.NotEqual(t, empty.RootHash.Bytes(), sequentialResult.root, "sequential rebuild root must differ from empty trie root")

	t.Logf("Sequential rebuild: root=%x time=%s files=%d",
		sequentialResult.root, sequentialResult.duration, len(sequentialResult.fileSizes))
	for f, sz := range seqSizes {
		t.Logf("  %s: %d bytes", f, sz)
	}

	// ========== Phase 3: Concurrent Rebuild (Primary Target) ==========
	t.Logf("=== Phase 3: Concurrent Rebuild ===")

	// Close aggregator, reopen with fresh state for concurrent run
	db, agg = reopenAggregator(t, db, agg, stepSize)

	// Wipe all commitment state
	wipeCommitment(t, db, agg, dirs)

	// Enable concurrent mode via the global flag.
	// This creates a ConcurrentPatriciaHashed trie and enables EnableParaTrieDB.
	// Whether ParallelHashSort actually fires depends on CanDoConcurrentNext() — the
	// first shard after a wipe always runs sequentially (empty trie has no branch at
	// nibble 0). After the first shard seeds trie state, subsequent shards exercise
	// the parallel path.
	statecfg.ExperimentalConcurrentCommitment = true

	// Run concurrent rebuild
	concStart := time.Now()
	concRoot, err := state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), true)
	require.NoError(t, err)
	concDuration := time.Since(concStart)

	// Collect file sizes after rebuild — must be non-empty (proves rebuild actually produced files)
	concSizes := collectCommitmentFiles(t, dirs)
	require.NotEmpty(t, concSizes, "concurrent rebuild must produce commitment files")

	concurrentResult := rebuildResult{
		root:      concRoot,
		duration:  concDuration,
		fileSizes: concSizes,
	}

	// The concurrent rebuild must match the sequential rebuild (ground truth).
	require.Equal(t, sequentialResult.root, concurrentResult.root,
		"concurrent rebuild root must match sequential: sequential=%x concurrent=%x", sequentialResult.root, concurrentResult.root)

	t.Logf("Concurrent rebuild: root=%x time=%s files=%d",
		concurrentResult.root, concurrentResult.duration, len(concurrentResult.fileSizes))
	for f, sz := range concSizes {
		t.Logf("  %s: %d bytes", f, sz)
	}

	// ========== Phase 4: Comparison Report ==========
	t.Logf("=== Phase 4: Comparison Report ===")
	logComparison(t, generationResult, sequentialResult, concurrentResult, originalSizes)
}

// TestConcurrentRebuildCommitmentNoSqueeze verifies that rebuilding commitment files
// with squeeze=false produces the same root for both sequential and concurrent modes.
func TestConcurrentRebuildCommitmentNoSqueeze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent rebuild integration test in short mode")
	}

	origConcurrent := statecfg.ExperimentalConcurrentCommitment
	t.Cleanup(func() { statecfg.ExperimentalConcurrentCommitment = origConcurrent })
	statecfg.ExperimentalConcurrentCommitment = false

	const (
		numAccounts     = 5000
		totalSteps      = 6
		slotsPerAcct    = 2
		numCodeAccounts = 1000
		stepSize        = 10
	)
	totalTxs := uint64(stepSize * totalSteps)

	t.Logf("Parameters: stepSize=%d totalSteps=%d totalTxs=%d accounts=%d storage=%d code=%d",
		stepSize, totalSteps, totalTxs, numAccounts, numAccounts*slotsPerAcct, numCodeAccounts)

	// --- Phase 1: Data generation ---
	db, agg, dirs := testDbAndAggregatorForLargeData(t, stepSize, "")
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	accPerTx := max((numAccounts+totalTxs-1)/totalTxs, 1)
	storPerTx := max((numAccounts*slotsPerAcct+totalTxs-1)/totalTxs, 1)
	codePerTx := max((numCodeAccounts+totalTxs-1)/totalTxs, 1)

	var (
		blockNum    uint64
		accIdx      uint64
		storAccIdx  uint64
		storSlotIdx uint64
		codeIdx     uint64
		lastRoot    []byte
	)

	for txNum := uint64(0); txNum < totalTxs; txNum++ {
		for i := uint64(0); i < accPerTx && accIdx < numAccounts; i++ {
			addr := makeAccountAddr(accIdx)
			acc := accounts.Account{
				Nonce:    txNum,
				Balance:  *uint256.NewInt(txNum * 1000),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, nil))
			accIdx++
		}

		for i := uint64(0); i < storPerTx && (storAccIdx*slotsPerAcct+storSlotIdx) < numAccounts*slotsPerAcct; i++ {
			skey := makeStorageKey(storAccIdx, storSlotIdx, slotsPerAcct)
			var val [32]byte
			binary.BigEndian.PutUint64(val[24:], txNum)
			require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, skey, val[:], txNum, nil))
			storSlotIdx++
			if storSlotIdx >= slotsPerAcct {
				storSlotIdx = 0
				storAccIdx++
			}
		}

		for i := uint64(0); i < codePerTx && codeIdx < numCodeAccounts; i++ {
			addr := makeAccountAddr(codeIdx)
			code := makeCodeValue(codeIdx)
			require.NoError(t, domains.DomainPut(kv.CodeDomain, rwTx, addr, code, txNum, nil))
			codeHash := accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(code)))
			acc := accounts.Account{
				Nonce:    txNum,
				Balance:  *uint256.NewInt(txNum * 1000),
				CodeHash: codeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr, buf, txNum, nil))
			codeIdx++
		}

		if (txNum+1)%stepSize == 0 {
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			lastRoot = rh
			require.NoError(t, domains.Flush(ctx, rwTx))
			require.NoError(t, rawdbv3.TxNums.Append(rwTx, blockNum, txNum))
			blockNum++
		}
	}

	require.NoError(t, domains.Flush(ctx, rwTx))
	domains.Close()
	require.NoError(t, rwTx.Commit())

	require.NoError(t, agg.BuildFiles(totalTxs))
	require.NotEmpty(t, lastRoot)

	// --- Phase 2: Sequential rebuild (squeeze=false) ---
	t.Logf("=== Sequential Rebuild (no squeeze) ===")
	statecfg.ExperimentalConcurrentCommitment = false
	db, agg = reopenAggregator(t, db, agg, stepSize)
	wipeCommitment(t, db, agg, dirs)

	seqRoot, err := state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), false)
	require.NoError(t, err)
	require.NotEmpty(t, seqRoot)
	require.NotEqual(t, empty.RootHash.Bytes(), seqRoot)
	seqSizes := collectCommitmentFiles(t, dirs)
	require.NotEmpty(t, seqSizes, "sequential rebuild must produce commitment files")
	t.Logf("Sequential: root=%x files=%d", seqRoot, len(seqSizes))

	// --- Phase 3: Concurrent rebuild (squeeze=false) ---
	t.Logf("=== Concurrent Rebuild (no squeeze) ===")
	statecfg.ExperimentalConcurrentCommitment = true
	db, agg = reopenAggregator(t, db, agg, stepSize)
	wipeCommitment(t, db, agg, dirs)

	concRoot, err := state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), false)
	require.NoError(t, err)
	require.NotEmpty(t, concRoot)
	concSizes := collectCommitmentFiles(t, dirs)
	require.NotEmpty(t, concSizes, "concurrent rebuild must produce commitment files")
	t.Logf("Concurrent: root=%x files=%d", concRoot, len(concSizes))

	// Roots must match between sequential and concurrent
	require.Equal(t, seqRoot, concRoot,
		"no-squeeze roots must match: sequential=%x concurrent=%x", seqRoot, concRoot)
}
