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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type stepRange struct{ from, to uint64 }

// fileStepRanges returns the [from,to) step ranges of every .kv file for the given
// domain in the snapshot dir, parsed from the filename (e.g. v1.0-accounts.0-8.kv).
func fileStepRanges(t *testing.T, dirs datadir.Dirs, domain kv.Domain) []stepRange {
	t.Helper()
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	tag := "-" + domain.String() + "."
	var out []stepRange
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".kv") || !strings.Contains(name, tag) {
			continue
		}
		parts := strings.Split(name, ".")
		stepPart := parts[len(parts)-2] // segment right before "kv"
		fromTo := strings.Split(stepPart, "-")
		require.Len(t, fromTo, 2, "unexpected file name %s", name)
		from, err := strconv.ParseUint(fromTo[0], 10, 64)
		require.NoError(t, err)
		to, err := strconv.ParseUint(fromTo[1], 10, 64)
		require.NoError(t, err)
		out = append(out, stepRange{from, to})
	}
	return out
}

// readCommitmentStateRoot opens the commitment .kv file covering [from,to) steps and
// returns the state root stored under KeyCommitmentState, or ok=false if the key is absent.
func readCommitmentStateRoot(t *testing.T, dirs datadir.Dirs, from, to uint64) (common.Hash, bool) {
	t.Helper()
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	tag := "-" + kv.CommitmentDomain.String() + "."
	suffix := "." + strconv.FormatUint(from, 10) + "-" + strconv.FormatUint(to, 10) + ".kv"
	var path string
	for _, e := range entries {
		name := e.Name()
		if strings.Contains(name, tag) && strings.HasSuffix(name, suffix) {
			path = filepath.Join(dirs.SnapDomain, name)
			break
		}
	}
	require.NotEmpty(t, path, "no commitment file for range %d-%d in %s", from, to, dirs.SnapDomain)

	decomp, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer decomp.Close()
	compression := statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
	g := seg.NewReader(decomp.MakeGetter(), compression)
	for g.HasNext() {
		k, _ := g.Next(nil)
		require.True(t, g.HasNext(), "dangling key in %s", path)
		v, _ := g.Next(nil)
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			rootBytes, _, _, err := commitment.HexTrieExtractStateRoot(v)
			require.NoError(t, err)
			return common.BytesToHash(rootBytes), true
		}
	}
	return common.Hash{}, false
}

// rawCommitmentState returns the raw KeyCommitmentState value (and its embedded txNum) from
// the highest commitment file in dirs — used to simulate a live datadir whose DB still
// holds committed commitment state.
func rawCommitmentState(t *testing.T, dirs datadir.Dirs) ([]byte, uint64) {
	t.Helper()
	ranges := fileStepRanges(t, dirs, kv.CommitmentDomain)
	require.NotEmpty(t, ranges)
	top := ranges[0]
	for _, r := range ranges {
		if r.to > top.to {
			top = r
		}
	}
	suffix := "." + strconv.FormatUint(top.from, 10) + "-" + strconv.FormatUint(top.to, 10) + ".kv"
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	tag := "-" + kv.CommitmentDomain.String() + "."
	var path string
	for _, e := range entries {
		if strings.Contains(e.Name(), tag) && strings.HasSuffix(e.Name(), suffix) {
			path = filepath.Join(dirs.SnapDomain, e.Name())
			break
		}
	}
	require.NotEmpty(t, path)
	decomp, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer decomp.Close()
	g := seg.NewReader(decomp.MakeGetter(), statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression)
	for g.HasNext() {
		k, _ := g.Next(nil)
		require.True(t, g.HasNext())
		v, _ := g.Next(nil)
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			_, _, txNum, err := commitment.HexTrieExtractStateRoot(v)
			require.NoError(t, err)
			return bytes.Clone(v), txNum
		}
	}
	t.Fatalf("no KeyCommitmentState in %s", path)
	return nil, 0
}

// injectStaleCommitmentState commits a KeyCommitmentState into the DB's commitment domain,
// reproducing a live datadir whose DB holds committed commitment state. Without the
// rebuild's per-shard ClearTable, SeekCommitment would restore this state and the range
// rebuild would fail with "empty branch data during unfold".
func injectStaleCommitmentState(t *testing.T, ctx context.Context, db kv.TemporalRwDB, state []byte, txNum uint64) {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	sd.SetTxNum(txNum)
	require.NoError(t, sd.DomainPut(kv.CommitmentDomain, rwTx, commitmentdb.KeyCommitmentState, state, txNum, nil))
	require.NoError(t, sd.Flush(ctx, rwTx))
	sd.Close()
	require.NoError(t, rwTx.Commit())
}

// TestRebuildCommitmentFilesRange regenerates a single commitment file range into a
// separate output datadir and verifies (1) the source is untouched, (2) only the targeted
// range is produced, (3) the regenerated file carries KeyCommitmentState, and (4) its root
// matches the canonical full-rebuild root for that range.
func TestRebuildCommitmentFilesRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rebuild-range integration test in short mode")
	}

	origConcurrent := statecfg.ExperimentalConcurrentCommitment
	t.Cleanup(func() { statecfg.ExperimentalConcurrentCommitment = origConcurrent })
	statecfg.ExperimentalConcurrentCommitment = false

	const (
		numAccounts     = 4000
		totalSteps      = 8
		slotsPerAcct    = 2
		numCodeAccounts = 800
		stepSize        = 10
	)
	totalTxs := uint64(stepSize * totalSteps)
	ctx := t.Context()

	// --- Phase 1: data generation ---
	db, agg, dirs := testDbAndAggregatorForLargeData(t, stepSize, "")
	rawDB := db.(*temporal.DB).InternalDB() // stable underlying mdbx, survives reopen
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)

	accPerTx := max((numAccounts+totalTxs-1)/totalTxs, 1)
	storPerTx := max((numAccounts*slotsPerAcct+totalTxs-1)/totalTxs, 1)
	codePerTx := max((numCodeAccounts+totalTxs-1)/totalTxs, 1)

	var blockNum, accIdx, storAccIdx, storSlotIdx, codeIdx uint64
	for txNum := uint64(0); txNum < totalTxs; txNum++ {
		for i := uint64(0); i < accPerTx && accIdx < numAccounts; i++ {
			addr := makeAccountAddr(accIdx)
			acc := accounts.Account{Nonce: txNum, Balance: *uint256.NewInt(txNum * 1000), CodeHash: accounts.EmptyCodeHash}
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), txNum, nil))
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
			acc := accounts.Account{Nonce: txNum, Balance: *uint256.NewInt(txNum * 1000), CodeHash: codeHash}
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), txNum, nil))
			codeIdx++
		}
		if (txNum+1)%stepSize == 0 {
			_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			require.NoError(t, domains.Flush(ctx, rwTx))
			require.NoError(t, rawdbv3.TxNums.Append(rwTx, blockNum, txNum))
			blockNum++
		}
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	domains.Close()
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(totalTxs))

	// --- Phase 2: canonical full rebuild on the source (ground truth) ---
	db, agg = reopenAggregator(t, db, agg, stepSize)
	wipeCommitment(t, db, agg, dirs)
	_, err = state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), false)
	require.NoError(t, err)

	accRanges := fileStepRanges(t, dirs, kv.AccountsDomain)
	require.NotEmpty(t, accRanges, "expected at least one accounts file")
	t.Logf("account ranges: %v", accRanges)

	// Simulate a live datadir: leave committed commitment state in the DB so the range
	// rebuild's SeekCommitment would (without the per-shard ClearTable fix) restore it and
	// fail to unfold against the empty output dir.
	staleState, staleTxNum := rawCommitmentState(t, dirs)
	injectStaleCommitmentState(t, ctx, db, staleState, staleTxNum)

	// Exercise the concurrent path for the range rebuilds only (generation/full-rebuild
	// above stay sequential): with stale DB state restored by SeekCommitment, this is the
	// path that failed with "empty branch data during unfold".
	statecfg.ExperimentalConcurrentCommitment = true

	// Verify a base range (from==0) and the first non-zero start range.
	base := accRanges[0]
	require.Zero(t, base.from, "first account range should start at step 0")
	regenerateAndVerifyRange(t, ctx, dirs, rawDB, stepSize, base)

	var nonZero stepRange
	found := false
	for _, r := range accRanges {
		if r.from > 0 {
			nonZero, found = r, true
			break
		}
	}
	require.True(t, found, "expected at least one non-zero start range; got %v", accRanges)
	regenerateAndVerifyRange(t, ctx, dirs, rawDB, stepSize, nonZero)
}

// regenerateAndVerifyRange regenerates a single commitment range into a fresh output dir
// and asserts: (1) the source is untouched, (2) only files within the target range remain,
// (3) the regenerated file carries KeyCommitmentState, and (4) its root matches the
// canonical full-rebuild root for that range.
func regenerateAndVerifyRange(t *testing.T, ctx context.Context, dirs datadir.Dirs, rawDB kv.RwDB, stepSize uint64, target stepRange) {
	t.Helper()
	t.Logf("=== regenerating range steps %d-%d ===", target.from, target.to)

	srcRoot, ok := readCommitmentStateRoot(t, dirs, target.from, target.to)
	require.True(t, ok, "source commitment file %d-%d must contain KeyCommitmentState", target.from, target.to)
	require.NotEqual(t, common.Hash{}, srcRoot)

	srcCommitmentBefore := collectCommitmentFiles(t, dirs)

	outDirs := datadir.New(t.TempDir())
	require.NoError(t, state.LinkCommitmentRebuildInputs(dirs, outDirs, kv.Step(target.from), log.New()))

	// the target range's commitment file must not be present yet (only priors may be linked)
	for _, r := range fileStepRanges(t, outDirs, kv.CommitmentDomain) {
		require.LessOrEqual(t, r.to, target.from, "unexpected pre-existing commitment file %d-%d", r.from, r.to)
	}

	outAgg := state.NewTest(outDirs).StepSize(stepSize).Logger(log.New()).MustOpen(ctx, rawDB)
	t.Cleanup(outAgg.Close)
	require.NoError(t, outAgg.OpenFolder())
	outAgg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)
	outDB, err := temporal.New(rawDB, outAgg)
	require.NoError(t, err)

	_, err = state.RebuildCommitmentFilesRange(ctx, outDB, &rawdbv3.TxNums,
		kv.Step(target.from), kv.Step(target.to), log.New(), false)
	require.NoError(t, err)

	// (3) regenerated file carries the state root key ...
	gotRoot, ok := readCommitmentStateRoot(t, outDirs, target.from, target.to)
	require.True(t, ok, "regenerated commitment file MUST contain KeyCommitmentState")
	// (4) ... and matches the canonical root for that range.
	require.Equal(t, srcRoot, gotRoot)

	// (2) only the targeted range remains (priors were dropped before the merge).
	outRanges := fileStepRanges(t, outDirs, kv.CommitmentDomain)
	require.NotEmpty(t, outRanges)
	for _, r := range outRanges {
		require.True(t, r.from >= target.from && r.to <= target.to,
			"unexpected out-of-range commitment file %d-%d (target %d-%d)", r.from, r.to, target.from, target.to)
	}

	// (1) source datadir is untouched.
	require.Equal(t, srcCommitmentBefore, collectCommitmentFiles(t, dirs))
}
