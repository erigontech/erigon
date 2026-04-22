package state_test

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	randOld "math/rand"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type testAggConfig struct {
	stepSize                         uint64
	disableCommitmentBranchTransform bool
}

func testDbAggregatorWithFiles(tb testing.TB, cfg *testAggConfig) (kv.TemporalRwDB, *state.Aggregator) {
	tb.Helper()
	txCount := int(cfg.stepSize) * 32 // will produce files up to step 31, good because covers different ranges (16, 8, 4, 2, 1)
	db, agg := testDbAggregatorWithNoFiles(tb, txCount, cfg)

	// build files out of db
	err := agg.BuildFiles(uint64(txCount))
	require.NoError(tb, err)
	return db, agg
}

type rndGen struct {
	*rand.Rand
	oldGen *randOld.Rand
}

func newRnd(seed uint64) *rndGen {
	return &rndGen{
		Rand:   rand.New(rand.NewChaCha8([32]byte{byte(seed)})),
		oldGen: randOld.New(randOld.NewSource(int64(seed))),
	}
}
func (r *rndGen) IntN(n int) int                   { return int(r.Uint64N(uint64(n))) }
func (r *rndGen) Read(p []byte) (n int, err error) { return r.oldGen.Read(p) } // seems `go1.22` doesn't have `Read` method on `math/v2` generator

func generateInputData(tb testing.TB, keySize, valueSize, keyCount int) ([][]byte, [][]byte) {
	tb.Helper()

	rnd := newRnd(0)
	values := make([][]byte, keyCount)
	keys := make([][]byte, keyCount)

	bk, bv := make([]byte, keySize), make([]byte, valueSize)
	for i := 0; i < keyCount; i++ {
		n, err := rnd.Read(bk)
		require.Equal(tb, keySize, n)
		require.NoError(tb, err)
		keys[i] = common.Copy(bk[:n])

		n, err = rnd.Read(bv[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		values[i] = common.Copy(bv[:n])
	}
	return keys, values
}

// testDbAndAggregatorForLargeData creates a temporal DB + aggregator sized for large datasets (10M+ keys).
// When persistentDir is non-empty, creates an on-disk MDBX at that path (for integration binary compatibility).
// When persistentDir is empty, uses t.TempDir() with InMem MDBX.
// Returns dirs so the caller knows the output path.
func testDbAndAggregatorForLargeData(tb testing.TB, aggStep uint64, persistentDir string) (kv.TemporalRwDB, *state.Aggregator, datadir.Dirs) {
	tb.Helper()
	logger := log.New()

	var dirs datadir.Dirs
	var db kv.RwDB

	if persistentDir != "" {
		dirs = datadir.New(persistentDir)
		db = mdbx.New(dbcfg.ChainDB, logger).
			Path(dirs.Chaindata).
			GrowthStep(64 * datasize.MB).
			MapSize(16 * datasize.GB).
			MustOpen()
	} else {
		dirs = datadir.New(tb.TempDir())
		db = mdbx.New(dbcfg.ChainDB, logger).
			InMem(tb, dirs.Chaindata).
			GrowthStep(64 * datasize.MB).
			MapSize(16 * datasize.GB).
			MustOpen()
	}
	tb.Cleanup(db.Close)

	agg := testAgg(tb, db, dirs, aggStep, logger)
	err := agg.OpenFolder()
	require.NoError(tb, err)
	tdb, err := temporal.New(db, agg)
	require.NoError(tb, err)
	tb.Cleanup(tdb.Close)
	return tdb, agg, dirs
}

func testDbAndAggregatorv3(tb testing.TB, aggStep uint64) (kv.TemporalRwDB, *state.Aggregator) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	agg := testAgg(tb, db, dirs, aggStep, logger)
	err := agg.OpenFolder()
	require.NoError(tb, err)
	tdb, err := temporal.New(db, agg)
	require.NoError(tb, err)
	tb.Cleanup(tdb.Close)
	return tdb, agg
}

func testAgg(tb testing.TB, db kv.RwDB, dirs datadir.Dirs, aggStep uint64, logger log.Logger) *state.Aggregator {
	tb.Helper()

	agg := state.NewTest(dirs).StepSize(aggStep).Logger(logger).MustOpen(tb.Context(), db)
	tb.Cleanup(agg.Close)
	return agg
}

func testDbAggregatorWithNoFiles(tb testing.TB, txCount int, cfg *testAggConfig) (kv.TemporalRwDB, *state.Aggregator) {
	tb.Helper()
	db, agg := testDbAndAggregatorv3(tb, cfg.stepSize)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, !cfg.disableCommitmentBranchTransform)

	ctx := tb.Context()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err := db.BeginTemporalRw(tb.Context())
	require.NoError(tb, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(tb.Context(), rwTx, log.New())
	require.NoError(tb, err)
	defer domains.Close()

	keys, vals := generateInputData(tb, length.Addr, 5, txCount)
	tb.Logf("keys %d vals %d\n", len(keys), len(vals))

	var txNum, blockNum uint64
	for i := 0; i < len(vals); i++ {
		txNum = uint64(i)

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, keys[j])
			require.NoError(tb, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev)
			require.NoError(tb, err)
		}
		if uint64(i+1)%agg.StepSize() == 0 {
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(tb, err)
			require.NotEmpty(tb, rh)
		}
	}

	err = domains.Flush(tb.Context(), rwTx)
	require.NoError(tb, err)

	require.NoError(tb, rwTx.Commit())

	return db, agg
}

func TestAggregator_SqueezeCommitment(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	cfgd := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfgd)

	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	var blockNum uint64
	// get latest commited root
	latestRoot, err := domains.ComputeCommitment(t.Context(), rwTx, false, blockNum, 0, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, latestRoot)
	domains.Close()

	// now do the squeeze
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)
	err = state.SqueezeCommitmentFiles(t.Context(), state.AggTx(rwTx), log.New())
	require.NoError(t, err)

	//agg.recalcVisibleFiles(matgh.MaxUint64)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)

	// collect account keys to trigger commitment
	acit, err := rwTx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, -1)
	require.NoError(t, err)
	defer acit.Close()

	trieCtx := domains.GetCommitmentContext()
	require.NoError(t, err)
	for acit.HasNext() {
		k, _, err := acit.Next()
		require.NoError(t, err)
		trieCtx.TouchKey(kv.AccountsDomain, string(k), nil)
	}

	// check if the commitment is the same
	root, err := domains.ComputeCommitment(t.Context(), rwTx, false, blockNum, 0, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, root)
	require.Equal(t, latestRoot, root)
	require.NotEqual(t, empty.RootHash.Bytes(), root)
}

func TestAggregator_RebuildCommitmentBasedOnFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{
		stepSize:                         10,
		disableCommitmentBranchTransform: false,
	})

	var rootInFiles []byte
	var fPaths []string

	{
		tx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		ac := state.AggTx(tx)

		// collect latest root from each available file
		stateVal, ok, _, _, _ := ac.DebugGetLatestFromFiles(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, math.MaxUint64)
		require.True(t, ok)
		rootInFiles, _, _, err = commitment.HexTrieExtractStateRoot(stateVal)
		require.NoError(t, err)

		for _, f := range ac.Files(kv.CommitmentDomain) {
			fPaths = append(fPaths, f.Fullpath())
		}
		tx.Rollback()
		agg.Close()
		//db.Close()
	}

	agg = testAgg(t, db, agg.Dirs(), agg.StepSize(), log.New())
	db, err := temporal.New(db, agg)
	require.NoError(t, err)
	defer db.Close()

	// now clean all commitment files along with related db buckets
	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	buckets, err := rwTx.ListTables()
	require.NoError(t, err)
	for _, b := range buckets {
		if strings.Contains(strings.ToLower(b), kv.CommitmentDomain.String()) {
			//size, err := rwTx.BucketSize(b)
			//require.NoError(t, err)
			//t.Logf("cleaned table %s: %d keys", b, size)

			err = rwTx.ClearTable(b)
			require.NoError(t, err)
		}
	}
	require.NoError(t, rwTx.Commit())

	for _, fn := range fPaths {
		if strings.Contains(fn, kv.CommitmentDomain.String()) {
			require.NoError(t, dir.RemoveFile(fn))
			//t.Logf("removed file %s", filepath.Base(fn))
		}
	}
	err = agg.OpenFolder()
	require.NoError(t, err)

	ctx := t.Context()
	finalRoot, err := state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), true)
	require.NoError(t, err)
	require.NotEmpty(t, finalRoot)
	require.NotEqual(t, empty.RootHash.Bytes(), finalRoot)

	require.Equal(t, rootInFiles, finalRoot)
}

func composite(k, k2 []byte) []byte {
	return append(common.Copy(k), k2...)
}

// makeAccountAddr generates a deterministic 20-byte account address with uniform
// first-nibble distribution using sha256(0xAC || idx).
func makeAccountAddr(idx uint64) []byte {
	var buf [9]byte
	buf[0] = 0xAC
	binary.BigEndian.PutUint64(buf[1:], idx)
	h := sha256.Sum256(buf[:])
	return h[:length.Addr]
}

// makeStorageKey generates a deterministic 52-byte composite storage key
// (20-byte addr + 32-byte slot) using sha256(0x57 || addrIdx*maxSlots+slotIdx).
func makeStorageKey(addrIdx, slotIdx uint64, maxSlots uint64) []byte {
	addr := makeAccountAddr(addrIdx)
	var buf [9]byte
	buf[0] = 0x57
	binary.BigEndian.PutUint64(buf[1:], addrIdx*maxSlots+slotIdx)
	h := sha256.Sum256(buf[:])
	return composite(addr, h[:length.Hash])
}

// makeCodeValue generates deterministic bytecode of 32-256 bytes.
// Size is derived from idx for full reproducibility independent of call order.
func makeCodeValue(idx uint64) []byte {
	size := 32 + int(idx%225) // 32..256 bytes
	code := make([]byte, size)
	// Use sha256 of index as repeatable seed data
	var buf [9]byte
	buf[0] = 0xCD
	binary.BigEndian.PutUint64(buf[1:], idx)
	h := sha256.Sum256(buf[:])
	// Fill code with hash-derived bytes, repeating as needed
	for i := 0; i < size; i++ {
		code[i] = h[i%len(h)]
	}
	return code
}

func TestMakeAccountAddr_NibbleDistribution(t *testing.T) {
	nibbles := make(map[byte]int, 16)
	const count = 1000
	for i := uint64(0); i < count; i++ {
		addr := makeAccountAddr(i)
		firstNibble := addr[0] >> 4
		nibbles[firstNibble]++
	}
	// All 16 nibble values must be present
	for n := byte(0); n < 16; n++ {
		require.Positive(t, nibbles[n], "missing first nibble %x in %d generated keys", n, count)
	}
	t.Logf("nibble distribution over %d keys: %v", count, nibbles)
}

func TestAggregatorV3_RestartOnDatadir(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	t.Run("BPlus", func(t *testing.T) {
		rc := runCfg{
			aggStep:  50,
			useBplus: true,
		}
		aggregatorV3_RestartOnDatadir(t, rc)
	})
	t.Run("B", func(t *testing.T) {
		rc := runCfg{
			aggStep: 50,
		}
		aggregatorV3_RestartOnDatadir(t, rc)
	})

}

type runCfg struct {
	aggStep      uint64
	useBplus     bool
	compressVals bool
	largeVals    bool
}

// here we create a bunch of updates for further aggregation.
// FinishTx should merge underlying files several times
// Expected that:
// - we could close first aggregator and open another with previous data still available
// - new aggregator SeekCommitment must return txNum equal to amount of total txns
func aggregatorV3_RestartOnDatadir(t *testing.T, rc runCfg) {
	t.Helper()
	ctx := t.Context()
	logger := log.New()
	aggStep := rc.aggStep
	db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	var latestCommitTxNum uint64
	rnd := newRnd(0)

	someKey := []byte("somekey")
	txs := (aggStep / 2) * 19
	t.Logf("step=%d tx_count=%d", aggStep, txs)
	var aux [8]byte
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite uint64
	addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
	var txNum, blockNum uint64
	for i := uint64(1); i <= txs; i++ {
		txNum = i
		binary.BigEndian.PutUint64(aux[:], txNum)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)
		//keys[txNum-1] = append(addr, loc...)
		acc := accounts.Account{
			Nonce:       1,
			Balance:     *uint256.NewInt(rnd.Uint64()),
			CodeHash:    accounts.EmptyCodeHash,
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf, txNum, nil)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, tx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, nil)
		require.NoError(t, err)

		err = domains.DomainPut(kv.CommitmentDomain, tx, someKey, common.Copy(aux[:]), txNum, nil)
		require.NoError(t, err)
		maxWrite = txNum
	}
	_, err = domains.ComputeCommitment(ctx, tx, true, blockNum, txNum, "", nil)
	require.NoError(t, err)

	err = domains.Flush(t.Context(), tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	agg.Close()

	// Start another aggregator on same datadir
	anotherAgg := state.NewTest(agg.Dirs()).StepSize(aggStep).Logger(logger).MustOpen(t.Context(), db)
	defer anotherAgg.Close()
	require.NoError(t, anotherAgg.OpenFolder())

	db, err = temporal.New(db, anotherAgg) // to set aggregator in the db
	require.NoError(t, err)
	defer db.Close()

	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	//anotherAgg.SetTx(rwTx)
	startTx := anotherAgg.EndTxNumMinimax()
	dom2, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer dom2.Close()

	latestTxNum, _, err := dom2.SeekCommitment(ctx, rwTx)

	require.NoError(t, err)
	require.GreaterOrEqual(t, latestTxNum, startTx)
	require.GreaterOrEqual(t, latestTxNum, latestCommitTxNum)
	_ = latestTxNum
	rwTx.Rollback()

	// Check the history
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	v, _, err := roTx.GetLatest(kv.CommitmentDomain, someKey)
	require.NoError(t, err)
	require.Equal(t, maxWrite, binary.BigEndian.Uint64(v))
}

// TestGenerateCommitmentRebuildData generates a dataset with valid commitment roots.
// The resulting datadir can be used for manual testing of
// `integration commitment rebuild`.
//
// Skipped under `-short`. In the default (non-short) run it uses a small
// CI-safe dataset (1K accounts, 3 steps). To generate the full-scale dataset
// (3M accounts, 59 steps, ~10M keys) for manual integration testing, set
// TEST_DATADIR to a persistent output directory — that both switches the
// parameters to full scale and writes the files to a reusable location.
//
// Environment variables:
//   - TEST_DATADIR: optional persistent output directory. When set, switches
//     to full-scale parameters and requires a long timeout. Unset (default):
//     small CI-safe parameters, writes to t.TempDir().
func TestGenerateCommitmentRebuildData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping data generation in short mode")
	}

	persistentDir := dbg.EnvString("TEST_DATADIR", "")

	// Fail early if persistent directory already contains data (avoids mixing old+new state).
	// Check chaindata and all snapshot subdirectories that OpenFolder/scanDirs will read.
	if persistentDir != "" {
		dirs := datadir.New(persistentDir)
		for _, sub := range []string{dirs.Chaindata, dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
			if entries, err := os.ReadDir(sub); err == nil && len(entries) > 0 {
				t.Fatalf("TEST_DATADIR %q already contains data in %s; use an empty directory or remove the existing data first", persistentDir, sub)
			}
		}
	}

	// Default: small CI-safe parameters.
	var (
		stepSize        uint64 = 10
		totalSteps      uint64 = 3
		numAccounts     uint64 = 1000
		slotsPerAcct    uint64 = 2
		numCodeAccounts uint64 = 300
	)

	// Scale up only when a persistent output directory is provided — this is
	// the manual integration-testing path.
	if persistentDir != "" {
		stepSize = 100
		totalSteps = 59
		numAccounts = 3_000_000
		slotsPerAcct = 2
		numCodeAccounts = 1_000_000
	}

	totalTxs := stepSize * totalSteps
	totalStorage := numAccounts * slotsPerAcct
	totalCode := numCodeAccounts
	totalKeys := numAccounts + totalStorage + totalCode

	t.Logf("Parameters: stepSize=%d totalSteps=%d totalTxs=%d", stepSize, totalSteps, totalTxs)
	t.Logf("Keys: accounts=%d storage=%d code=%d total=%d", numAccounts, totalStorage, totalCode, totalKeys)

	db, agg, dirs := testDbAndAggregatorForLargeData(t, stepSize, persistentDir)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)

	ctx := t.Context()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	// Calculate per-tx batch sizes (ceiling division to ensure all keys are written)
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

		// At step boundary: compute commitment, flush, and record block→txNum mapping
		if (txNum+1)%stepSize == 0 {
			step := (txNum + 1) / stepSize
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			require.NotEmpty(t, rh)
			lastRoot = rh
			t.Logf("Step %d/%d (txNum=%d): root=%x", step, totalSteps, txNum, rh)

			err = domains.Flush(ctx, rwTx)
			require.NoError(t, err)

			// Populate MaxTxNum table so `integration commitment rebuild` passes TxNums check
			err = rawdbv3.TxNums.Append(rwTx, blockNum, txNum)
			require.NoError(t, err)
			blockNum++
		}
	}

	// Final flush and commit
	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	domains.Close()
	require.NoError(t, rwTx.Commit())

	// Build files
	t.Logf("Building files for %d txs...", totalTxs)
	err = agg.BuildFiles(totalTxs)
	require.NoError(t, err)

	// Validate
	require.NotEmpty(t, lastRoot, "final root must be non-empty")
	require.NotEqual(t, empty.RootHash.Bytes(), lastRoot, "final root must differ from empty trie root")

	t.Logf("Done. Final root: %x", lastRoot)
	t.Logf("Output datadir: %s", dirs.DataDir)
	if persistentDir != "" {
		fmt.Fprintf(os.Stderr, "\n=== DATADIR: %s ===\n", dirs.DataDir)
	}
}

func TestAggregatorV3_SharedDomains(t *testing.T) {
	t.Parallel()
	db, _ := testDbAndAggregatorv3(t, 20)
	ctx := t.Context()

	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	changesetAt5 := &changeset.StateChangeSet{}
	changesetAt3 := &changeset.StateChangeSet{}

	keys, vals := generateInputData(t, 20, 4, 10)
	keys = keys[:2]

	var i int
	roots := make([][]byte, 0, 10)
	var pruneFrom uint64 = 5

	blockNum := uint64(0)
	for i = 0; i < len(vals); i++ {
		txNum := uint64(i)
		if i == 3 {
			domains.SetChangesetAccumulator(changesetAt3)
		}
		if i == 5 {
			domains.SetChangesetAccumulator(changesetAt5)
		}

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			require.NoError(t, err)
		}
		rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		roots = append(roots, rh)
	}

	err = domains.Flush(t.Context(), rwTx)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	diffs := [kv.DomainLen][]kv.DomainEntryDiff{}
	for idx := range changesetAt5.Diffs {
		diffs[idx] = changesetAt5.Diffs[idx].GetDiffSet()
	}
	err = rwTx.Unwind(ctx, pruneFrom, &diffs)
	//err = domains.Unwind(t.Context(), rwTx, 0, pruneFrom, &diffs)
	require.NoError(t, err)

	domains.SetChangesetAccumulator(changesetAt3)
	for i = int(pruneFrom); i < len(vals); i++ {
		txNum := uint64(i)

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, _, err := rwTx.GetLatest(kv.AccountsDomain, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.Equal(t, roots[i], rh)
	}

	err = domains.Flush(t.Context(), rwTx)
	require.NoError(t, err)

	pruneFrom = 3

	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	for idx := range changesetAt3.Diffs {
		diffs[idx] = changesetAt3.Diffs[idx].GetDiffSet()
	}
	err = rwTx.Unwind(t.Context(), pruneFrom, &diffs)
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		txNum := uint64(i)

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    accounts.EmptyCodeHash,
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, _, err := rwTx.GetLatest(kv.AccountsDomain, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.Equal(t, roots[i], rh)
	}
}
