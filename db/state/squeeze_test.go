package state_test

import (
	"context"
	"encoding/binary"
	"math"
	randOld "math/rand"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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
	"github.com/erigontech/erigon/execution/commitment"
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
		n, err := rnd.Read(bk[:])
		require.Equal(tb, keySize, n)
		require.NoError(tb, err)
		keys[i] = common.Copy(bk[:n])

		n, err = rnd.Read(bv[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		values[i] = common.Copy(bv[:n])
	}
	return keys, values
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

	ctx := context.Background()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(tb, err)
	defer rwTx.Rollback()

	domains, err := state.NewSharedDomains(rwTx, log.New())
	require.NoError(tb, err)
	defer domains.Close()

	keys, vals := generateInputData(tb, length.Addr, 5, txCount)
	tb.Logf("keys %d vals %d\n", len(keys), len(vals))

	var txNum, blockNum uint64
	for i := 0; i < len(vals); i++ {
		txNum = uint64(i)
		domains.SetTxNum(txNum)

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := domains.GetLatest(kv.AccountsDomain, rwTx, keys[j])
			require.NoError(tb, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev, step)
			require.NoError(tb, err)
		}
		if uint64(i+1)%agg.StepSize() == 0 {
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(tb, err)
			require.NotEmpty(tb, rh)
		}
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(tb, err)
	domains.Close() // closes ac

	require.NoError(tb, rwTx.Commit())

	return db, agg
}

func TestAggregator_SqueezeCommitment(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	cfgd := &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfgd)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	var blockNum uint64
	// get latest commited root
	latestRoot, err := domains.ComputeCommitment(context.Background(), rwTx, false, blockNum, 0, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, latestRoot)
	domains.Close()

	// now do the squeeze
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)
	err = state.SqueezeCommitmentFiles(context.Background(), state.AggTx(rwTx), log.New())
	require.NoError(t, err)

	//agg.recalcVisibleFiles(matgh.MaxUint64)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = state.NewSharedDomains(rwTx, log.New())
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
	root, err := domains.ComputeCommitment(context.Background(), rwTx, false, blockNum, 0, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, root)
	require.Equal(t, latestRoot, root)
	require.NotEqual(t, empty.RootHash.Bytes(), root)
}

// by that key stored latest root hash and tree state
const keyCommitmentStateS = "state"

var KeyCommitmentState = []byte(keyCommitmentStateS)

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
		tx, err := db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		ac := state.AggTx(tx)

		// collect latest root from each available file
		stateVal, ok, _, _, _ := ac.DebugGetLatestFromFiles(kv.CommitmentDomain, KeyCommitmentState, math.MaxUint64)
		require.True(t, ok)
		rootInFiles, err = commitment.HexTrieExtractStateRoot(stateVal)
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
	rwTx, err := db.BeginRw(context.Background())
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

	ctx := context.Background()
	finalRoot, err := state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), true)
	require.NoError(t, err)
	require.NotEmpty(t, finalRoot)
	require.NotEqual(t, empty.RootHash.Bytes(), finalRoot)

	require.Equal(t, rootInFiles, finalRoot[:])
}

func composite(k, k2 []byte) []byte {
	return append(common.Copy(k), k2...)
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
	ctx := context.Background()
	logger := log.New()
	aggStep := rc.aggStep
	db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := state.NewSharedDomains(tx, log.New())
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
		domains.SetTxNum(txNum)
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
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf, txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, tx, composite(addr, loc), []byte{addr[0], loc[0]}, txNum, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.CommitmentDomain, tx, someKey, aux[:], txNum, nil, 0)
		require.NoError(t, err)
		maxWrite = txNum
	}
	_, err = domains.ComputeCommitment(ctx, tx, true, blockNum, txNum, "", nil)
	require.NoError(t, err)

	err = domains.Flush(context.Background(), tx)
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

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	//anotherAgg.SetTx(rwTx)
	startTx := anotherAgg.EndTxNumMinimax()
	dom2, err := state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer dom2.Close()

	err = dom2.SeekCommitment(ctx, rwTx)
	sstartTx := dom2.TxNum()

	require.NoError(t, err)
	require.GreaterOrEqual(t, sstartTx, startTx)
	require.GreaterOrEqual(t, sstartTx, latestCommitTxNum)
	_ = sstartTx
	rwTx.Rollback()

	// Check the history
	roTx, err := db.BeginTemporalRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	v, _, err := roTx.GetLatest(kv.CommitmentDomain, someKey)
	require.NoError(t, err)
	require.Equal(t, maxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregatorV3_SharedDomains(t *testing.T) {
	t.Parallel()
	db, _ := testDbAndAggregatorv3(t, 20)
	ctx := context.Background()

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := state.NewSharedDomains(rwTx, log.New())
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
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := domains.GetLatest(kv.AccountsDomain, rwTx, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev, step)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			require.NoError(t, err)
		}
		rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		roots = append(roots, rh)
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	diffs := [kv.DomainLen][]kv.DomainEntryDiff{}
	for idx := range changesetAt5.Diffs {
		diffs[idx] = changesetAt5.Diffs[idx].GetDiffSet()
	}
	err = rwTx.Unwind(ctx, pruneFrom, &diffs)
	//err = domains.Unwind(context.Background(), rwTx, 0, pruneFrom, &diffs)
	require.NoError(t, err)

	domains.SetChangesetAccumulator(changesetAt3)
	for i = int(pruneFrom); i < len(vals); i++ {
		txNum := uint64(i)

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := rwTx.GetLatest(kv.AccountsDomain, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.Equal(t, roots[i], rh)
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)

	pruneFrom = 3

	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	for idx := range changesetAt3.Diffs {
		diffs[idx] = changesetAt3.Diffs[idx].GetDiffSet()
	}
	err = rwTx.Unwind(context.Background(), pruneFrom, &diffs)
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		txNum := uint64(i)

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := rwTx.GetLatest(kv.AccountsDomain, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, txNum, prev, step)
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
