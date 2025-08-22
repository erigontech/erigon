package state_test

import (
	"context"
	"math"
	randOld "math/rand"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type testAggConfig struct {
	stepSize                         uint64
	disableCommitmentBranchTransform bool
}

func testDbAggregatorWithFiles(tb testing.TB, cfg *testAggConfig) (kv.RwDB, *state.Aggregator) {
	tb.Helper()
	txCount := int(cfg.stepSize) * 32 // will produce files up to step 31, good because covers different ranges (16, 8, 4, 2, 1)
	db, agg := testDbAggregatorWithNoFiles(tb, txCount, cfg)

	// build files out of db
	err := agg.BuildFiles(uint64(txCount))
	require.NoError(tb, err)
	return db, agg
}

// wrapDbWithCtx - deprecated copy of kv_temporal.go - visible only in tests
// need to move non-unit-tests to own package
func wrapDbWithCtx(db kv.RwDB, ctx *state.Aggregator) kv.TemporalRwDB {
	v, err := New(db, ctx)
	if err != nil {
		panic(err)
	}
	return v
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

func testDbAndAggregatorv3(tb testing.TB, aggStep uint64) (kv.RwDB, *state.Aggregator) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	agg := testAgg(tb, db, dirs, aggStep, logger)
	err := agg.OpenFolder()
	require.NoError(tb, err)
	return db, agg
}

func testAgg(tb testing.TB, db kv.RwDB, dirs datadir.Dirs, aggStep uint64, logger log.Logger) *state.Aggregator {
	tb.Helper()

	salt, err := state.GetStateIndicesSalt(dirs, true, logger)
	require.NoError(tb, err)
	agg, err := state.NewAggregator2(context.Background(), dirs, aggStep, salt, db, logger)
	require.NoError(tb, err)
	tb.Cleanup(agg.Close)
	agg.DisableFsync()
	return agg
}

func testDbAggregatorWithNoFiles(tb testing.TB, txCount int, cfg *testAggConfig) (kv.RwDB, *state.Aggregator) {
	tb.Helper()
	_db, agg := testDbAndAggregatorv3(tb, cfg.stepSize)
	db := wrapDbWithCtx(_db, agg)

	agg.ForceCommitmentValTransformForTests(true)

	ctx := context.Background()
	//agg.logger = log.Root().New()

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
			rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
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
	_db, agg := testDbAggregatorWithFiles(t, cfgd)
	db := wrapDbWithCtx(_db, agg)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := state.NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	var blockNum uint64
	// get latest commited root
	latestRoot, err := domains.ComputeCommitment(context.Background(), false, blockNum, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, latestRoot)
	domains.Close()

	// now do the squeeze
	agg.ForceCommitmentValTransformForTests(true)
	err = state.SqueezeCommitmentFiles(context.Background(), state.AggTx(rwTx), log.New())
	require.NoError(t, err)

	//agg.recalcVisibleFiles(math.MaxUint64)
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

	trieCtx := domains.TrieCtxForTests()
	require.NoError(t, err)
	for acit.HasNext() {
		k, _, err := acit.Next()
		require.NoError(t, err)
		trieCtx.TouchKey(kv.AccountsDomain, string(k), nil)
	}

	// check if the commitment is the same
	root, err := domains.ComputeCommitment(context.Background(), false, blockNum, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, root)
	require.Equal(t, latestRoot, root)
	require.NotEqual(t, empty.RootHash.Bytes(), root)
}

// by that key stored latest root hash and tree state
const keyCommitmentStateS = "state"

var keyCommitmentState = []byte(keyCommitmentStateS)

func TestAggregator_RebuildCommitmentBasedOnFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_db, agg := testDbAggregatorWithFiles(t, &testAggConfig{
		stepSize:                         10,
		disableCommitmentBranchTransform: false,
	})

	var rootInFiles []byte
	var fPaths []string

	{
		db := wrapDbWithCtx(_db, agg)

		tx, err := db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		ac := state.AggTx(tx)

		// collect latest root from each available file
		stateVal, ok, _, _, _ := ac.DebugGetLatestFromFiles(kv.CommitmentDomain, keyCommitmentState, math.MaxUint64)
		require.True(t, ok)
		rootInFiles, err = commitment.HexTrieExtractStateRoot(stateVal)
		require.NoError(t, err)

		for _, f := range ac.Files(kv.CommitmentDomain) {
			fPaths = append(fPaths, f.Fullpath())
		}
		tx.Rollback()
		agg.Close()
	}

	agg = testAgg(t, _db, agg.Dirs(), agg.StepSize(), log.New())
	db := wrapDbWithCtx(_db, agg)

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
