package state

import (
	"context"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
)

type testAggConfig struct {
	stepSize                         uint64
	disableCommitmentBranchTransform bool
}

func testDbAggregatorWithFiles(tb testing.TB, cfg *testAggConfig) (kv.RwDB, *Aggregator) {
	tb.Helper()
	txCount := int(cfg.stepSize) * 32 // will produce files up to step 31, good because covers different ranges (16, 8, 4, 2, 1)
	db, agg := testDbAggregatorWithNoFiles(tb, txCount, cfg)

	// build files out of db
	err := agg.BuildFiles(uint64(txCount))
	require.NoError(tb, err)
	return db, agg
}

func testDbAggregatorWithNoFiles(tb testing.TB, txCount int, cfg *testAggConfig) (kv.RwDB, *Aggregator) {
	tb.Helper()
	_db, agg := testDbAndAggregatorv3(tb, cfg.stepSize)
	db := wrapDbWithCtx(_db, agg)

	agg.d[kv.CommitmentDomain].ReplaceKeysInValues = !cfg.disableCommitmentBranchTransform

	ctx := context.Background()
	agg.logger = log.Root().New()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(tb, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(tb, err)
	defer domains.Close()

	keys, vals := generateInputData(tb, length.Addr, 5, txCount)
	tb.Logf("keys %d vals %d\n", len(keys), len(vals))

	var txNum, blockNum uint64
	for i := 0; i < len(vals); i++ {
		txNum = uint64(i)
		domains.SetTxNum(txNum)

		for j := 0; j < len(keys); j++ {
			acc := accounts3.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts3.SerialiseV3(&acc)
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

	domains, err := NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	var blockNum uint64
	// get latest commited root
	latestRoot, err := domains.ComputeCommitment(context.Background(), false, blockNum, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, latestRoot)
	domains.Close()

	// now do the squeeze
	agg.d[kv.CommitmentDomain].ReplaceKeysInValues = true
	err = SqueezeCommitmentFiles(context.Background(), AggTx(rwTx), log.New())
	require.NoError(t, err)

	agg.recalcVisibleFiles(math.MaxUint64)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err = NewSharedDomains(rwTx, log.New())
	require.NoError(t, err)

	// collect account keys to trigger commitment
	acit, err := rwTx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, -1)
	require.NoError(t, err)
	defer acit.Close()

	require.NoError(t, err)
	for acit.HasNext() {
		k, _, err := acit.Next()
		require.NoError(t, err)
		domains.sdCtx.updates.TouchPlainKey(string(k), nil, domains.sdCtx.updates.TouchAccount)
	}

	// check if the commitment is the same
	root, err := domains.ComputeCommitment(context.Background(), false, blockNum, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, root)
	require.Equal(t, latestRoot, root)
	require.NotEqual(t, empty.RootHash.Bytes(), root)
}
