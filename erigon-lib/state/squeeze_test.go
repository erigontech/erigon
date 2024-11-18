package state

import (
	"context"
	"math"
	"testing"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

type testAggConfig struct {
	stepSize                         uint64
	disableCommitmentBranchTransform bool
}

func testDbAggregatorWithFiles(tb testing.TB, cfg *testAggConfig) (kv.RwDB, *Aggregator) {
	tb.Helper()

	db, agg := testDbAndAggregatorv3(tb.(*testing.T), cfg.stepSize)
	agg.commitmentValuesTransform = !cfg.disableCommitmentBranchTransform
	agg.d[kv.CommitmentDomain].replaceKeysInValues = agg.commitmentValuesTransform

	ctx := context.Background()
	agg.logger = log.Root().New()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(tb, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(tb, err)
	defer domains.Close()

	txCount := int(cfg.stepSize) * 32 // will produce files up to step 31, good because covers different ranges (16, 8, 4, 2, 1)

	keys, vals := generateInputData(tb, length.Addr, 16, txCount)
	tb.Logf("keys %d vals %d\n", len(keys), len(vals))

	for i := 0; i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			buf := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)
			prev, step, err := domains.GetLatest(kv.AccountsDomain, keys[j], nil)
			require.NoError(tb, err)

			err = domains.DomainPut(kv.AccountsDomain, keys[j], nil, buf, prev, step)
			require.NoError(tb, err)
		}
		if uint64(i+1)%agg.StepSize() == 0 {
			rh, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
			require.NoError(tb, err)
			require.NotEmpty(tb, rh)
		}
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(tb, err)
	domains.Close() // closes ac

	require.NoError(tb, rwTx.Commit())

	// build files out of db
	err = agg.BuildFiles(uint64(txCount))
	require.NoError(tb, err)
	return db, agg
}

func TestAggregator_SqueezeCommitment(t *testing.T) {

	cfgd := &testAggConfig{stepSize: 32, disableCommitmentBranchTransform: true}
	db, agg := testDbAggregatorWithFiles(t, cfgd)
	defer db.Close()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	// get latest commited root
	latestRoot, err := domains.ComputeCommitment(context.Background(), false, domains.BlockNum(), "")
	require.NoError(t, err)
	require.NotEmpty(t, latestRoot)
	domains.Close()

	// now do the squeeze
	agg.commitmentValuesTransform = true
	agg.d[kv.CommitmentDomain].replaceKeysInValues = true
	err = ac.SqueezeCommitmentFiles()
	require.NoError(t, err)
	ac.Close()
	agg.recalcVisibleFiles(math.MaxUint64)

	// check now
	ac = agg.BeginFilesRo()
	defer ac.Close()

	domains, err = NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)

	// collect account keys to trigger commitment
	acit, err := ac.DomainRangeLatest(rwTx, kv.AccountsDomain, nil, nil, -1)
	require.NoError(t, err)
	defer acit.Close()

	require.NoError(t, err)
	for acit.HasNext() {
		k, _, err := acit.Next()
		require.NoError(t, err)
		domains.sdCtx.updates.TouchPlainKey(k, nil, domains.sdCtx.updates.TouchAccount)
	}

	// check if the commitment is the same
	root, err := domains.ComputeCommitment(context.Background(), false, domains.BlockNum(), "")
	require.NoError(t, err)
	require.NotEmpty(t, root)
	require.EqualValues(t, latestRoot, root)
	require.NotEqualValues(t, commitment.EmptyRootHash, root)
}
