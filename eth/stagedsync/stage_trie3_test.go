package stagedsync

import (
	"context"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/temporal/temporaltest"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func TestRebuildPatriciaTrieBasedOnFiles(t *testing.T) {
	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	v3, db, agg := temporaltest.NewTestDB(t, dirs)
	if !v3 {
		t.Skip("this test is v3 only")
	}
	logger := log.New()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
			tx = nil
		}
		if db != nil {
			db.Close()
		}
		if agg != nil {
			agg.Close()
		}
	}()

	before, after, writer := apply(tx, logger)
	blocksTotal := uint64(100_000)
	generateBlocks2(t, 1, blocksTotal, writer, before, after, staticCodeStaticIncarnations)

	err = stages.SaveStageProgress(tx, stages.Execution, blocksTotal)
	require.NoError(t, err)

	for i := uint64(0); i <= blocksTotal; i++ {
		err = rawdbv3.TxNums.Append(tx, i, i)
		require.NoError(t, err)
	}

	domains, err := state.NewSharedDomains(tx, logger)
	require.NoError(t, err)
	defer domains.Close()
	domains.SetBlockNum(blocksTotal)
	domains.SetTxNum(blocksTotal - 1) // generated 1tx per block

	expectedRoot, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
	require.NoError(t, err)
	t.Logf("expected root is %x", expectedRoot)

	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)

	domains.Close()

	require.NoError(t, tx.Commit())
	tx = nil

	// start another tx
	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	buckets, err := tx.ListBuckets()
	require.NoError(t, err)
	for i, b := range buckets {
		if strings.Contains(strings.ToLower(b), "commitment") {
			size, err := tx.BucketSize(b)
			require.NoError(t, err)
			t.Logf("cleaned table #%d %s: %d keys", i, b, size)

			err = tx.ClearBucket(b)
			require.NoError(t, err)
		}
	}

	// checkRoot is false since we do not pass blockReader and want to check root manually afterwards.
	cfg := StageTrieCfg(db, false /* checkRoot */, true /* saveHashesToDb */, false /* badBlockHalt */, dirs.Tmp, nil, nil /* hd */, v3, agg)

	rebuiltRoot, err := RebuildPatriciaTrieBasedOnFiles(tx, cfg, context.Background(), log.New())
	require.NoError(t, err)

	require.EqualValues(t, expectedRoot, rebuiltRoot)
	t.Logf("rebuilt commitment %q", rebuiltRoot)
}
