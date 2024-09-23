// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
)

func TestRebuildPatriciaTrieBasedOnFiles(t *testing.T) {
	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	db, agg := temporaltest.NewTestDB(t, dirs)
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
	blocksTotal := agg.StepSize() + 1
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

	err = agg.BuildFiles(blocksTotal)
	require.NoError(t, err)

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
	require.NoError(t, tx.Commit())
	for _, fn := range agg.Files() {
		if strings.Contains(fn, "v1-commitment") {
			fn = filepath.Join(dirs.SnapDomain, fn)
			require.NoError(t, os.Remove(fn))

			t.Logf("removed file %s", fn)
		}

	}
	agg.OpenFolder()
	//agg.BeginFilesRo().

	// checkRoot is false since we do not pass blockReader and want to check root manually afterwards.
	historyV3 := true
	cfg := StageTrieCfg(db, false /* checkRoot */, true /* saveHashesToDb */, false /* badBlockHalt */, dirs.Tmp, nil, nil /* hd */, historyV3, agg)

	rebuiltRoot, err := RebuildPatriciaTrieBasedOnFiles(cfg, context.Background(), log.New())
	require.NoError(t, err)

	require.EqualValues(t, expectedRoot, rebuiltRoot)
	t.Logf("rebuilt commitment %q", rebuiltRoot)
}
