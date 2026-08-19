// Copyright 2026 The Erigon Authors
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

package state_test

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// A range wider than one shard is rebuilt in several shards, each made visible
// before the next starts, with commitment merges running between them. The root
// must come out where the forward run left it regardless of that reshuffling.
func TestAggregator_RebuildCommitmentAcrossMergedShards(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}

	// 257 steps collate into a 256-step file: four shards, so a merged file lands
	// while shards are still being built and later ones read it. Keys must outnumber
	// steps or the shard loop divides by a zero keys-per-step. Branch transform off:
	// referencing pins the accounts, storage and commitment ranges together, and a
	// mid-range commitment merge stands down.
	const stepSize = uint64(2)
	const txCount = int(stepSize) * 257

	db, agg := testDbAggregatorWithNoFiles(t, txCount, &testAggConfig{
		stepSize:                         stepSize,
		disableCommitmentBranchTransform: true,
	})
	require.NoError(t, agg.BuildFiles(uint64(txCount)))

	var rootInFiles []byte
	var fPaths []string
	{
		tx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		ac := state.AggTx(tx)

		stateVal, ok, _, _, _ := ac.DebugGetLatestFromFiles(kv.CommitmentDomain, commitmentdb.KeyCommitmentState, math.MaxUint64)
		require.True(t, ok)
		rootInFiles, _, _, err = commitment.HexTrieExtractStateRoot(stateVal)
		require.NoError(t, err)

		var widest uint64
		for _, f := range ac.Files(kv.AccountsDomain) {
			if steps := (f.EndRootNum() - f.StartRootNum()) / stepSize; steps > widest {
				widest = steps
			}
		}
		require.Greaterf(t, widest, uint64(commitment.DefaultRebuildShardMaxSteps),
			"the widest range must exceed one shard, or the rebuild never shards and this proves nothing")

		for _, f := range ac.Files(kv.CommitmentDomain) {
			fPaths = append(fPaths, f.Fullpath())
		}
		tx.Rollback()
		agg.Close()
	}

	agg = testAgg(t, db, agg.Dirs(), stepSize, log.New())
	db, err := temporal.New(db, agg, nil)
	require.NoError(t, err)
	defer db.Close()

	rwTx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	buckets, err := rwTx.ListTables()
	require.NoError(t, err)
	for _, b := range buckets {
		if strings.Contains(strings.ToLower(b), kv.CommitmentDomain.String()) {
			require.NoError(t, rwTx.ClearTable(b))
		}
	}
	require.NoError(t, rwTx.Commit())

	for _, fn := range fPaths {
		if strings.Contains(fn, kv.CommitmentDomain.String()) {
			require.NoError(t, dir.RemoveFile(fn))
		}
	}
	require.NoError(t, agg.OpenFolder())

	finalRoot, _, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false, state.DefaultRebuildTarget())
	require.NoError(t, err)
	require.NotEmpty(t, finalRoot)
	require.NotEqual(t, empty.RootHash[:], finalRoot)
	require.Equal(t, rootInFiles, finalRoot)
}
