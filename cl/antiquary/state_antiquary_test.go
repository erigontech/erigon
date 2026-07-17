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

package antiquary

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func runTest(t *testing.T, blocks []*cltypes.SignedBeaconBlock, preState, postState *state.CachingBeaconState) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	reader := tests.LoadChain(blocks, postState, db, t)
	sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	sn.OnHeadState(postState)
	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, nil, nil, reader, sn, log.New(), true, true, true, false, nil)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
}

func TestStateAntiquaryElectra(t *testing.T) {
	blocks, preState, postState := tests.GetElectraRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryCapella(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryBellatrix(t *testing.T) {
	blocks, preState, postState := tests.GetBellatrixRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryPhase0(t *testing.T) {
	blocks, preState, postState := tests.GetPhase0Random()
	runTest(t, blocks, preState, postState)
}

type commitCountingDB struct {
	kv.RwDB
	commits *atomic.Int64
}

func (d *commitCountingDB) BeginRw(ctx context.Context) (kv.RwTx, error) {
	tx, err := d.RwDB.BeginRw(ctx) //nolint:gocritic // wrapper returns the tx to the caller; it owns Rollback
	if err != nil {
		return nil, err
	}
	return &commitCountingTx{RwTx: tx, commits: d.commits}, nil
}

type commitCountingTx struct {
	kv.RwTx
	commits *atomic.Int64
}

func (t *commitCountingTx) Commit() error {
	t.commits.Add(1)
	return t.RwTx.Commit()
}

// A single antiquary run must reconstruct the same slots whether committed in
// one big transaction or in maxSlotsPerCommit-bounded batches, and the bounded
// run must flush more often — bounding the per-commit retired-page list is what
// keeps libmdbx's gc_fill_returned from overflowing on large DBs.
func TestStateAntiquaryCommitIsBounded(t *testing.T) {
	blocks, preState, postState := tests.GetCapellaRandom()
	to := blocks[len(blocks)-1].Block.Slot + 33

	run := func(maxSlotsPerCommit uint64) (commits int64, progress uint64) {
		db := memdb.NewTestDB(t, dbcfg.ChainDB)
		reader := tests.LoadChain(blocks, postState, db, t)
		sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
		sn.OnHeadState(postState)
		ctx := context.Background()
		vt := state_accessors.NewStaticValidatorTable()
		counter := &atomic.Int64{}
		countingDB := &commitCountingDB{RwDB: db, commits: counter}
		a := NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, countingDB, nil, nil, reader, sn, log.New(), true, true, true, false, nil)
		a.maxSlotsPerCommit = maxSlotsPerCommit
		require.NoError(t, a.IncrementBeaconState(ctx, to))
		require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
			var e error
			progress, e = state_accessors.GetStateProcessingProgress(tx)
			return e
		}))
		return counter.Load(), progress
	}

	singleCommits, singleProgress := run(1 << 62)
	boundedCommits, boundedProgress := run(8)

	require.Equal(t, singleProgress, boundedProgress)
	require.Greater(t, boundedCommits, singleCommits)
	require.GreaterOrEqual(t, boundedCommits, singleCommits+2)
}
