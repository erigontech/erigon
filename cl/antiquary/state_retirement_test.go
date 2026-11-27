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

package antiquary

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestCaplinStateRetireRangeKeepsSafetyMargin(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	stateSnapshots := snapshotsync.NewCaplinStateSnapshots(
		ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dirs,
		snapshotsync.SnapshotTypes{KeyValueGetters: map[string]snapshotsync.KeyValueGetter{kv.PendingDepositsDump: nil}},
		log.New(),
	)
	t.Cleanup(stateSnapshots.Close)
	require.NoError(t, stateSnapshots.OpenFolder())

	from, to := CaplinStateRetireRange(stateSnapshots, 120_000)
	require.Equal(t, uint64(1), from)
	require.Equal(t, uint64(50_000), to)
	from, to = CaplinStateRetireRange(stateSnapshots, 150_000)
	require.Equal(t, uint64(1), from)
	require.Equal(t, uint64(50_000), to)
}

func TestRetireCaplinStateCapsPruneAtProgress(t *testing.T) {
	ctx := context.Background()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	types := snapshotsync.SnapshotTypes{KeyValueGetters: map[string]snapshotsync.KeyValueGetter{
		kv.PendingDepositsDump: func(slot uint64) ([]byte, []byte, error) {
			return base_encoding.Encode64ToBytes4(slot), []byte{1}, nil
		},
	}}
	stateSnapshots := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs, types, log.New())
	t.Cleanup(stateSnapshots.Close)
	require.NoError(t, stateSnapshots.DumpCaplinState(ctx, 50_000, 50_000, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, stateSnapshots.OpenFolder())
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for _, slot := range []uint64{10_000, 40_000} {
			if err := tx.Put(kv.PendingDepositsDump, base_encoding.Encode64ToBytes4(slot), []byte{1}); err != nil {
				return err
			}
		}
		return nil
	}))

	_, err := RetireCaplinState(ctx, db, stateSnapshots, 25_000, 0, dirs, 1, log.New(), StateRetireHooks{})
	require.NoError(t, err)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		below, err := tx.GetOne(kv.PendingDepositsDump, base_encoding.Encode64ToBytes4(10_000))
		if err != nil {
			return err
		}
		require.Empty(t, below)
		at, err := tx.GetOne(kv.PendingDepositsDump, base_encoding.Encode64ToBytes4(40_000))
		if err != nil {
			return err
		}
		require.NotEmpty(t, at)
		return nil
	}))
}

func TestNodeStateDumpBelowMarginKeepsRows(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.PendingDepositsDump, []uint64{10})
	cfg := &clparams.MainnetBeaconConfig
	types := snapshotsync.MakeCaplinStateSnapshotsTypes(db, cfg)
	types.KeyValueGetters = map[string]snapshotsync.KeyValueGetter{kv.PendingDepositsDump: types.KeyValueGetters[kv.PendingDepositsDump]}
	stateSn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs, types, log.New())
	defer stateSn.Close()
	require.NoError(t, stateSn.DumpCaplinState(t.Context(), 1_000, 1_000, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, stateSn.OpenFolder())
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, cfg, dirs, log.New())
	defer sn.Close()
	a := NewAntiquary(t.Context(), nil, nil, nil, cfg, dirs, nil, db, stateSn, sn, nil, nil, log.New(), true, true, false, true, nil)
	a.currentState = state.New(cfg)
	require.NoError(t, a.currentState.SetSlot(69_999))
	to, err := a.dumpCaplinStateIfDue(t.Context())
	require.NoError(t, err)
	require.Zero(t, to)
	require.Equal(t, []uint64{10}, tableSlots(t, db, kv.PendingDepositsDump), "no dump is due, so the node must not start another prune pass")
}

func TestRetireStateRetriesOverlapWithoutDump(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	seedStateSlots(t, db, kv.StateEvents, []uint64{10})
	types := snapshotsync.MakeCaplinStateSnapshotsTypes(db, &clparams.MainnetBeaconConfig)
	types.KeyValueGetters = map[string]snapshotsync.KeyValueGetter{
		kv.PendingDepositsDump: types.KeyValueGetters[kv.PendingDepositsDump],
		kv.StateEvents:         types.KeyValueGetters[kv.StateEvents],
	}
	writeStateFixture(t, dirs.SnapCaplin, kv.PendingDepositsDump, 0, 50_000, log.New())
	sub := writeStateFixture(t, dirs.SnapCaplin, kv.PendingDepositsDump, 20_000, 30_000, log.New())
	sn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs, types, log.New())
	defer sn.Close()
	require.NoError(t, sn.OpenFolder())
	fault := errors.New("stop after prune")
	hooks := StateRetireHooks{Prune: func(ctx context.Context, progress uint64) error {
		_, err := pruneCaplinStateTablesChanged(ctx, db, sn, progress, log.New())
		require.NoError(t, err)
		return fault
	}}
	_, err := RetireCaplinState(t.Context(), db, sn, 120_000, 0, dirs, 1, log.New(), hooks)
	require.ErrorIs(t, err, fault)
	require.Empty(t, tableSlots(t, db, kv.StateEvents))
	value, err := sn.Get(kv.StateEvents, 10)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, value)
	require.NoError(t, sn.OpenFolder())
	_, err = RetireCaplinState(t.Context(), db, sn, 120_000, 0, dirs, 1, log.New(), StateRetireHooks{})
	require.NoError(t, err)
	_, err = os.Stat(sub)
	require.ErrorIs(t, err, os.ErrNotExist, "retire left the covered state segment on disk")
}
