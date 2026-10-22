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
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func newBuildersList(builders ...cltypes.Builder) *solid.ListSSZ[*cltypes.Builder] {
	l := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	for i := range builders {
		l.Append(&builders[i])
	}
	return l
}

func TestBuilderPendingWithdrawalsDiffKeepsDuplicateAppend(t *testing.T) {
	collector := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer collector.close()

	w := cltypes.BuilderPendingWithdrawal{Amount: 7, BuilderIndex: 1}
	newList := func(n int) *solid.ListSSZ[*cltypes.BuilderPendingWithdrawal] {
		l := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](1024, new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ())
		for range n {
			c := w
			l.Append(&c)
		}
		return l
	}
	pre, post := newList(1), newList(2)

	collector.builderPendingWithdrawalsWriter.Initialize(pre)
	var diff bytes.Buffer
	require.NoError(t, collector.builderPendingWithdrawalsWriter.WriteDiff(&diff, post))

	reconstructed := newList(1)
	require.NoError(t, base_encoding.ApplySSZQueueDiff(&diff, reconstructed, 0))
	require.Equal(t, post.Len(), reconstructed.Len())
}

func TestBeaconStatesCollectorBuildersRoundTrip(t *testing.T) {
	collector := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer collector.close()

	dumpSlot := uint64(clparams.SlotsPerDump)
	initial := newBuildersList(
		cltypes.Builder{Balance: 100},
		cltypes.Builder{Balance: 200},
	)
	changed := newBuildersList(
		cltypes.Builder{Balance: 101},
		cltypes.Builder{Balance: 200},
	)
	appended := newBuildersList(
		cltypes.Builder{Balance: 101},
		cltypes.Builder{Balance: 200},
		cltypes.Builder{Balance: 300},
	)
	emptyWithdrawals := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](1, new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ())
	emptyPayloadWithdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](1, new(cltypes.Withdrawal).EncodingSizeSSZ())

	require.NoError(t, collector.collectBuildersDump(dumpSlot, initial))
	collector.buildersWriter.Initialize(initial)
	require.NoError(t, collector.collectGloasQueuesDiffs(dumpSlot+1, changed, emptyWithdrawals, emptyPayloadWithdrawals))
	require.NoError(t, collector.slotDataCollector.Collect(base_encoding.Encode64ToBytes4(dumpSlot+1), []byte{1}))
	collector.buildersWriter.Initialize(changed)
	require.NoError(t, collector.collectGloasQueuesDiffs(dumpSlot+2, appended, emptyWithdrawals, emptyPayloadWithdrawals))
	require.NoError(t, collector.slotDataCollector.Collect(base_encoding.Encode64ToBytes4(dumpSlot+2), []byte{1}))

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, collector.flush(context.Background(), tx))
	require.NoError(t, tx.Commit())

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()
	got := newBuildersList()
	getter := state_accessors.GetValFnTxAndSnapshot(roTx, nil)
	require.NoError(t, historical_states_reader.ReadBuildersSSZ(getter, dumpSlot+2, 0, clparams.MainnetBeaconConfig.BuilderRegistryLimit, got))
	require.Equal(t, appended.Len(), got.Len())
	for i := 0; i < appended.Len(); i++ {
		require.Equal(t, *appended.Get(i), *got.Get(i))
	}

	row, err := roTx.GetOne(kv.Builders, base_encoding.Encode64ToBytes4(dumpSlot+1))
	require.NoError(t, err)
	require.Len(t, row, 26+101)
}

func TestBeaconStatesCollectorBuildersAcrossGloasFork(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	collector := newBeaconStatesCollector(cfg, t.TempDir(), log.New())
	defer collector.close()

	forkSlot := uint64(clparams.SlotsPerDump)
	fuluState := state.New(cfg)
	fuluState.SetVersion(clparams.FuluVersion)
	gloasState := state.New(cfg)
	gloasState.SetVersion(clparams.GloasVersion)
	gloasState.SetBuilders(newBuildersList(
		cltypes.Builder{Balance: 100},
		cltypes.Builder{Balance: 200},
		cltypes.Builder{Balance: 300},
	))
	collectBlock := func(preState *state.CachingBeaconState, slot uint64, transition func()) {
		collector.preStateTransitionHook(preState)
		transition()
		require.NoError(t, collector.collectGloasQueuesDiffs(slot, gloasState.GetBuilders(), gloasState.GetBuilderPendingWithdrawals(), gloasState.GetPayloadExpectedWithdrawals()))
		require.NoError(t, collector.slotDataCollector.Collect(base_encoding.Encode64ToBytes4(slot), []byte{1}))
	}

	collectBlock(fuluState, forkSlot, func() {})
	afterFork := []uint64{100, 200, 300}
	collectBlock(gloasState, forkSlot+1, func() { gloasState.GetBuilders().Get(1).Balance = 250 })
	afterChange := []uint64{100, 250, 300}

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, collector.flush(context.Background(), tx))
	require.NoError(t, tx.Commit())

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()
	getter := state_accessors.GetValFnTxAndSnapshot(roTx, nil)
	for slot, want := range map[uint64][]uint64{forkSlot: afterFork, forkSlot + 1: afterChange} {
		got := newBuildersList()
		require.NoError(t, historical_states_reader.ReadBuildersSSZ(getter, slot, forkSlot, cfg.BuilderRegistryLimit, got))
		balances := make([]uint64, got.Len())
		for i := range balances {
			balances[i] = got.Get(i).Balance
		}
		require.Equal(t, want, balances, "slot %d", slot)
	}

	changeRow, err := roTx.GetOne(kv.Builders, base_encoding.Encode64ToBytes4(forkSlot+1))
	require.NoError(t, err)
	require.Len(t, changeRow, 26+101)
}

func TestBeaconStatesCollectorGloasGenesisBuilders(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	for name, genesisSlot := range map[string]uint64{"dump slot": 0, "unaligned slot": 32} {
		t.Run(name, func(t *testing.T) {
			collector := newBeaconStatesCollector(cfg, t.TempDir(), log.New())
			defer collector.close()

			genesis := state.New(cfg)
			genesis.SetVersion(clparams.GloasVersion)
			require.NoError(t, genesis.SetSlot(genesisSlot))
			genesis.SetBuilders(newBuildersList(cltypes.Builder{Balance: 100}, cltypes.Builder{Balance: 200}))
			require.NoError(t, collector.addGenesisState(genesis))

			collector.preStateTransitionHook(genesis)
			genesis.GetBuilders().Get(1).Balance = 250
			require.NoError(t, collector.collectGloasQueuesDiffs(genesisSlot+1, genesis.GetBuilders(), genesis.GetBuilderPendingWithdrawals(), genesis.GetPayloadExpectedWithdrawals()))
			require.NoError(t, collector.slotDataCollector.Collect(base_encoding.Encode64ToBytes4(genesisSlot+1), []byte{1}))

			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, collector.flush(context.Background(), tx))

			getter := state_accessors.GetValFnTxAndSnapshot(tx, nil)
			for slot, want := range map[uint64][]uint64{genesisSlot: {100, 200}, genesisSlot + 1: {100, 250}} {
				got := newBuildersList()
				require.NoError(t, historical_states_reader.ReadBuildersSSZ(getter, slot, 0, cfg.BuilderRegistryLimit, got))
				balances := make([]uint64, got.Len())
				for i := range balances {
					balances[i] = got.Get(i).Balance
				}
				require.Equal(t, want, balances, "slot %d", slot)
			}

			genesisRow, err := tx.GetOne(kv.Builders, base_encoding.Encode64ToBytes4(genesisSlot))
			require.NoError(t, err)
			require.Len(t, genesisRow, 26)
		})
	}
}
