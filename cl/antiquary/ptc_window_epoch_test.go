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
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/node/ethconfig"
)

func expectedPtcWindowEpochs(t *testing.T, fixture tests.GloasFixture) map[uint64][]byte {
	t.Helper()
	replay, err := fixture.PreState.Copy()
	require.NoError(t, err)
	windows := make(map[uint64][]byte)
	capture := func(st *state.CachingBeaconState, slot uint64) error {
		encoded, err := st.GetPtcWindow().EncodeSSZ(nil)
		if err == nil {
			windows[fixture.Config.RoundSlotToEpoch(slot)] = encoded
		}
		return err
	}
	replay.SetEvents(raw.Events{OnEpochBoundary: func(epoch uint64) error {
		if replay.Version() < clparams.GloasVersion {
			return nil
		}
		return capture(replay, epoch*fixture.Config.SlotsPerEpoch)
	}})
	if replay.Version() >= clparams.GloasVersion {
		require.NoError(t, capture(replay, replay.Slot()))
	}
	forkSlot := fixture.Config.GloasForkEpoch * fixture.Config.SlotsPerEpoch
	for _, block := range fixture.Blocks {
		if replay.Version() < clparams.GloasVersion && replay.Slot() < forkSlot && block.Block.Slot > forkSlot {
			forkState, err := replay.Copy()
			require.NoError(t, err)
			require.NoError(t, transition.DefaultMachine.ProcessSlots(forkState, forkSlot))
			require.NoError(t, capture(forkState, forkSlot))
		}
		require.NoError(t, transition.TransitionState(replay, block, &eth2.BlockRewardsCollector{}, false))
		if replay.Version() >= clparams.GloasVersion {
			require.NoError(t, capture(replay, replay.Slot()))
		}
	}
	return windows
}

type gloasAntiquaryEnv struct {
	db      kv.RwDB
	reader  *tests.MockBlockReader
	synced  synced_data.SyncedData
	genesis *state.CachingBeaconState
	cfg     *clparams.BeaconChainConfig
	dirs    datadir.Dirs
}

func newGloasAntiquaryEnv(t *testing.T, fixture tests.GloasFixture) gloasAntiquaryEnv {
	t.Helper()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	reader := tests.LoadChain(fixture.Blocks, db, t)
	synced := synced_data.NewSyncedDataManager(fixture.Config, true)
	require.NoError(t, synced.OnHeadState(fixture.PostState))
	preState, err := fixture.PreState.Copy()
	require.NoError(t, err)
	return gloasAntiquaryEnv{db: db, reader: reader, synced: synced, genesis: preState, cfg: fixture.Config, dirs: datadir.New(t.TempDir())}
}

func (env gloasAntiquaryEnv) newAntiquary(stateSn *snapshotsync.CaplinStateSnapshots) *Antiquary {
	return NewAntiquary(context.Background(), nil, env.genesis, state_accessors.NewStaticValidatorTable(), env.cfg,
		env.dirs, nil, env.db, stateSn, nil, env.reader, env.synced, log.New(), true, true, true, false, nil)
}

func runGloasAntiquary(t *testing.T, fixture tests.GloasFixture) gloasAntiquaryEnv {
	t.Helper()
	env := newGloasAntiquaryEnv(t, fixture)
	a := env.newAntiquary(nil)
	require.NoError(t, a.IncrementBeaconState(context.Background(), fixture.Blocks[len(fixture.Blocks)-1].Block.Slot+1))
	return env
}

func clearPtcWindowEpochRows(t *testing.T, db kv.RwDB) {
	t.Helper()
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.ClearTable(kv.PtcWindowEpochTable)
	}))
}

func readGloasHistoricalState(t *testing.T, env gloasAntiquaryEnv, stateSn *snapshotsync.CaplinStateSnapshots, slot uint64) (*state.CachingBeaconState, error) {
	t.Helper()
	var got *state.CachingBeaconState
	err := env.db.View(context.Background(), func(tx kv.Tx) error {
		validators := state_accessors.NewStaticValidatorTable()
		if err := state_accessors.ReadValidatorsTable(tx, validators); err != nil {
			return err
		}
		reader := historical_states_reader.NewHistoricalStatesReader(env.cfg, env.reader, validators, env.genesis, stateSn, env.synced)
		var err error
		got, err = reader.ReadHistoricalState(context.Background(), tx, slot)
		return err
	})
	return got, err
}

func readPtcWindowEpochRows(t *testing.T, db kv.RoDB) map[uint64][]byte {
	t.Helper()
	rows := make(map[uint64][]byte)
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.PtcWindowEpochTable)
		if err != nil {
			return err
		}
		defer cursor.Close()
		for key, compressed, err := cursor.First(); key != nil; key, compressed, err = cursor.Next() {
			if err != nil {
				return err
			}
			slot := base_encoding.Decode64FromBytes4(key)
			reader, err := base_encoding.GetZstdReader(bytes.NewReader(compressed))
			if err != nil {
				return err
			}
			decoded, err := io.ReadAll(reader)
			base_encoding.PutZstdReader(reader)
			if err != nil {
				return err
			}
			rows[slot] = decoded
		}
		return nil
	}))
	return rows
}

func TestPtcWindowStoredOncePerEpoch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		transition bool
		forkEpoch  uint64
	}{
		{name: "normal_transition", transition: true, forkEpoch: 2},
		{name: "transition_missing_first_post_block", transition: true, forkEpoch: 2},
		{name: "transition_only_blocks_post_fork", transition: true, forkEpoch: 2},
		{name: "empty_epoch_transition", forkEpoch: 0},
		{name: "builder_payment_after_missed_epochs", forkEpoch: 0},
		{name: "finality_no_updates_at_genesis", forkEpoch: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := tests.GetGloasFixture(tc.name, tc.transition, tc.forkEpoch)
			expected := expectedPtcWindowEpochs(t, fixture)
			env := runGloasAntiquary(t, fixture)
			actual := readPtcWindowEpochRows(t, env.db)

			require.Equal(t, expected, actual)
			require.NoError(t, env.db.View(context.Background(), func(tx kv.Tx) error {
				count, err := tx.Count(kv.PtcWindowTable)
				require.NoError(t, err)
				require.Zero(t, count)
				return nil
			}))
		})
	}
}

func TestGloasHistoricalStateRoots(t *testing.T) {
	fixture := tests.GetGloasFixture("empty_epoch_transition", false, 0)
	env := runGloasAntiquary(t, fixture)
	require.NoError(t, env.db.View(context.Background(), func(tx kv.Tx) error {
		validators := state_accessors.NewStaticValidatorTable()
		if err := state_accessors.ReadValidatorsTable(tx, validators); err != nil {
			return err
		}
		reader := historical_states_reader.NewHistoricalStatesReader(env.cfg, env.reader, validators, env.genesis, nil, env.synced)
		for _, block := range fixture.Blocks {
			got, err := reader.ReadHistoricalState(context.Background(), tx, block.Block.Slot)
			require.NoError(t, err, "slot %d", block.Block.Slot)
			require.NotNil(t, got, "slot %d", block.Block.Slot)
			root, err := got.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, block.Block.StateRoot, common.Hash(root), "slot %d", block.Block.Slot)
		}
		return nil
	}))
}

func TestPtcWindowEpochSnapshotRead(t *testing.T) {
	fixture := tests.GetGloasFixture("empty_epoch_transition", false, 0)
	expected := expectedPtcWindowEpochs(t, fixture)
	env := runGloasAntiquary(t, fixture)

	require.NoError(t, os.MkdirAll(env.dirs.SnapCaplin, 0o755))
	require.NoError(t, os.MkdirAll(env.dirs.Tmp, 0o755))
	allTypes := snapshotsync.MakeCaplinStateSnapshotsTypes(env.db, fixture.Config)
	ptcTypes := snapshotsync.SnapshotTypes{
		KeyValueGetters: map[string]snapshotsync.KeyValueGetter{
			kv.PtcWindowEpochTable: allTypes.KeyValueGetters[kv.PtcWindowEpochTable],
		},
		Compression: map[string]bool{},
	}
	stateSn := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: "test"}, env.dirs, ptcTypes, log.New())
	t.Cleanup(stateSn.Close)
	require.NoError(t, stateSn.DumpCaplinState(context.Background(), 64, 64, 0, env.dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, stateSn.OpenFolder())

	clearPtcWindowEpochRows(t, env.db)
	got, err := readGloasHistoricalState(t, env, stateSn, 32)
	require.NoError(t, err)
	require.NotNil(t, got)
	encoded, err := got.GetPtcWindow().EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, expected[32], encoded)
}

func TestPtcWindowEpochRestart(t *testing.T) {
	fixture := tests.GetGloasFixture("finality_no_updates_at_genesis", false, 0)
	expected := expectedPtcWindowEpochs(t, fixture)
	env := newGloasAntiquaryEnv(t, fixture)
	require.NoError(t, env.newAntiquary(nil).IncrementBeaconState(context.Background(), 48))
	require.NoError(t, env.newAntiquary(nil).IncrementBeaconState(context.Background(), 65))

	actual := readPtcWindowEpochRows(t, env.db)
	require.Equal(t, expected, actual)
}

func TestPtcWindowEpochUpgradeReplay(t *testing.T) {
	fixture := tests.GetGloasFixture("empty_epoch_transition", false, 0)
	expected := expectedPtcWindowEpochs(t, fixture)
	env := runGloasAntiquary(t, fixture)
	clearPtcWindowEpochRows(t, env.db)

	_, err := readGloasHistoricalState(t, env, nil, 32)
	require.ErrorIs(t, err, historical_states_reader.ErrMissingGloasData)
	require.ErrorContains(t, err, kv.PtcWindowEpochTable)

	require.NoError(t, env.newAntiquary(nil).IncrementBeaconState(context.Background(), 33))
	require.Equal(t, expected, readPtcWindowEpochRows(t, env.db))
	got, err := readGloasHistoricalState(t, env, nil, 32)
	require.NoError(t, err)
	require.NotNil(t, got)
	root, err := got.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, fixture.Blocks[0].Block.StateRoot, common.Hash(root))
}

func TestPtcWindowEpochUpgradeReplayStartsBeforeFork(t *testing.T) {
	fixture := tests.GetGloasFixture("normal_transition", true, 2)
	expected := expectedPtcWindowEpochs(t, fixture)
	env := runGloasAntiquary(t, fixture)
	clearPtcWindowEpochRows(t, env.db)

	a := env.newAntiquary(nil)
	require.NoError(t, env.db.View(context.Background(), func(tx kv.Tx) error {
		if err := state_accessors.ReadValidatorsTable(tx, a.validatorsTable); err != nil {
			return err
		}
		return a.initializeStateAntiquaryIfNeeded(context.Background(), tx)
	}))
	require.Equal(t, uint64(32), a.currentState.Slot())
	require.Less(t, a.currentState.Version(), clparams.GloasVersion)

	require.NoError(t, a.IncrementBeaconState(context.Background(), fixture.Blocks[len(fixture.Blocks)-1].Block.Slot+1))
	require.Equal(t, expected, readPtcWindowEpochRows(t, env.db))
}
