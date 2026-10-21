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

package historical_states_reader_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func setupGloasReplay(t *testing.T, fixture string) (kv.RwDB, []*cltypes.SignedBeaconBlock, map[uint64]*state.CachingBeaconState, *historical_states_reader.HistoricalStatesReader, *tests.MockBlockReader) {
	t.Helper()
	gloas := tests.GetGloasFixture(fixture, false, 0)
	blocks, preState, cfg := gloas.Blocks, gloas.PreState, gloas.Config
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	reader := tests.LoadChain(blocks, db, t)

	live, err := preState.Copy()
	require.NoError(t, err)
	expected := make(map[uint64]*state.CachingBeaconState, len(blocks))
	for _, block := range blocks {
		require.NoError(t, transition.TransitionState(live, block, nil, true))
		expected[block.Block.Slot], err = live.Copy()
		require.NoError(t, err)
	}

	sn := synced_data.NewSyncedDataManager(cfg, true)
	require.NoError(t, sn.OnHeadState(live))
	antiquaryState, err := preState.Copy()
	require.NoError(t, err)
	vt := state_accessors.NewStaticValidatorTable()
	ctx := context.Background()
	a := antiquary.NewAntiquary(ctx, nil, antiquaryState, vt, cfg, datadir.New(t.TempDir()), nil, db, nil, nil, reader, sn, log.New(), true, true, true, false, nil)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))

	readerVT := state_accessors.NewStaticValidatorTable()
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		return state_accessors.ReadValidatorsTable(tx, readerVT)
	}))
	genesisState, err := preState.Copy()
	require.NoError(t, err)
	hr := historical_states_reader.NewHistoricalStatesReader(cfg, reader, readerVT, genesisState, nil, sn)
	return db, blocks, expected, hr, reader
}

func runTest(t *testing.T, blocks []*cltypes.SignedBeaconBlock, preState, postState *state.CachingBeaconState) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	reader := tests.LoadChain(blocks, db, t)

	sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, sn.OnHeadState(postState))
	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, nil, nil, reader, sn, log.New(), true, true, true, false, nil)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	// Now lets test it against the reader
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	vt = state_accessors.NewStaticValidatorTable()
	require.NoError(t, state_accessors.ReadValidatorsTable(tx, vt))
	hr := historical_states_reader.NewHistoricalStatesReader(&clparams.MainnetBeaconConfig, reader, vt, preState, nil, sn)
	s, err := hr.ReadHistoricalState(ctx, tx, blocks[len(blocks)-1].Block.Slot)
	require.NoError(t, err)

	postHash, err := s.HashSSZ()
	require.NoError(t, err)
	_, err = postState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(postHash), blocks[len(blocks)-1].Block.StateRoot)
}

func TestStateAntiquaryCapella(t *testing.T) {
	if os.Getenv("ERIGON_RUN_HEAVY_TESTS") == "" {
		t.Skip("oom-prone; set ERIGON_RUN_HEAVY_TESTS=1 to run")
	}
	blocks, preState, postState := tests.GetCapellaRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryPhase0(t *testing.T) {
	if os.Getenv("ERIGON_RUN_HEAVY_TESTS") == "" {
		t.Skip("oom-prone; set ERIGON_RUN_HEAVY_TESTS=1 to run")
	}
	blocks, preState, postState := tests.GetPhase0Random()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryBellatrix(t *testing.T) {
	if os.Getenv("ERIGON_RUN_HEAVY_TESTS") == "" {
		t.Skip("oom-prone; set ERIGON_RUN_HEAVY_TESTS=1 to run")
	}
	blocks, preState, postState := tests.GetBellatrixRandom()
	runTest(t, blocks, preState, postState)
}

func TestReadHistoricalStateGloasParticipation(t *testing.T) {
	for _, fixture := range []string{"missed_slot_payload_availability", "attestation"} {
		t.Run(fixture, func(t *testing.T) {
			db, blocks, expected, hr, _ := setupGloasReplay(t, fixture)
			tx, err := db.BeginRo(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			for _, block := range blocks {
				got, err := hr.ReadHistoricalState(context.Background(), tx, block.Block.Slot)
				require.NoError(t, err)
				require.Equalf(t, expected[block.Block.Slot].CurrentEpochParticipation().Bytes(), got.CurrentEpochParticipation().Bytes(), "current participation at slot %d", block.Block.Slot)
				require.Equalf(t, expected[block.Block.Slot].PreviousEpochParticipation().Bytes(), got.PreviousEpochParticipation().Bytes(), "previous participation at slot %d", block.Block.Slot)
				root, err := got.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, block.Block.StateRoot, common.Hash(root))
			}
		})
	}
}

func TestReadHistoricalStateGloasGenesisBlockWithoutAttestations(t *testing.T) {
	db, blocks, expected, hr, reader := setupGloasReplay(t, "missed_slot_payload_availability")
	gloas := tests.GetGloasFixture("missed_slot_payload_availability", false, 0)
	preState, cfg := gloas.PreState, gloas.Config
	genesisBlock := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	genesisBlock.Block.Slot = preState.Slot()
	genesisBlock.Block.ParentRoot = preState.LatestBlockHeader().ParentRoot
	reader.U[preState.Slot()] = genesisBlock

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	slot := blocks[0].Block.Slot
	got, err := hr.ReadHistoricalState(context.Background(), tx, slot)
	require.NoError(t, err)
	require.Equal(t, expected[slot].CurrentEpochParticipation().Bytes(), got.CurrentEpochParticipation().Bytes())
	require.Equal(t, expected[slot].PreviousEpochParticipation().Bytes(), got.PreviousEpochParticipation().Bytes())
	root, err := got.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, blocks[0].Block.StateRoot, common.Hash(root))
}

func TestReadParticipationsReturnsBlockRootError(t *testing.T) {
	db, _, _, hr, _ := setupGloasReplay(t, "missed_slot_payload_availability")
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.Delete(kv.BlockRoot, base_encoding.Encode64ToBytes4(2)))

	getter := state_accessors.GetValFnTxAndSnapshot(tx, nil)
	_, _, err = hr.ReadParticipations(tx, getter, 3)
	require.ErrorContains(t, err, "invalid block root length")
}
