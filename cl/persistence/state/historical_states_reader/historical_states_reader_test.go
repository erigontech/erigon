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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func runTest(t *testing.T, blocks []*cltypes.SignedBeaconBlock, preState, postState *state.CachingBeaconState) {
	db := memdb.NewTestDB(t, kv.ChainDB)
	reader := tests.LoadChain(blocks, postState, db, t)

	sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	sn.OnHeadState(postState)
	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New("/tmp"), nil, db, nil, nil, reader, sn, log.New(), true, true, true, false, nil)
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
	t.Skip("oom on CI")
	blocks, preState, postState := tests.GetCapellaRandom()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryPhase0(t *testing.T) {
	t.Skip("oom on CI")
	blocks, preState, postState := tests.GetPhase0Random()
	runTest(t, blocks, preState, postState)
}

func TestStateAntiquaryBellatrix(t *testing.T) {
	t.Skip("oom on CI")
	blocks, preState, postState := tests.GetBellatrixRandom()
	runTest(t, blocks, preState, postState)
}
