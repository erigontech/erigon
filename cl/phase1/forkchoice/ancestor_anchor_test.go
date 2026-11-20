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

package forkchoice

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

// reanchoredStore builds a store anchored on a state past genesis, as a warm restart does.
func reanchoredStore(t *testing.T) *ForkChoiceStore {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	src := buildExAnteStore(t)
	anchor, err := src.GetStateAtBlockRoot(src.ProposerBoostRoot())
	require.NoError(t, err)
	require.Positive(t, anchor.Slot())

	em := beaconevents.NewEventEmitter()
	gs, err := initial_state.GetGenesisState(t.Context(), 1)
	require.NoError(t, err)
	clk := eth_clock.NewEthereumClock(gs.GenesisTime(), gs.GenesisValidatorsRoot(), cfg)
	bs := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	forkGraph, err := fork_graph.NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	f, err := NewForkChoiceStore(clk, anchor, nil, pool.NewOperationsPool(cfg),
		forkGraph,
		em, synced_data.NewSyncedDataManager(cfg, true), bs, public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(), false, nil)
	require.NoError(t, err)
	return f
}

func TestAncestorBelowTheAnchorIsTheAnchor(t *testing.T) {
	f := reanchoredStore(t)
	anchorRoot, anchorSlot := f.forkGraph.AnchorRoot(), f.forkGraph.AnchorSlot()

	require.Equal(t, anchorRoot, f.Ancestor(anchorRoot, anchorSlot-1).Root)
}

func TestAReanchoredStoreKeepsItsAnchorViable(t *testing.T) {
	f := reanchoredStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)

	blocks := f.getFilteredBlockTree(justified.Root, justified, f.Slot())

	require.Contains(t, blocks, f.forkGraph.AnchorRoot())
}
