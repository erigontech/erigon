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

package stages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestProcessBlockPreservesStateRootForUnacceptedBlock(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ignored bool
	}{
		{name: "rejected"},
		{name: "ignored at anchor", ignored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLockOrderFixture(t, clparams.MainnetBeaconConfig.SlotsPerEpoch)
			cfg := fixture.cfg
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			cfg.indiciesDB = db
			stateRoot, err := fixture.headState.HashSSZ()
			require.NoError(t, err)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return beacon_indicies.WriteStateRoot(tx, fixture.headRoot, stateRoot)
			}))

			block := cltypes.NewSignedBeaconBlock(cfg.beaconCfg, clparams.GloasVersion)
			block.Block.Slot = fixture.headState.Slot() + 1
			block.Block.ParentRoot = fixture.headRoot
			block.Block.StateRoot = stateRoot
			if tc.ignored {
				block.Block.Slot = fixture.headState.Slot()
			}
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)
			cfg.forkChoice.OnTick((fixture.headState.GenesisTime() + block.Block.Slot*cfg.beaconCfg.SecondsPerSlot) * 1000)

			err = processBlock(t.Context(), cfg, block, true)
			if tc.ignored {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, forkchoice.ErrForkSchemaSlotMismatch)
				require.ErrorIs(t, err, forkchoice.ErrBlockInvalid)
			}
			_, accepted := cfg.forkChoice.GetHeader(blockRoot)
			require.False(t, accepted)

			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				root, err := beacon_indicies.ReadBlockRootByStateRoot(tx, stateRoot)
				require.NoError(t, err)
				require.Equal(t, fixture.headRoot, root)
				slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
				require.NoError(t, err)
				require.Nil(t, slot)
				body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, blockRoot))
				require.NoError(t, err)
				require.Empty(t, body)
				return nil
			}))
		})
	}
}

func TestProcessBlockPersistsAcceptedBlock(t *testing.T) {
	for _, known := range []bool{false, true} {
		name := "new block"
		if known {
			name = "known block"
		}
		t.Run(name, func(t *testing.T) {
			cfg, graph, blockRoot, _, _, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
			block := graph.parents[blockRoot]
			if !known {
				delete(graph.parents, blockRoot)
				graph.insertOnAdd = true
			}

			require.NoError(t, processBlock(t.Context(), cfg, block, false))
			_, accepted := cfg.forkChoice.GetHeader(blockRoot)
			require.True(t, accepted)
			require.NoError(t, cfg.indiciesDB.View(t.Context(), func(tx kv.Tx) error {
				root, err := beacon_indicies.ReadBlockRootByStateRoot(tx, block.Block.StateRoot)
				require.NoError(t, err)
				require.Equal(t, blockRoot, root)
				body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, blockRoot))
				require.NoError(t, err)
				require.NotEmpty(t, body)
				return nil
			}))
		})
	}
}

func TestProcessBlockPreservesAcceptedSignature(t *testing.T) {
	cfg, graph, root, _, _, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	accepted := graph.parents[root]
	require.NoError(t, processBlock(t.Context(), cfg, accepted, false))
	forged := *accepted
	forged.Signature[0] ^= 1

	require.NoError(t, processBlock(t.Context(), cfg, &forged, false))

	require.NoError(t, cfg.indiciesDB.View(t.Context(), func(tx kv.Tx) error {
		header, _, err := beacon_indicies.ReadSignedHeaderByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.Equal(t, accepted.Signature, header.Signature)
		return nil
	}))
}
