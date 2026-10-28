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

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

var (
	realizedCheckpoint   = solid.Checkpoint{Epoch: 1, Root: common.Hash{0xa1}}
	unrealizedCheckpoint = solid.Checkpoint{Epoch: 2, Root: common.Hash{0xa2}}
	genesisRealized      = solid.Checkpoint{Root: common.Hash{0xb1}}
	genesisUnrealized    = solid.Checkpoint{Root: common.Hash{0xb2}}
	genesisRoot          = common.Hash{0x01}
	genesisHeadRoot      = common.Hash{0x02}
	previousRoot         = common.Hash{0x11}
	epochStartRoot       = common.Hash{0x22}
	headRoot             = common.Hash{0x33}
	oldRoot              = common.Hash{0x44}
	noAncestorRoot       = common.Hash{0x55}
	unknownRoot          = common.Hash{0x99}
)

func TestAttestationData(t *testing.T) {
	tests := []struct {
		name             string
		headRoot         common.Hash
		slot             uint64
		removeFinality   bool
		removeUnrealized bool
		want             solid.AttestationData
		ok               bool
	}{
		{
			name:     "genesis epoch uses realized source and genesis target",
			headRoot: genesisHeadRoot,
			slot:     7,
			want: solid.AttestationData{
				Slot:            7,
				BeaconBlockRoot: genesisHeadRoot,
				Source:          genesisRealized,
				Target:          solid.Checkpoint{Root: genesisRoot},
			},
			ok: true,
		},
		{
			name:     "first epoch start uses genesis unrealized source and head target",
			headRoot: genesisHeadRoot,
			slot:     32,
			want: solid.AttestationData{
				Slot:            32,
				BeaconBlockRoot: genesisHeadRoot,
				Source:          genesisUnrealized,
				Target:          solid.Checkpoint{Epoch: 1, Root: genesisHeadRoot},
			},
			ok: true,
		},
		{
			name:     "same epoch uses realized source and epoch start target",
			headRoot: headRoot,
			slot:     66,
			want: solid.AttestationData{
				Slot:            66,
				BeaconBlockRoot: headRoot,
				Source:          realizedCheckpoint,
				Target:          solid.Checkpoint{Epoch: 2, Root: epochStartRoot},
			},
			ok: true,
		},
		{
			name:     "one epoch behind uses unrealized source and head target",
			headRoot: previousRoot,
			slot:     70,
			want: solid.AttestationData{
				Slot:            70,
				BeaconBlockRoot: previousRoot,
				Source:          unrealizedCheckpoint,
				Target:          solid.Checkpoint{Epoch: 2, Root: previousRoot},
			},
			ok: true,
		},
		{
			name:     "two epochs behind misses",
			headRoot: oldRoot,
			slot:     96,
		},
		{
			name:     "block at epoch start remains target",
			headRoot: epochStartRoot,
			slot:     70,
			want: solid.AttestationData{
				Slot:            70,
				BeaconBlockRoot: epochStartRoot,
				Source:          realizedCheckpoint,
				Target:          solid.Checkpoint{Epoch: 2, Root: epochStartRoot},
			},
			ok: true,
		},
		{
			name:           "finality checkpoints miss",
			headRoot:       headRoot,
			slot:           70,
			removeFinality: true,
		},
		{
			name:             "unrealized justification miss",
			headRoot:         headRoot,
			slot:             70,
			removeUnrealized: true,
		},
		{
			name:     "unknown head misses",
			headRoot: unknownRoot,
			slot:     70,
		},
		{
			name:     "head after requested slot misses",
			headRoot: headRoot,
			slot:     65,
		},
		{
			name:     "zero ancestor misses",
			headRoot: noAncestorRoot,
			slot:     70,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newAttestationDataTestStore(t)
			if tt.removeFinality {
				store.finalityCheckpoints.Remove(tt.headRoot)
			}
			if tt.removeUnrealized {
				store.unrealizedJustifications.Delete(tt.headRoot)
			}

			got, ok := store.AttestationData(tt.headRoot, tt.slot)

			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func newAttestationDataTestStore(t *testing.T) *ForkChoiceStore {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	graph := &getFinalizedExecutionHashForkGraph{
		blocks:  make(map[common.Hash]*cltypes.SignedBeaconBlock),
		headers: make(map[common.Hash]*cltypes.BeaconBlockHeader),
	}
	addBlock := func(root, parent common.Hash, slot uint64) {
		block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
		block.Block.Slot = slot
		block.Block.ParentRoot = parent
		graph.blocks[root] = block
		graph.headers[root] = block.SignedBeaconBlockHeader().Header
	}
	addBlock(genesisRoot, common.Hash{}, 0)
	addBlock(genesisHeadRoot, genesisRoot, 1)
	addBlock(oldRoot, common.Hash{}, 31)
	addBlock(previousRoot, oldRoot, 63)
	addBlock(epochStartRoot, previousRoot, 64)
	addBlock(headRoot, epochStartRoot, 66)
	graph.headers[noAncestorRoot] = &cltypes.BeaconBlockHeader{Slot: 66}

	checkpoints, err := lru.New[common.Hash, finalityCheckpoints](checkpointsPerCache)
	require.NoError(t, err)
	store := &ForkChoiceStore{
		beaconCfg:           &cfg,
		forkGraph:           graph,
		finalityCheckpoints: checkpoints,
	}
	for _, root := range []common.Hash{oldRoot, previousRoot, epochStartRoot, headRoot, noAncestorRoot} {
		store.finalityCheckpoints.Add(root, finalityCheckpoints{currentJustifiedCheckpoint: realizedCheckpoint})
		store.unrealizedJustifications.Store(root, unrealizedCheckpoint)
	}
	store.finalityCheckpoints.Add(genesisHeadRoot, finalityCheckpoints{currentJustifiedCheckpoint: genesisRealized})
	store.unrealizedJustifications.Store(genesisHeadRoot, genesisUnrealized)
	return store
}
