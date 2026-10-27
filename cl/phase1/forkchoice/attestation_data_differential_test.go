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

package forkchoice_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/common"
)

func TestAttestationDataMatchesProducer(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	blocks, anchorState, _ := tests.GetBellatrixRandom()
	store := newBellatrixForkChoiceStore(t, anchorState, nil)
	for _, block := range blocks {
		require.NoError(t, store.OnBlock(context.Background(), block, false, true, false))
	}

	cfg := &clparams.MainnetBeaconConfig
	sources := make(map[solid.Checkpoint]struct{})
	var sawHeadTarget, sawAncestorTarget bool
	for _, block := range blocks {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		headRoot := common.Hash(root)
		headState, err := store.GetStateAtBlockRoot(headRoot)
		require.NoError(t, err)
		require.NotNil(t, headState)
		headEpoch := block.Block.Slot / cfg.SlotsPerEpoch
		for _, slot := range []uint64{
			block.Block.Slot,
			(headEpoch+1)*cfg.SlotsPerEpoch - 1,
			(headEpoch + 1) * cfg.SlotsPerEpoch,
			(headEpoch+2)*cfg.SlotsPerEpoch - 1,
			(headEpoch + 2) * cfg.SlotsPerEpoch,
		} {
			got, ok := store.AttestationData(headRoot, slot)
			if slot/cfg.SlotsPerEpoch > headEpoch+1 {
				require.False(t, ok, "head slot %d, requested slot %d", block.Block.Slot, slot)
				continue
			}

			require.True(t, ok, "head slot %d, requested slot %d", block.Block.Slot, slot)
			want, err := attestation_producer.New(cfg).ProduceAndCacheAttestationData(nil, headState, headRoot, slot)
			require.NoError(t, err)
			require.Equal(t, want, got)

			sources[got.Source] = struct{}{}
			if got.Target.Root == headRoot {
				sawHeadTarget = true
			} else {
				sawAncestorTarget = true
			}
		}
	}

	require.GreaterOrEqual(t, len(sources), 2)
	require.True(t, sawHeadTarget)
	require.True(t, sawAncestorTarget)
}
