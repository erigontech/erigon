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

package handler

import (
	"embed"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beacontest"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/lightclient_utils"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
)

//go:embed test_data/*
var testData embed.FS

var TestDatae = beacontest.NewBasePathFs(afero.FromIOFS{FS: testData}, "test_data")

//go:embed harness/*
var testHarness embed.FS

var Harnesses = beacontest.NewBasePathFs(afero.FromIOFS{FS: testHarness}, "harness")

type harnessConfig struct {
	t         *testing.T
	v         clparams.StateVersion
	finalized bool
	forkmode  int
}

func defaultHarnessOpts(c harnessConfig) []beacontest.HarnessOption {
	logger := log.New()
	for _, v := range os.Args {
		if !strings.Contains(v, "test.v") || strings.Contains(v, "test.v=false") {
			logger.SetHandler(log.DiscardHandler())
		}
	}
	_, blocks, _, _, postState, handler, _, sm, fcu, _ := setupTestingHandler(c.t, c.v, logger, true)

	var err error

	lastBlockRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(c.t, err)

	if c.v >= clparams.AltairVersion {
		fcu.LightClientBootstraps[lastBlockRoot], err = lightclient_utils.CreateLightClientBootstrap(postState, blocks[len(blocks)-1])
		require.NoError(c.t, err)
		fcu.NewestLCUpdate = cltypes.NewLightClientUpdate(postState.Version())
		fcu.NewestLCUpdate.NextSyncCommittee = postState.NextSyncCommittee()
		fcu.NewestLCUpdate.SignatureSlot = 1234
		fcu.NewestLCUpdate.SyncAggregate = blocks[len(blocks)-1].Block.Body.SyncAggregate
		fcu.NewestLCUpdate.AttestedHeader, err = lightclient_utils.BlockToLightClientHeader(blocks[len(blocks)-1])
		require.NoError(c.t, err)
		fcu.NewestLCUpdate.FinalizedHeader = fcu.NewestLCUpdate.AttestedHeader
		fcu.LCUpdates[1] = fcu.NewestLCUpdate
		fcu.LCUpdates[2] = fcu.NewestLCUpdate
	}

	if c.forkmode == 0 {
		fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
		require.NoError(c.t, err)
		fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

		fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}
		if c.finalized {
			fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}
			fcu.FinalizedSlotVal = math.MaxUint64
		} else {
			fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}
			fcu.FinalizedSlotVal = 0
			fcu.StateAtBlockRootVal[fcu.HeadVal] = postState
			require.NoError(c.t, sm.OnHeadState(postState))
		}
	}

	if c.forkmode == 1 {
		sm.OnHeadState(postState)
		var s *state.CachingBeaconState
		for s == nil {
			sm.ViewHeadState(func(headState *state.CachingBeaconState) error {
				s = headState
				return nil
			})
		}
		s.SetSlot(789274827847783)

		fcu.HeadSlotVal = 128
		fcu.HeadVal = common.Hash{1, 2, 3}

		fcu.WeightsMock = []forkchoice.ForkNode{
			{
				BlockRoot:  common.Hash{1, 2, 3},
				ParentRoot: common.Hash{1, 2, 3},
				Slot:       128,
				Weight:     1,
			},
			{
				BlockRoot:  common.Hash{1, 2, 2, 4, 5, 3},
				ParentRoot: common.Hash{1, 2, 5},
				Slot:       128,
				Weight:     2,
			},
		}

		fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 1, Root: common.Hash{1, 2, 3}}
		fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: 2, Root: common.Hash{1, 2, 3}}
	}
	sm.OnHeadState(postState)

	return []beacontest.HarnessOption{
		beacontest.WithTesting(c.t),
		beacontest.WithFilesystem("td", TestDatae),
		beacontest.WithHandler("i", handler),
	}
}
