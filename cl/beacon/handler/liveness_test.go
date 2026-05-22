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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestUpdateLivenessWithBlock_ProposerIsLive(t *testing.T) {
	bcfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&bcfg, clparams.Phase0Version)
	block.Block.Slot = 100
	block.Block.ProposerIndex = 42

	liveSet := map[uint64]*live{
		42: {Index: 42, IsLive: false},
		99: {Index: 99, IsLive: false},
	}

	updateLivenessWithBlock(block, liveSet)

	require.True(t, liveSet[42].IsLive, "proposer should be marked live")
	require.False(t, liveSet[99].IsLive, "non-proposer should remain not live")
}

func TestUpdateLivenessWithBlock_VoluntaryExit(t *testing.T) {
	bcfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&bcfg, clparams.Phase0Version)
	block.Block.Slot = 100
	block.Block.ProposerIndex = 1

	block.Block.Body.VoluntaryExits.Append(&cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{
			Epoch:          3,
			ValidatorIndex: 55,
		},
	})

	liveSet := map[uint64]*live{
		55: {Index: 55, IsLive: false},
	}

	updateLivenessWithBlock(block, liveSet)

	require.True(t, liveSet[55].IsLive, "validator with voluntary exit should be marked live")
}

func TestUpdateLivenessWithBlock_ExecutionChange(t *testing.T) {
	bcfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&bcfg, clparams.BellatrixVersion)
	block.Block.Slot = 100
	block.Block.ProposerIndex = 1

	block.Block.Body.ExecutionChanges.Append(&cltypes.SignedBLSToExecutionChange{
		Message: &cltypes.BLSToExecutionChange{
			ValidatorIndex: 77,
		},
	})

	liveSet := map[uint64]*live{
		77: {Index: 77, IsLive: false},
	}

	updateLivenessWithBlock(block, liveSet)

	require.True(t, liveSet[77].IsLive, "validator with execution change should be marked live")
}

func TestUpdateLivenessWithBlock_UntrackedValidatorUnchanged(t *testing.T) {
	bcfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&bcfg, clparams.Phase0Version)
	block.Block.Slot = 100
	block.Block.ProposerIndex = 999

	liveSet := map[uint64]*live{
		42: {Index: 42, IsLive: false},
	}

	updateLivenessWithBlock(block, liveSet)

	require.False(t, liveSet[42].IsLive, "unrelated validator should remain not live")
}

func TestUpdateLivenessWithBlock_LastSlotOfEpoch(t *testing.T) {
	bcfg := clparams.MainnetBeaconConfig
	slotsPerEpoch := bcfg.SlotsPerEpoch
	epoch := uint64(5)
	lastSlot := (epoch+1)*slotsPerEpoch - 1

	block := cltypes.NewSignedBeaconBlock(&bcfg, clparams.Phase0Version)
	block.Block.Slot = lastSlot
	block.Block.ProposerIndex = 10

	liveSet := map[uint64]*live{
		10: {Index: 10, IsLive: false},
	}

	// The loop in the handler iterates: for i := epoch*SlotsPerEpoch; i < (epoch+1)*SlotsPerEpoch; i++
	// The last slot of the epoch is (epoch+1)*SlotsPerEpoch - 1, which must be included.
	// Before the fix, the loop used < ((epoch+1)*SlotsPerEpoch)-1 which missed this slot.
	updateLivenessWithBlock(block, liveSet)

	require.True(t, liveSet[10].IsLive, "validator proposing in last slot of epoch should be marked live")
}

func TestLivenessEndpoint_ValidatorLiveViaParticipation(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)

	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}
	fcu.FinalizedSlotVal = fcu.HeadSlotVal

	epoch := fcu.HeadSlotVal / 32

	// Set up participation: validator 0 has participation flags, validator 1 does not.
	participation := solid.NewParticipationBitList(256, 256)
	participation.Set(0, 0x07) // all participation flags set
	// validator 1 left at 0 (no participation)
	fcu.ParticipationVal = map[uint64]*solid.ParticipationBitList{
		epoch: participation,
	}

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal([]string{"0", "1"})
	require.NoError(t, err)

	resp, err := server.Client().Post(
		server.URL+"/eth/v1/validator/liveness/"+epochStr(epoch),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		Data []live `json:"data"`
	}
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(respBody, &result))

	require.Len(t, result.Data, 2)

	liveByIdx := map[int]bool{}
	for _, l := range result.Data {
		liveByIdx[l.Index] = l.IsLive
	}
	require.True(t, liveByIdx[0], "validator 0 with participation flags should be live")
	require.False(t, liveByIdx[1], "validator 1 without participation flags should not be live")
}

func TestLivenessEndpoint_NoParticipationNotLive(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)

	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}
	fcu.FinalizedSlotVal = fcu.HeadSlotVal

	epoch := fcu.HeadSlotVal / 32

	// Current epoch: participation with all zeros (validator 5 is NOT participating).
	currParticipation := solid.NewParticipationBitList(256, 256)
	// Previous epoch: validator 5 HAS participation flags.
	// If the handler incorrectly falls back to previous-epoch participation,
	// validator 5 would be reported as live — this test catches that regression.
	prevParticipation := solid.NewParticipationBitList(256, 256)
	prevParticipation.Set(5, 0x07)
	fcu.ParticipationVal = map[uint64]*solid.ParticipationBitList{
		epoch:     currParticipation,
		epoch - 1: prevParticipation,
	}

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	// Ask about validator 5 which has no participation and did not propose any block.
	body, err := json.Marshal([]string{"5"})
	require.NoError(t, err)

	resp, err := server.Client().Post(
		server.URL+"/eth/v1/validator/liveness/"+epochStr(epoch),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		Data []live `json:"data"`
	}
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(respBody, &result))

	require.Len(t, result.Data, 1)
	require.Equal(t, 5, result.Data[0].Index)
	require.False(t, result.Data[0].IsLive, "validator with no participation and no block proposal should not be live")
}

func TestLivenessEndpoint_FutureEpochReturnsError(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)

	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}
	fcu.FinalizedSlotVal = fcu.HeadSlotVal

	// The current epoch is determined by ethClock.GetCurrentEpoch(). The genesis-based
	// clock will return a very large epoch since time has advanced far past genesis.
	// Use an epoch that is definitely in the future: maxUint64-ish.
	futureEpoch := uint64(999999999)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal([]string{"0"})
	require.NoError(t, err)

	resp, err := server.Client().Post(
		server.URL+"/eth/v1/validator/liveness/"+epochStr(futureEpoch),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "future epoch should return 400")
}

func epochStr(v uint64) string {
	return strconv.FormatUint(v, 10)
}
