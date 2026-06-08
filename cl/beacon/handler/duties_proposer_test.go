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

package handler

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestGetDutiesProposerHistoricalEpochIgnoresHeadEffectiveBalance(t *testing.T) {
	_, blocks, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = postState.Slot()

	epoch := uint64(3)
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: headRoot}
	fcu.FinalizedSlotVal = (epoch+1)*handler.beaconChainCfg.SlotsPerEpoch - 1

	before := getProposerDutiesForEpoch(t, handler, epoch)
	require.True(t, before.Finalized)
	require.NotEmpty(t, before.DependentRoot)

	headWithChangedBalances, err := postState.Copy()
	require.NoError(t, err)
	activeIndices := headWithChangedBalances.GetActiveValidatorsIndices(epoch)
	require.NotEmpty(t, activeIndices)
	chosenIndex := activeIndices[0]
	for _, validatorIndex := range activeIndices {
		headWithChangedBalances.SetEffectiveBalanceForValidatorAtIndex(int(validatorIndex), 0)
	}
	headWithChangedBalances.SetEffectiveBalanceForValidatorAtIndex(int(chosenIndex), handler.beaconChainCfg.MaxEffectiveBalanceForVersion(headWithChangedBalances.Version()))

	manager, ok := syncedData.(*synced_data.SyncedDataManager)
	require.True(t, ok)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(headWithChangedBalances, headRoot))

	after := getProposerDutiesForEpoch(t, handler, epoch)
	require.Equal(t, before, after)
}

func TestGetHistoricalProposerDependentRootEpochZeroReturnsGenesisRoot(t *testing.T) {
	db, _, _, preState, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	genesisRoot, err := preState.GetBlockRootAtSlot(0)
	require.NoError(t, err)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.MarkRootCanonical(context.Background(), tx, 0, genesisRoot))
	require.NoError(t, tx.Commit())

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()
	dependentRoot, err := handler.getHistoricalProposerDependentRoot(roTx, state_accessors.GetValFnTxAndSnapshot(roTx, nil), 0)
	require.NoError(t, err)
	require.Equal(t, genesisRoot, dependentRoot)
}

func TestGetDutiesProposerEpochZeroReturnsGenesisRootAndDuties(t *testing.T) {
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)

	genesisState, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)
	handler.stateReader = historical_states_reader.NewHistoricalStatesReader(handler.beaconChainCfg, nil, state_accessors.NewStaticValidatorTable(), genesisState, nil, handler.syncedData)

	genesisRoot := common.Hash{1, 2, 3}
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.MarkRootCanonical(context.Background(), tx, 0, genesisRoot))
	require.NoError(t, tx.Commit())

	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 0, Root: genesisRoot}
	fcu.FinalizedSlotVal = handler.beaconChainCfg.SlotsPerEpoch - 1

	expectedProposers, err := genesisState.GetBeaconProposerIndices(0)
	require.NoError(t, err)

	response := getProposerDutiesForEpoch(t, handler, 0)
	require.True(t, response.Finalized)
	require.Equal(t, genesisRoot.String(), response.DependentRoot)
	require.Len(t, response.Data, int(handler.beaconChainCfg.SlotsPerEpoch))
	for i, duty := range response.Data {
		require.Equal(t, uint64(i), duty.Slot)
		require.Equal(t, expectedProposers[i], duty.ValidatorIndex)
	}
}

func TestGetDutiesProposerAdvancesHeadCopyAcrossTargetFork(t *testing.T) {
	_, blocks, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	headEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	targetEpoch := headEpoch + 1
	handler.beaconChainCfg.FuluForkEpoch = headEpoch
	handler.beaconChainCfg.GloasForkEpoch = targetEpoch
	handler.beaconChainCfg.InitializeForkSchedule()

	stateCfg := *postState.BeaconConfig()
	stateCfg.FuluForkEpoch = headEpoch
	stateCfg.GloasForkEpoch = targetEpoch
	stateCfg.InitializeForkSchedule()
	encodedPostState, err := postState.EncodeSSZ(nil)
	require.NoError(t, err)
	isolatedPostState := state.New(&stateCfg)
	require.NoError(t, isolatedPostState.DecodeSSZ(encodedPostState, int(clparams.ElectraVersion)))

	fuluHead, err := isolatedPostState.Copy()
	require.NoError(t, err)
	require.NoError(t, fuluHead.UpgradeToFulu())
	require.Equal(t, clparams.FuluVersion, fuluHead.Version())

	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = fuluHead.Slot()

	manager, ok := syncedData.(*synced_data.SyncedDataManager)
	require.True(t, ok)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(fuluHead, headRoot))

	expectedState, err := fuluHead.Copy()
	require.NoError(t, err)
	expectedSlot := targetEpoch * handler.beaconChainCfg.SlotsPerEpoch
	require.NoError(t, transition.DefaultMachine.ProcessSlots(expectedState, expectedSlot))
	require.Equal(t, clparams.GloasVersion, expectedState.Version())
	expectedProposers, err := expectedState.GetBeaconProposerIndices(targetEpoch)
	require.NoError(t, err)

	response := getProposerDutiesForEpoch(t, handler, targetEpoch)
	require.False(t, response.Finalized)
	require.Equal(t, clparams.GloasVersion.String(), response.Version)
	require.NoError(t, handler.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, clparams.FuluVersion, headState.Version())
		return nil
	}))
	for i, duty := range response.Data {
		require.Equal(t, expectedSlot+uint64(i), duty.Slot)
		require.Equal(t, expectedProposers[i], duty.ValidatorIndex)
	}
}

func TestGetDutiesProposerUsesTargetForkBeforeHeadFork(t *testing.T) {
	_, blocks, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	headEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	targetEpoch := headEpoch - 1
	handler.beaconChainCfg.ElectraForkEpoch = headEpoch
	handler.beaconChainCfg.FuluForkEpoch = headEpoch
	handler.beaconChainCfg.GloasForkEpoch = headEpoch
	handler.beaconChainCfg.InitializeForkSchedule()

	stateCfg := *postState.BeaconConfig()
	stateCfg.ElectraForkEpoch = headEpoch
	stateCfg.FuluForkEpoch = headEpoch
	stateCfg.GloasForkEpoch = headEpoch
	stateCfg.InitializeForkSchedule()
	encodedPostState, err := postState.EncodeSSZ(nil)
	require.NoError(t, err)
	isolatedPostState := state.New(&stateCfg)
	require.NoError(t, isolatedPostState.DecodeSSZ(encodedPostState, int(clparams.ElectraVersion)))

	gloasHead, err := isolatedPostState.Copy()
	require.NoError(t, err)
	require.NoError(t, gloasHead.UpgradeToFulu())
	require.NoError(t, gloasHead.UpgradeToGloas())
	require.Equal(t, clparams.GloasVersion, gloasHead.Version())

	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = gloasHead.Slot()
	fcu.FinalizedSlotVal = targetEpoch*handler.beaconChainCfg.SlotsPerEpoch - 1

	manager, ok := syncedData.(*synced_data.SyncedDataManager)
	require.True(t, ok)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(gloasHead, headRoot))

	expectedState, err := gloasHead.Copy()
	require.NoError(t, err)
	expectedState.SetVersion(clparams.DenebVersion)
	expectedProposers, err := expectedState.GetBeaconProposerIndices(targetEpoch)
	require.NoError(t, err)
	gloasProposers, err := gloasHead.GetBeaconProposerIndices(targetEpoch)
	require.NoError(t, err)
	require.NotEqual(t, gloasProposers, expectedProposers)

	response := getProposerDutiesForEpoch(t, handler, targetEpoch)
	require.False(t, response.Finalized)
	require.Equal(t, clparams.DenebVersion.String(), response.Version)
	require.NoError(t, handler.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		require.Equal(t, clparams.GloasVersion, headState.Version())
		return nil
	}))
	expectedSlot := targetEpoch * handler.beaconChainCfg.SlotsPerEpoch
	for i, duty := range response.Data {
		require.Equal(t, expectedSlot+uint64(i), duty.Slot)
		require.Equal(t, expectedProposers[i], duty.ValidatorIndex)
	}
}

func TestGetDutiesProposerFutureEpochTooFarReturnsBadRequest(t *testing.T) {
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = postState.Slot()

	headEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	epoch := headEpoch + maxEpochsLookaheadForDuties + 1
	request := httptest.NewRequest(http.MethodGet, "/eth/v1/validator/duties/proposer/"+strconv.FormatUint(epoch, 10), nil)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

type proposerDutiesResponse struct {
	Data          []proposerDuties `json:"data"`
	Finalized     bool             `json:"finalized"`
	DependentRoot string           `json:"dependent_root"`
	Version       string           `json:"version"`
}

func TestEpochSlotOverflows(t *testing.T) {
	require.False(t, epochSlotOverflows(0, 32))
	require.False(t, epochSlotOverflows(1000, 32))
	require.False(t, epochSlotOverflows(math.MaxUint64/32-1, 32))
	require.True(t, epochSlotOverflows(math.MaxUint64/32, 32))
	require.True(t, epochSlotOverflows(math.MaxUint64, 32))
	require.False(t, epochSlotOverflows(0, 0))
}

func TestGetDutiesProposerEpochOverflowReturnsBadRequest(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	epoch := uint64(math.MaxUint64)
	request := httptest.NewRequest(http.MethodGet, "/eth/v1/validator/duties/proposer/"+strconv.FormatUint(epoch, 10), nil)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "overflows")
}

func TestDependentRootSlotNoUnderflow(t *testing.T) {
	slotsPerEpoch := uint64(32)
	tests := []struct {
		name     string
		epoch    uint64
		attester bool
		wantSlot uint64
	}{
		{"proposer_epoch0", 0, false, 0},
		{"attester_epoch0", 0, true, 0},
		{"attester_epoch1", 1, true, 0},
		{"proposer_epoch1", 1, false, 31},
		{"attester_epoch2", 2, true, 31},
		{"proposer_epoch2", 2, false, 63},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var slot uint64
			switch {
			case tc.epoch == 0:
				slot = 0
			case tc.attester && tc.epoch <= 1:
				slot = 0
			case tc.attester:
				slot = (tc.epoch-1)*slotsPerEpoch - 1
			default:
				slot = tc.epoch*slotsPerEpoch - 1
			}
			require.Equal(t, tc.wantSlot, slot)
		})
	}
}

func TestGetAttesterDutiesEpochOverflowReturnsBadRequest(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	epoch := uint64(math.MaxUint64)
	body := strings.NewReader(`["0"]`)
	request := httptest.NewRequest(http.MethodPost, "/eth/v1/validator/duties/attester/"+strconv.FormatUint(epoch, 10), body)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "overflows")
}

func getProposerDutiesForEpoch(t *testing.T, handler *ApiHandler, epoch uint64) proposerDutiesResponse {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, "/eth/v1/validator/duties/proposer/"+strconv.FormatUint(epoch, 10), nil)
	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var body proposerDutiesResponse
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&body))
	require.NotEmpty(t, body.Data)
	return body
}
