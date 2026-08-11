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
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	mock_services2 "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
)

func TestGetSelectedHeadDoesNotTrailTheMemoizedHeadState(t *testing.T) {
	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().StateHead().Return(common.Hash{0xbb}, uint64(99), true)
	syncedData.EXPECT().SelectedHead().Return(common.Hash{0xaa}, uint64(100), true)

	fcu := mock_services2.NewForkChoiceStorageMock(t)

	a := &ApiHandler{
		enableMemoizedHeadState: true,
		syncedData:              syncedData,
		forkchoiceStore:         fcu,
	}

	root, slot, statusCode, err := a.getSelectedHead()
	require.NoError(t, err)
	require.Equal(t, 0, statusCode)
	require.Equal(t, common.Hash{0xaa}, root)
	require.Equal(t, uint64(100), slot)
}

func TestDebugBeaconHeadsReportsSelectedHeadOptimistic(t *testing.T) {
	for _, optimistic := range []bool{false, true} {
		t.Run(strconv.FormatBool(optimistic), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			syncedData := sync_mock_services.NewMockSyncedData(ctrl)
			syncedData.EXPECT().Syncing().Return(false)
			syncedData.EXPECT().StateHead().Return(common.Hash{0xbb}, uint64(99), true)
			syncedData.EXPECT().SelectedHead().Return(common.Hash{0xaa}, uint64(100), true)

			fcu := mock_services2.NewForkChoiceStorageMock(t)
			fcu.IsRootOptimisticVal = optimistic
			a := &ApiHandler{
				enableMemoizedHeadState: true,
				syncedData:              syncedData,
				forkchoiceStore:         fcu,
			}

			response, err := a.GetEthV2DebugBeaconHeads(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/eth/v2/debug/beacon/heads", nil))
			require.NoError(t, err)
			heads := response.Data.([]any)
			require.Len(t, heads, 1)
			require.Equal(t, optimistic, heads[0].(map[string]any)["execution_optimistic"])
		})
	}
}

func TestGetStateHeadKeepsMemoizedStateIdentity(t *testing.T) {
	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().StateHead().Return(common.Hash{0xbb}, uint64(99), true)

	fcu := mock_services2.NewForkChoiceStorageMock(t)
	fcu.HeadVal = common.Hash{0xaa}
	fcu.HeadSlotVal = 100

	a := &ApiHandler{
		enableMemoizedHeadState: true,
		syncedData:              syncedData,
		forkchoiceStore:         fcu,
	}

	root, slot, statusCode, err := a.getStateHead()
	require.NoError(t, err)
	require.Equal(t, 0, statusCode)
	require.Equal(t, common.Hash{0xbb}, root)
	require.Equal(t, uint64(99), slot)
}

func TestGetSelectedHeadFallsBackToMemoizedStateIdentity(t *testing.T) {
	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().StateHead().Return(common.Hash{0xbb}, uint64(99), true)
	syncedData.EXPECT().SelectedHead().Return(common.Hash{}, uint64(0), false)

	a := &ApiHandler{
		enableMemoizedHeadState: true,
		syncedData:              syncedData,
		forkchoiceStore:         mock_services2.NewForkChoiceStorageMock(t),
	}

	root, slot, statusCode, err := a.getSelectedHead()
	require.NoError(t, err)
	require.Equal(t, 0, statusCode)
	require.Equal(t, common.Hash{0xbb}, root)
	require.Equal(t, uint64(99), slot)
}

func TestHeadBlockIDUsesSelectedHead(t *testing.T) {
	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().StateHead().Return(common.Hash{0xbb}, uint64(99), true)
	syncedData.EXPECT().SelectedHead().Return(common.Hash{0xaa}, uint64(100), true)

	fcu := mock_services2.NewForkChoiceStorageMock(t)
	fcu.HeadVal = common.Hash{0xcc}
	fcu.HeadSlotVal = 101
	a := &ApiHandler{
		enableMemoizedHeadState: true,
		syncedData:              syncedData,
		forkchoiceStore:         fcu,
	}

	request := httptest.NewRequest(http.MethodGet, "/eth/v1/beacon/blocks/head/root", nil)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("block_id", "head")
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
	blockID, err := beaconhttp.BlockIdFromRequest(request)
	require.NoError(t, err)

	root, err := a.rootFromBlockId(request.Context(), nil, blockID)
	require.NoError(t, err)
	require.Equal(t, common.Hash{0xaa}, root)
}

func TestGetStateHeadReportsSyncing(t *testing.T) {
	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().StateHead().Return(common.Hash{}, uint64(0), false)

	a := &ApiHandler{
		enableMemoizedHeadState: true,
		syncedData:              syncedData,
		forkchoiceStore:         mock_services2.NewForkChoiceStorageMock(t),
	}

	_, _, statusCode, err := a.getStateHead()
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, statusCode)
}

func TestSelectedHeadStateUsesWinningSideBranch(t *testing.T) {
	submittedRoot := common.Hash{0xaa}
	selectedRoot := common.Hash{0xbb}
	submittedState := state.New(&clparams.MainnetBeaconConfig)
	selectedState := state.New(&clparams.MainnetBeaconConfig)
	fcu := mock_services2.NewForkChoiceStorageMock(t)
	fcu.HeadVal = selectedRoot
	fcu.HeadSlotVal = 99
	fcu.StateAtBlockRootVal[submittedRoot] = submittedState
	fcu.StateAtBlockRootVal[selectedRoot] = selectedState
	a := &ApiHandler{forkchoiceStore: fcu}

	root, slot, headState, err := a.selectedHeadState(submittedRoot)
	require.NoError(t, err)
	require.Equal(t, selectedRoot, root)
	require.Equal(t, uint64(99), slot)
	require.Same(t, selectedState, headState)
}

func TestSelectedHeadStateReportsMissingAuxiliaryRoot(t *testing.T) {
	auxiliaryRoot := common.Hash{0xaa}
	a := &ApiHandler{forkchoiceStore: mock_services2.NewForkChoiceStorageMock(t)}

	_, _, _, err := a.selectedHeadState(auxiliaryRoot)
	require.EqualError(t, err, "failed to get auxiliary state for root 0xaa00000000000000000000000000000000000000000000000000000000000000")
}

func TestSelectedHeadStateReportsMissingSelectedRoot(t *testing.T) {
	auxiliaryRoot := common.Hash{0xaa}
	selectedRoot := common.Hash{0xbb}
	fcu := mock_services2.NewForkChoiceStorageMock(t)
	fcu.HeadVal = selectedRoot
	fcu.StateAtBlockRootVal[auxiliaryRoot] = state.New(&clparams.MainnetBeaconConfig)
	a := &ApiHandler{forkchoiceStore: fcu}

	_, _, _, err := a.selectedHeadState(auxiliaryRoot)
	require.EqualError(t, err, "failed to get selected head state for root 0xbb00000000000000000000000000000000000000000000000000000000000000")
}

func TestMemoizedExpectedWithdrawalsMatchesExplicitHeadRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	headState := state.New(&cfg)
	headState.SetVersion(clparams.CapellaVersion)
	headRoot := common.Hash{0xaa}

	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateWithIdentityFn) error {
		return fn(headState, headRoot, 0)
	})
	a := &ApiHandler{
		beaconChainCfg:  &cfg,
		syncedData:      syncedData,
		forkchoiceStore: mock_services2.NewForkChoiceStorageMock(t),
	}

	response, matched, err := a.memoizedExpectedWithdrawals(&headRoot)
	require.NoError(t, err)
	require.True(t, matched)
	require.NotNil(t, response)
}

func TestViewHeadStateWithIdentityReportsSyncing(t *testing.T) {
	ctrl := gomock.NewController(t)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).Return(synced_data.ErrNotSynced)
	a := &ApiHandler{syncedData: syncedData}

	err := a.viewHeadStateWithIdentity(func(*state.CachingBeaconState, common.Hash, uint64) error { return nil })
	endpointErr, ok := err.(*beaconhttp.EndpointError)
	require.True(t, ok)
	require.Equal(t, http.StatusServiceUnavailable, endpointErr.Code)
}
