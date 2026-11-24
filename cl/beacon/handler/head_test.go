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
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	mock_services2 "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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

			response, err := a.GetEthV2DebugBeaconHeads(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v2/debug/beacon/heads", nil))
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

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/blocks/head/root", nil)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("block_id", "head")
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
	blockID, err := beaconhttp.BlockIdFromRequest(request)
	require.NoError(t, err)

	root, err := a.rootFromBlockId(nil, blockID)
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
	require.NoError(t, selectedState.SetSlot(98))
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
	require.NotSame(t, selectedState, headState)
	require.Equal(t, uint64(98), headState.Slot())
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
	var endpointErr *beaconhttp.EndpointError
	require.ErrorAs(t, err, &endpointErr)
	require.Equal(t, http.StatusServiceUnavailable, endpointErr.Code)
}

func requestHeadRoot(t *testing.T, handler *ApiHandler) (int, common.Hash) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/blocks/head/root", http.NoBody))
	if recorder.Code != http.StatusOK {
		t.Logf("head root response: %s", recorder.Body.String())
		return recorder.Code, common.Hash{}
	}
	var response struct {
		Data struct {
			Root common.Hash `json:"root"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	return recorder.Code, response.Data.Root
}

// headImport makes the fork choice mock behave like the store: the head moves to the imported block once its
// import ends, and every head computation publishes the selected head.
type headImport struct {
	imported             atomic.Bool
	parent, importedRoot common.Hash
	parentSlot, slot     uint64
}

func (h *headImport) attach(fcu *mock_services2.ForkChoiceStorageMock, syncedData *synced_data.SyncedDataManager) {
	fcu.TryGetHeadNodeFn = func() (forkchoice.ForkChoiceNode, bool) {
		root, slot := h.parent, h.parentSlot
		if h.imported.Load() {
			root, slot = h.importedRoot, h.slot
		}
		syncedData.OnSelectedHead(root, slot)
		return forkchoice.ForkChoiceNode{Root: root}, true
	}
}

func TestHeadRootWaitsForTheImportOfTheCurrentSlotBlock(t *testing.T) {
	_, blocks, _, _, _, handler, _, sd, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	syncedData := sd.(*synced_data.SyncedDataManager)
	handler.enableMemoizedHeadState = true
	parent, block := blocks[len(blocks)-2].Block, blocks[len(blocks)-1].Block
	parentRoot, err := parent.HashSSZ()
	require.NoError(t, err)
	blockRoot, err := block.HashSSZ()
	require.NoError(t, err)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(block.Slot).AnyTimes()
	handler.ethClock = clock
	handler.attestationDataImportWait = time.Second
	fcu.HighestSeenVal = parent.Slot
	h := &headImport{parent: common.Hash(parentRoot), importedRoot: common.Hash(blockRoot), parentSlot: parent.Slot, slot: block.Slot}
	h.attach(fcu, syncedData)
	syncedData.OnSelectedHead(common.Hash(parentRoot), parent.Slot)

	finishImport := fcu.BeginPayloadImport(block.Slot)
	time.AfterFunc(50*time.Millisecond, func() {
		h.imported.Store(true)
		finishImport()
	})

	status, root := requestHeadRoot(t, handler)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, common.Hash(blockRoot), root, "the head root waits for the block whose import is in progress")
}

func TestHeadRootServesAnImportedBlockBeforeItsIndicesAreWritten(t *testing.T) {
	_, blocks, _, _, _, handler, _, sd, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	syncedData := sd.(*synced_data.SyncedDataManager)
	handler.enableMemoizedHeadState = true
	parent := blocks[len(blocks)-1].Block
	parentRoot, err := parent.HashSSZ()
	require.NoError(t, err)
	importedRoot, importedSlot := common.Hash{0x1d}, parent.Slot+1
	fcu.Headers[importedRoot] = &cltypes.BeaconBlockHeader{Slot: importedSlot, ParentRoot: parentRoot}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(importedSlot).AnyTimes()
	handler.ethClock = clock
	h := &headImport{parent: common.Hash(parentRoot), importedRoot: importedRoot, parentSlot: parent.Slot, slot: importedSlot}
	h.imported.Store(true)
	h.attach(fcu, syncedData)

	status, root := requestHeadRoot(t, handler)
	require.Equal(t, http.StatusOK, status, "fork choice already holds the block")
	require.Equal(t, importedRoot, root)
}

// importedOnHeaderLookup lets an optimistic import land between the handler's first fork choice reads and its
// header lookup.
type importedOnHeaderLookup struct {
	*mock_services2.ForkChoiceStorageMock
	root     common.Hash
	header   *cltypes.BeaconBlockHeader
	imported atomic.Bool
}

func (f *importedOnHeaderLookup) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if root != f.root {
		return f.ForkChoiceStorageMock.GetHeader(root)
	}
	f.imported.Store(true)
	return f.header, true
}

func (f *importedOnHeaderLookup) IsRootOptimistic(root common.Hash) bool {
	return root == f.root && f.imported.Load()
}

func TestBlockRootReportsTheOptimisticStatusOfABlockFoundByItsHeader(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	parent := blocks[len(blocks)-1].Block
	root := common.Hash{0x0b}
	handler.forkchoiceStore = &importedOnHeaderLookup{ForkChoiceStorageMock: fcu, root: root, header: &cltypes.BeaconBlockHeader{Slot: parent.Slot + 1}}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/blocks/"+root.Hex()+"/root", http.NoBody))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response struct {
		ExecutionOptimistic bool `json:"execution_optimistic"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.True(t, response.ExecutionOptimistic, "an optimistically imported block is not reported as validated")
}
