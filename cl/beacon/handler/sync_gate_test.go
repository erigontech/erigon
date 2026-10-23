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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// The sync gate looks at blocks imported on top of the served head, not at the clock or at blocks
// merely seen, so neither a chain-wide gap nor a rejected block trips it, while a head that
// stopped advancing behind its imported descendants does.
func TestSyncGateFollowsBlocksImportedOnTheHead(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	headSlot := postState.Slot()
	tolerance := handler.beaconChainCfg.SlotsPerEpoch

	// A block that was seen but never imported does not count.
	fcu.HighestSeenVal = headSlot + 10*tolerance

	fcu.HighestImportedDescendantVal = headSlot
	require.False(t, handler.headLagsBehind())
	fcu.HighestImportedDescendantVal = headSlot + tolerance
	require.False(t, handler.headLagsBehind())
	fcu.HighestImportedDescendantVal = headSlot + tolerance + 1
	require.True(t, handler.headLagsBehind())
	require.ErrorContains(t, handler.refuseLaggingHead(), errHeadBehindForkChoice.Error())

	// The gate asks about the served head, not about every imported branch.
	headRoot, _, ok := handler.syncedData.StateHead()
	require.True(t, ok)
	var asked []common.Hash
	fcu.HasImportedDescendantAfterFn = func(root common.Hash, _ uint64) bool {
		asked = append(asked, root)
		return false
	}
	require.False(t, handler.headLagsBehind())
	require.Equal(t, []common.Hash{headRoot}, asked)
}

// get_head returns only descendants of the justified checkpoint, so a served head whose ancestor at
// the checkpoint is another block is stale: an older head, or one on a branch fork choice left. A
// checkpoint or a path the fork graph cannot resolve says nothing either way.
func TestSyncGateRefusesAHeadThatDoesNotDescendFromTheJustifiedCheckpoint(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	headRoot, headSlot, ok := handler.syncedData.StateHead()
	require.True(t, ok)
	justifiedRoot := common.Hash{0x77}
	epoch := headSlot / handler.beaconChainCfg.SlotsPerEpoch
	checkpointSlot := epoch * handler.beaconChainCfg.SlotsPerEpoch
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: justifiedRoot}
	fcu.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{justifiedRoot: {Slot: checkpointSlot}}
	// Each case is another fork graph, so the remembered answer of the previous one is dropped.
	lags := func() bool {
		handler.justifiedDescent.Store(nil)
		return handler.headLagsBehind()
	}

	fcu.Ancestors = map[uint64]forkchoice.ForkChoiceNode{checkpointSlot: {Root: justifiedRoot}}
	require.False(t, lags(), "a descendant of the justified block")

	fcu.Ancestors = map[uint64]forkchoice.ForkChoiceNode{checkpointSlot: {Root: common.Hash{0x88}}}
	require.True(t, lags(), "a head on a branch fork choice left")
	err := handler.refuseLaggingHead()
	require.ErrorContains(t, err, errHeadBehindForkChoice.Error())

	fcu.Ancestors = map[uint64]forkchoice.ForkChoiceNode{checkpointSlot: {Root: headRoot}}
	require.True(t, lags(), "a head older than the justified block")

	fcu.Ancestors = nil
	require.False(t, lags(), "an ancestry the fork graph cannot resolve")

	fcu.Ancestors = map[uint64]forkchoice.ForkChoiceNode{checkpointSlot: {Root: common.Hash{0x88}}}
	fcu.Headers = nil
	require.False(t, lags(), "a justified block the fork graph does not hold")
}

// The justified block can come before its epoch start, when the epoch's first slots are empty. A head
// that descends from it through a block at the epoch start is still a head get_head may select.
func TestSyncGateComparesAtTheJustifiedBlock(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	_, headSlot, ok := handler.syncedData.StateHead()
	require.True(t, ok)
	justifiedRoot := common.Hash{0x77}
	epoch := headSlot / handler.beaconChainCfg.SlotsPerEpoch
	checkpointSlot := epoch * handler.beaconChainCfg.SlotsPerEpoch
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: justifiedRoot}
	fcu.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{justifiedRoot: {Slot: checkpointSlot - 1}}
	fcu.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
		checkpointSlot - 1: {Root: justifiedRoot},
		checkpointSlot:     {Root: common.Hash{0x88}},
	}

	require.False(t, handler.headLagsBehind())
}

// A head below the justified block cannot descend from it, also once the fork graph pruned its path.
func TestSyncGateRefusesAHeadBelowTheJustifiedBlock(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	_, headSlot, ok := handler.syncedData.StateHead()
	require.True(t, ok)
	justifiedRoot := common.Hash{0x77}
	epoch := headSlot/handler.beaconChainCfg.SlotsPerEpoch + 2
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: justifiedRoot}
	fcu.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{justifiedRoot: {Slot: epoch * handler.beaconChainCfg.SlotsPerEpoch}}

	require.True(t, handler.headLagsBehind())
}

// The ancestry walk is remembered per served head and justified checkpoint once it resolves: the
// parent links between them do not change. An unresolved path is walked again.
func TestSyncGateRemembersTheResolvedJustifiedAncestry(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	_, headSlot, ok := handler.syncedData.StateHead()
	require.True(t, ok)
	justifiedRoot := common.Hash{0x77}
	epoch := headSlot / handler.beaconChainCfg.SlotsPerEpoch
	checkpointSlot := epoch * handler.beaconChainCfg.SlotsPerEpoch
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: justifiedRoot}
	fcu.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{justifiedRoot: {Slot: checkpointSlot}}

	require.False(t, handler.headLagsBehind())
	require.False(t, handler.headLagsBehind())
	require.Equal(t, int64(2), fcu.AncestorCalls.Load(), "an unresolved path is walked each time")

	fcu.Ancestors = map[uint64]forkchoice.ForkChoiceNode{checkpointSlot: {Root: justifiedRoot}}
	require.False(t, handler.headLagsBehind())
	require.False(t, handler.headLagsBehind())
	require.Equal(t, int64(3), fcu.AncestorCalls.Load())

	otherRoot := common.Hash{0x99}
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: otherRoot}
	fcu.Headers[otherRoot] = &cltypes.BeaconBlockHeader{Slot: checkpointSlot}
	require.True(t, handler.headLagsBehind(), "a new checkpoint is checked again")
	require.Equal(t, int64(4), fcu.AncestorCalls.Load())
}

func TestSyncGateReportsAMissingHeadStateAsNotSynced(t *testing.T) {
	_, _, _, _, _, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)
	syncedData.(*sync_mock_services.MockSyncedData).EXPECT().StateHead().Return(common.Hash{}, uint64(0), false)

	require.ErrorContains(t, handler.refuseLaggingHead(), synced_data.ErrNotSynced.Error())
}

func TestNodeSyncingReportsHeadBehindImportedBlocks(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.HighestImportedDescendantVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1

	resp, err := handler.GetEthV1NodeSyncing(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/node/syncing", http.NoBody))
	require.NoError(t, err)
	require.Equal(t, true, resp.Data.(map[string]any)["is_syncing"])
}

func TestNodeHealthReportsHeadBehindImportedBlocks(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.HighestImportedDescendantVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1

	recorder := httptest.NewRecorder()
	handler.GetEthV1NodeHealth(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/node/health?syncing_status=206", http.NoBody))
	require.Equal(t, http.StatusPartialContent, recorder.Code)
}

func TestBlockProductionRefusesHeadBehindImportedBlocks(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.HighestImportedDescendantVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1
	targetSlot := postState.Slot() + 1
	startBuilderHandoffWindow(handler, targetSlot)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("block production passed the gate")).AnyTimes()
	engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).Return(nil, errors.New("unavailable")).AnyTimes()
	handler.engine = engine

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf(
		"/eth/v3/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true", targetSlot, (common.Bytes96{}).String()), http.NoBody)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("slot", fmt.Sprint(targetSlot))
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))

	var err error
	require.NotPanics(t, func() { _, err = handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), request) })
	var endpointErr *beaconhttp.EndpointError
	require.ErrorAs(t, err, &endpointErr)
	require.Equal(t, http.StatusServiceUnavailable, endpointErr.Code)
}

func TestBlockProductionRefusesHistoricSlot(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	handler.beaconChainCfg.InitializeForkSchedule()
	targetSlot := postState.Slot() + 1
	currentSlot := targetSlot + 1
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	handler.ethClock = clock
	fcu.HighestImportedDescendantVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1

	requests := []struct {
		name, method, path, body string
		headers                  map[string]string
	}{
		{
			name:   "v3 GET",
			method: http.MethodGet,
			path: fmt.Sprintf("/eth/v3/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true",
				targetSlot, (common.Bytes96{}).String()),
		},
		{
			name:   "v4 POST",
			method: http.MethodPost,
			path:   fmt.Sprintf("/eth/v4/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true&include_payload=true", targetSlot, (common.Bytes96{}).String()),
			body:   `{"min_bid":"0","builder_boost_factor":"100","builders":[]}`,
			headers: map[string]string{
				"Content-Type":          "application/json",
				"Eth-Consensus-Version": "gloas",
			},
		},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			httpRequest := httptest.NewRequestWithContext(t.Context(), request.method, request.path, strings.NewReader(request.body))
			for name, value := range request.headers {
				httpRequest.Header.Set(name, value)
			}
			recorder := httptest.NewRecorder()
			handler.mux.ServeHTTP(recorder, httpRequest)
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), "historic slot")
		})
	}

	for _, slot := range []uint64{currentSlot, currentSlot + 1} {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf(
			"/eth/v3/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true", slot, (common.Bytes96{}).String()), http.NoBody)
		recorder := httptest.NewRecorder()
		handler.mux.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
		require.NotContains(t, recorder.Body.String(), "historic slot")
	}
}

// Validator endpoints refuse to serve a head that trails the imported blocks, as block
// production does: data built on it cannot become canonical.
func TestValidatorEndpointsRefuseHeadBehindImportedBlocks(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodGet, "/eth/v1/validator/attestation_data?slot=1&committee_index=0", ""},
		{http.MethodGet, "/eth/v1/validator/payload_attestation_data/1", ""},
		{http.MethodPost, "/eth/v1/validator/duties/attester/0", `["1"]`},
		{http.MethodGet, "/eth/v1/validator/duties/proposer/0", ""},
		{http.MethodGet, "/eth/v2/validator/duties/proposer/0", ""},
		{http.MethodPost, "/eth/v1/validator/duties/sync/0", `["1"]`},
		{http.MethodPost, "/eth/v1/validator/duties/ptc/0", `["1"]`},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			fcu.HighestImportedDescendantVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1

			recorder := httptest.NewRecorder()
			require.NotPanics(t, func() {
				handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, strings.NewReader(tc.body)))
			})
			require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
		})
	}
}

// Submitted attestations are validated and published as on a synced node: they may come from
// another node's data, and the spec answers a submission only with 200, 400, 415 or 500.
func TestPoolAttestationsAreNotGatedByALaggingHead(t *testing.T) {
	for _, tc := range []struct {
		path, version string
		code          int
	}{
		{"/eth/v1/beacon/pool/attestations", "", http.StatusOK},
		{"/eth/v2/beacon/pool/attestations", "electra", http.StatusOK},
		{"/eth/v2/beacon/pool/attestations", "", http.StatusBadRequest},
	} {
		t.Run(tc.path+" "+tc.version, func(t *testing.T) {
			_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			fcu.HighestImportedDescendantVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1
			require.True(t, handler.headLagsBehind())

			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tc.path, strings.NewReader("[]"))
			if tc.version != "" {
				request.Header.Set("Eth-Consensus-Version", tc.version)
			}
			recorder := httptest.NewRecorder()
			handler.mux.ServeHTTP(recorder, request)
			require.Equal(t, tc.code, recorder.Code, recorder.Body.String())
		})
	}
}

// A malformed request is a 400 whatever the head: the parameters are checked before the gate.
func TestBlockProductionRejectsAnOutOfRangeBuilderBoostFactorOnALaggingHead(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.HighestImportedDescendantVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1
	targetSlot := postState.Slot() + 1

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf(
		"/eth/v3/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true&builder_boost_factor=18446744073709551616", targetSlot, (common.Bytes96{}).String()), http.NoBody)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("slot", fmt.Sprint(targetSlot))
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))

	_, err := handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), request)
	var endpointErr *beaconhttp.EndpointError
	require.ErrorAs(t, err, &endpointErr)
	require.Equal(t, http.StatusBadRequest, endpointErr.Code, endpointErr.Error())
}
