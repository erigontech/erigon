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
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// The sync gate compares the head with the highest imported block, not with the clock or with
// blocks merely seen, so neither a chain-wide gap nor a rejected block trips it, while a head
// that stopped advancing behind imported blocks does.
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

// get_head starts at the justified checkpoint, so a served head that neither is that block nor
// descends from it is stale. This holds after finalization pruned the head's child index.
func TestSyncGateRefusesAHeadOlderThanTheJustifiedCheckpoint(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	headRoot, headSlot, ok := handler.syncedData.StateHead()
	require.True(t, ok)
	nextEpoch := headSlot/handler.beaconChainCfg.SlotsPerEpoch + 1

	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: nextEpoch, Root: common.Hash{0x77}}
	require.True(t, handler.headLagsBehind())

	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: nextEpoch, Root: headRoot}
	require.False(t, handler.headLagsBehind(), "the justified block itself, at a skipped boundary slot")

	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: nextEpoch - 1, Root: common.Hash{0x77}}
	require.False(t, handler.headLagsBehind())
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

// Validator endpoints refuse to serve a head that trails the imported blocks, as block
// production does: data built on it cannot become canonical.
func TestValidatorEndpointsRefuseHeadBehindImportedBlocks(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodGet, "/eth/v1/validator/attestation_data?slot=1&committee_index=0", ""},
		{http.MethodGet, "/eth/v1/validator/payload_attestation_data/1", ""},
		{http.MethodPost, "/eth/v1/beacon/pool/attestations", "[]"},
		{http.MethodPost, "/eth/v2/beacon/pool/attestations", "[]"},
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
