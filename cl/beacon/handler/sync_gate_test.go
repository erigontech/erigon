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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// The sync gate compares the head with the highest imported block, not with the clock or with
// blocks merely seen, so neither a chain-wide gap nor a rejected block trips it, while a head
// that stopped advancing behind imported blocks does.
func TestSyncGateFollowsHighestImportedBlock(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	headSlot := postState.Slot()
	tolerance := handler.beaconChainCfg.SlotsPerEpoch

	// A block that was seen but never imported does not count.
	fcu.HighestSeenVal = headSlot + 10*tolerance

	fcu.HighestImportedVal = headSlot
	require.False(t, handler.headLagsBehind())
	fcu.HighestImportedVal = headSlot + tolerance
	require.False(t, handler.headLagsBehind())
	fcu.HighestImportedVal = headSlot + tolerance + 1
	require.True(t, handler.headLagsBehind())
}

func TestNodeSyncingReportsHeadBehindImportedBlocks(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.HighestImportedVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1

	resp, err := handler.GetEthV1NodeSyncing(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/node/syncing", http.NoBody))
	require.NoError(t, err)
	require.Equal(t, true, resp.Data.(map[string]any)["is_syncing"])
}

func TestNodeHealthReportsHeadBehindImportedBlocks(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.HighestImportedVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1

	recorder := httptest.NewRecorder()
	handler.GetEthV1NodeHealth(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/node/health?syncing_status=206", http.NoBody))
	require.Equal(t, http.StatusPartialContent, recorder.Code)
}

func TestBlockProductionRefusesHeadBehindImportedBlocks(t *testing.T) {
	_, _, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.HighestImportedVal = postState.Slot() + handler.beaconChainCfg.SlotsPerEpoch + 1
	targetSlot := postState.Slot() + 1

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf(
		"/eth/v3/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true", targetSlot, (common.Bytes96{}).String()), http.NoBody)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("slot", fmt.Sprint(targetSlot))
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))

	_, err := handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), request)
	var endpointErr *beaconhttp.EndpointError
	require.ErrorAs(t, err, &endpointErr)
	require.Equal(t, http.StatusServiceUnavailable, endpointErr.Code)
}

// An EMPTY head whose envelope is parked is an open decision, so production waits for it
// instead of building on the EMPTY variant right away. A parked copy of an envelope that is
// already persisted is stale and does not count.
func TestEnvelopeParkedFlagsOnlyAnUnpersistedParkedEnvelope(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	head := forkchoice.ForkChoiceNode{Root: common.Hash{1}, PayloadStatus: cltypes.PayloadStatusEmpty}

	require.False(t, handler.envelopeParked(head.Root))
	require.Equal(t, gloasPayloadPathEmpty, handler.gloasPayloadPathForHead(head, 10))
	fcu.PendingEnvelopeRoots = map[common.Hash]struct{}{head.Root: {}}
	require.True(t, handler.envelopeParked(head.Root))
	require.Equal(t, gloasPayloadPathEmpty, handler.gloasPayloadPathForHead(head, 10), "the path stays EMPTY so preparation primes the fallback")
	fcu.SetEnvelope(head.Root, &cltypes.SignedExecutionPayloadEnvelope{})
	require.False(t, handler.envelopeParked(head.Root))
}
