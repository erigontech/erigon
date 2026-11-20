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
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestGetValidatorBlockV4RouteAcceptsOnlyPost(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	handler.beaconChainCfg.InitializeForkSchedule()

	recorder := httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		"/eth/v4/validator/blocks/10?randao_reveal="+(common.Bytes96{}).String()+"&include_payload=true", http.NoBody))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Header().Get("Allow"), http.MethodPost)

	recorder = httptest.NewRecorder()
	handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodPost,
		"/eth/v4/validator/blocks/10?include_payload=true", strings.NewReader("{}")))
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestGetValidatorBlockRejectsGloasSlotsOnGetRoutes(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch, cfg.GloasForkEpoch = 0, 0, 0, 0
	handler.beaconChainCfg.InitializeForkSchedule()

	for _, path := range []string{"/eth/v3/validator/blocks/10", "/eth/v2/validator/blocks/10"} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.mux.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
				path+"?randao_reveal="+(common.Bytes96{}).String()+"&skip_randao_verification=true", http.NoBody))
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), `"code":400`)
			require.Contains(t, recorder.Body.String(), `"message"`)
			require.Contains(t, recorder.Body.String(), "Gloas")
		})
	}
}

func v3BlockRequest(t *testing.T, slot uint64) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf(
		"/eth/v3/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true", slot, (common.Bytes96{}).String()), http.NoBody)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("slot", fmt.Sprint(slot))
	return request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
}

func TestGetEthV3ValidatorBlockGloasForkBoundary(t *testing.T) {
	const forkEpoch = 10
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch, cfg.GloasForkEpoch = 0, forkEpoch
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch = 0, 0
	firstGloasSlot := uint64(forkEpoch) * cfg.SlotsPerEpoch
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	handler := &ApiHandler{ethClock: clock, beaconChainCfg: &cfg}

	for _, tc := range []struct {
		name     string
		slot     uint64
		rejected bool
	}{
		{"last pre-Gloas slot", firstGloasSlot - 1, false},
		{"first Gloas slot", firstGloasSlot, true},
		{"later Gloas slot", firstGloasSlot + 5*cfg.SlotsPerEpoch, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), v3BlockRequest(t, tc.slot))
			var endpointErr *beaconhttp.EndpointError
			require.ErrorAs(t, err, &endpointErr)
			require.Equal(t, http.StatusBadRequest, endpointErr.Code)
			if tc.rejected {
				require.ErrorContains(t, err, "Gloas")
			} else {
				require.ErrorContains(t, err, "after the next slot")
			}
		})
	}
}

func TestGetEthV3ValidatorBlockRejectsFirstGloasSlotOnAPreGloasHead(t *testing.T) {
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = 0, 0, 0
	handler.beaconChainCfg.GloasForkEpoch = state.Epoch(postState) + 1
	handler.beaconChainCfg.InitializeForkSchedule()
	firstGloasSlot := handler.beaconChainCfg.GloasForkEpoch * handler.beaconChainCfg.SlotsPerEpoch
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(firstGloasSlot).AnyTimes()
	handler.ethClock = clock

	_, err := handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), v3BlockRequest(t, firstGloasSlot))
	var endpointErr *beaconhttp.EndpointError
	require.ErrorAs(t, err, &endpointErr)
	require.Equal(t, http.StatusBadRequest, endpointErr.Code)
	require.ErrorContains(t, err, "Gloas")
}

func TestGetEthV3ValidatorBlockUsesFirstFutureForkEpochForGloasGate(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch = 0, 0
	cfg.FuluForkEpoch, cfg.GloasForkEpoch = 2, 1
	require.Less(t, cfg.GetCurrentStateVersion(1), clparams.GloasVersion)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	handler := &ApiHandler{ethClock: clock, beaconChainCfg: &cfg}

	_, err := handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), v3BlockRequest(t, cfg.SlotsPerEpoch))
	require.ErrorContains(t, err, "after the next slot")
}

func v4RequestBody(minBid, boostFactor uint64, builders ...string) string {
	return fmt.Sprintf(`{"min_bid":"%d","builder_boost_factor":"%d","builders":[%s]}`, minBid, boostFactor, strings.Join(builders, ","))
}

func postV4BlockRequest(ctx context.Context, slot uint64, includePayload bool, query, body string) *http.Request {
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf(
		"/eth/v4/validator/blocks/%d?randao_reveal=%s&skip_randao_verification=true&include_payload=%t%s",
		slot, (common.Bytes96{}).String(), includePayload, query), strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("slot", fmt.Sprint(slot))
	return request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
}
