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
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	builder_mock "github.com/erigontech/erigon/cl/beacon/builder/mock_services"
)

func TestBuilderRoutes(t *testing.T) {
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/eth/v1/builder/states/head/expected_withdrawals"},
		{http.MethodPost, "/eth/v1/beacon/blinded_blocks"},
		{http.MethodPost, "/eth/v2/beacon/blinded_blocks"},
		{http.MethodPost, "/eth/v1/validator/register_validator"},
	}
	for _, tc := range []struct {
		name            string
		builderEnabled  bool
		relayConfigured bool
		want            []bool
	}{
		{name: "builder namespace without relay", builderEnabled: true, want: []bool{true, false, false, false}},
		{name: "builder namespace with relay", builderEnabled: true, relayConfigured: true, want: []bool{true, true, true, true}},
		{name: "builder namespace disabled", relayConfigured: true, want: []bool{false, false, false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := builder_mock.NewMockBuilderClient(gomock.NewController(t))
			client.EXPECT().RelayConfigured().Return(tc.relayConfigured).AnyTimes()
			handler := &ApiHandler{
				routerCfg: &beacon_router_configuration.RouterConfiguration{
					Beacon:    true,
					Builder:   tc.builderEnabled,
					Validator: true,
				},
				builderClient: client,
			}
			handler.Init()
			for i, route := range routes {
				matched := handler.mux.Match(chi.NewRouteContext(), route.method, route.path)
				require.Equal(t, tc.want[i], matched, route.path)
			}
		})
	}
}
