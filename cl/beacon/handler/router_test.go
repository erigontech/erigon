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

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
)

func TestBuilderRoutesWithoutRelay(t *testing.T) {
	t.Run("builder namespace enabled", func(t *testing.T) {
		handler := &ApiHandler{routerCfg: &beacon_router_configuration.RouterConfiguration{
			Beacon:           true,
			BuilderNamespace: true,
			Validator:        true,
		}}
		handler.Init()

		route := chi.NewRouteContext()
		require.True(t, handler.mux.Match(route, http.MethodGet, "/eth/v1/builder/states/head/expected_withdrawals"))
		for _, path := range []string{
			"/eth/v1/beacon/blinded_blocks",
			"/eth/v2/beacon/blinded_blocks",
			"/eth/v1/validator/register_validator",
		} {
			require.False(t, handler.mux.Match(chi.NewRouteContext(), http.MethodPost, path), path)
		}
	})

	t.Run("builder namespace disabled", func(t *testing.T) {
		handler := &ApiHandler{routerCfg: &beacon_router_configuration.RouterConfiguration{Beacon: true, Validator: true}}
		handler.Init()
		require.False(t, handler.mux.Match(chi.NewRouteContext(), http.MethodGet, "/eth/v1/builder/states/head/expected_withdrawals"))
	})
}
