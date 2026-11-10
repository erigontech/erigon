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

package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

type countingEthAPI struct{ calls atomic.Int32 }

func (a *countingEthAPI) GetBlockByNumber(context.Context, rpc.BlockNumber, bool) (*ethapi.RPCBlock, error) {
	a.calls.Add(1)
	return &ethapi.RPCBlock{}, nil
}

func (a *countingEthAPI) Syncing(context.Context) (any, error) {
	a.calls.Add(1)
	return false, nil
}

// TestCreateHandlerAuthenticatesHealth pins that a handler with a JWT secret answers
// /health only to callers with a valid token.
func TestCreateHandlerAuthenticatesHealth(t *testing.T) {
	secret := make([]byte, 32)
	serveHealth := func(t *testing.T, authorization string) (int, int32) {
		eth := &countingEthAPI{}
		httpHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
		handler, err := createHandler(&httpcfg.HttpCfg{}, []rpc.API{{Namespace: "eth", Service: eth}}, httpHandler, nil, nil, secret)
		require.NoError(t, err)
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/health", strings.NewReader(`{"known_block": 1}`))
		if authorization != "" {
			r.Header.Set("Authorization", authorization)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code, eth.calls.Load()
	}

	t.Run("without token", func(t *testing.T) {
		code, calls := serveHealth(t, "")
		require.Equal(t, http.StatusForbidden, code)
		require.Zero(t, calls)
	})
	t.Run("with token", func(t *testing.T) {
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iat": time.Now().Unix()}).SignedString(secret)
		require.NoError(t, err)
		code, calls := serveHealth(t, "Bearer "+token)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, int32(1), calls)
	})
}
