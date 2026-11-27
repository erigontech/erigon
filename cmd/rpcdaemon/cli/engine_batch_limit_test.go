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
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common/log/v3"
)

// TestEngineListenerAppliesBatchLimit pins that --rpc.batch.limit also bounds batches on
// the engine port.
func TestEngineListenerAppliesBatchLimit(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &httpcfg.HttpCfg{
		AuthRpcListener:    ln,
		AuthRpcVirtualHost: []string{"localhost"},
		JWTSecretPath:      filepath.Join(t.TempDir(), "jwt.hex"),
		BatchLimit:         2,
	}
	server, engine, _, err := createEngineListener(cfg, nil, log.New())
	require.NoError(t, err)
	t.Cleanup(func() {
		engine.Stop()
		require.NoError(t, server.Close())
	})
	secret, err := ObtainJWTSecret(cfg, log.New())
	require.NoError(t, err)
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iat": time.Now().Unix()}).SignedString(secret)
	require.NoError(t, err)

	call := `{"jsonrpc":"2.0","id":1,"method":"rpc_modules"}`
	batch := "[" + strings.Repeat(call+",", 2) + call + "]"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://localhost/", strings.NewReader(batch))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
		}},
		Timeout: 5 * time.Second,
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Contains(t, string(got), "batch limit 2 exceeded")
}
