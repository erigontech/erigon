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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/nodecfg"
)

func TestRpcServerFlags_Defaults(t *testing.T) {
	cfg := buildHttpCfg(t, nil)

	require.Empty(t, cfg.Http.HttpURL)
	require.False(t, cfg.Http.SocketServerEnabled)
	require.Equal(t, "unix:///var/run/erigon.sock", cfg.Http.SocketListenUrl)
	require.False(t, cfg.Http.HttpsServerEnabled)
	require.Equal(t, nodecfg.DefaultHTTPHost, cfg.Http.HttpsListenAddress)
	require.Equal(t, 0, cfg.Http.HttpsPort)
	require.Empty(t, cfg.Http.HttpsURL)
	require.Empty(t, cfg.Http.HttpsCertfile)
	require.Empty(t, cfg.Http.HttpsKeyFile)
}

func TestRpcServerFlags_Set(t *testing.T) {
	cfg := buildHttpCfg(t, []string{
		"--http.url", "unix:///var/run/erigon-rpc.sock",
		"--socket.enabled",
		"--socket.url", "tcp://127.0.0.1:7777",
		"--https.enabled",
		"--https.addr", "0.0.0.0",
		"--https.port", "8443",
		"--https.url", "tcp://0.0.0.0:8443",
		"--https.cert", "/path/to/cert.pem",
		"--https.key", "/path/to/key.pem",
	})

	require.Equal(t, "unix:///var/run/erigon-rpc.sock", cfg.Http.HttpURL)
	require.True(t, cfg.Http.SocketServerEnabled)
	require.Equal(t, "tcp://127.0.0.1:7777", cfg.Http.SocketListenUrl)
	require.True(t, cfg.Http.HttpsServerEnabled)
	require.Equal(t, "0.0.0.0", cfg.Http.HttpsListenAddress)
	require.Equal(t, 8443, cfg.Http.HttpsPort)
	require.Equal(t, "tcp://0.0.0.0:8443", cfg.Http.HttpsURL)
	require.Equal(t, "/path/to/cert.pem", cfg.Http.HttpsCertfile)
	require.Equal(t, "/path/to/key.pem", cfg.Http.HttpsKeyFile)
}
