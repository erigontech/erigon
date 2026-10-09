// Copyright 2024 The Erigon Authors
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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/golang-jwt/jwt/v4"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// TestIsWebsocket tests if an incoming websocket upgrade request is detected properly.
func TestIsWebsocket(t *testing.T) {
	r, _ := http.NewRequestWithContext(t.Context(), "GET", "/", nil)

	require.False(t, isWebsocket(r))
	r.Header.Set("upgrade", "websocket")
	require.False(t, isWebsocket(r))
	r.Header.Set("connection", "upgrade")
	require.True(t, isWebsocket(r))
	r.Header.Set("connection", "upgrade,keep-alive")
	require.True(t, isWebsocket(r))
	r.Header.Set("connection", " UPGRADE,keep-alive")
	require.True(t, isWebsocket(r))
}

func TestParseSocketUrl(t *testing.T) {
	t.Run("sock", func(t *testing.T) {
		socketUrl, err := url.Parse("unix:///some/file/path.sock")
		require.NoError(t, err)
		require.Equal(t, "/some/file/path.sock", socketUrl.Host+socketUrl.EscapedPath())
	})
	t.Run("sock", func(t *testing.T) {
		socketUrl, err := url.Parse("tcp://localhost:1234")
		require.NoError(t, err)
		require.Equal(t, "localhost:1234", socketUrl.Host+socketUrl.EscapedPath())
	})
}

// TestRemoteRulesEngineFinalizeDelegates guards the BAL-regeneration path on a
// datadir-less rpcdaemon: block replay runs Initialize and Finalize on the
// remote engine wrapper, so Finalize must delegate rather than panic.
func TestRemoteRulesEngineFinalizeDelegates(t *testing.T) {
	e := &remoteRulesEngine{engine: merge.New(ethash.NewFaker())}
	header := &types.Header{Number: *uint256.NewInt(1)} // zero difficulty → PoS header
	require.NotPanics(t, func() {
		_, err := e.Finalize(&chain.Config{}, header, nil, nil, nil, nil, nil, nil, false, log.New())
		require.NoError(t, err)
	})
}

func TestRegularRpcServerGraphQLHostAndCORS(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	cfg := &httpcfg.HttpCfg{
		Enabled:           true,
		HttpServerEnabled: true,
		GraphQLEnabled:    true,
		HttpListenAddress: "127.0.0.1",
		HttpPort:          port,
		HttpListener:      ln,
		HttpCORSDomain:    []string{"https://dapp.example"},
		HttpVirtualHost:   []string{"localhost"},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- StartRpcServer(ctx, cfg, nil, log.New()) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	query := func(t *testing.T, host, origin string) (int, http.Header) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+ln.Addr().String()+"/graphql", strings.NewReader(`{"query":"{__typename}"}`))
		require.NoError(t, err)
		req.Host = host
		req.Header.Set("Content-Type", "application/json")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp.StatusCode, resp.Header
	}

	status, _ := query(t, "rebind.example", "")
	require.Equal(t, http.StatusForbidden, status)
	status, header := query(t, "localhost", "https://dapp.example")
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "https://dapp.example", header.Get("Access-Control-Allow-Origin"))
}

type subscribingService struct{}

func (subscribingService) Heads(ctx context.Context) (*rpc.Subscription, error) {
	notifier, ok := rpc.NotifierFromContext(ctx)
	if !ok {
		return nil, rpc.ErrNotificationsUnsupported
	}
	return notifier.CreateSubscription(), nil
}

// The authenticated endpoint applies the configured subscription limit as the regular one does.
func TestAuthenticatedRpcServerAppliesSubscriptionLimit(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := &httpcfg.HttpCfg{
		WebsocketEnabled:  true,
		AuthRpcListener:   ln,
		JWTSecretPath:     filepath.Join(t.TempDir(), "jwt.hex"),
		SubscriptionLimit: 1,
	}
	logger := log.New()
	listener, engineSrv, _, err := createEngineListener(cfg, []rpc.API{{Namespace: "test", Service: subscribingService{}}}, logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		engineSrv.Stop()
		_ = listener.Close()
	})
	jwtSecret, err := ObtainJWTSecret(cfg, logger)
	require.NoError(t, err)
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iat": time.Now().Unix()}).SignedString(jwtSecret)
	require.NoError(t, err)

	conn, resp, err := websocket.Dial(t.Context(), "ws://"+ln.Addr().String(), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
	})
	if err != nil && resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })

	subscribe := func(id int) *struct{ Code int } {
		t.Helper()
		request := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"test_subscribe","params":["heads"]}`, id)
		require.NoError(t, conn.Write(t.Context(), websocket.MessageText, []byte(request)))
		_, data, err := conn.Read(t.Context())
		require.NoError(t, err)
		var answer struct {
			Error *struct{ Code int } `json:"error"`
		}
		require.NoError(t, json.Unmarshal(data, &answer))
		return answer.Error
	}
	require.Nil(t, subscribe(1))
	second := subscribe(2)
	require.NotNil(t, second, "second subscription accepted with a limit of 1")
	require.Equal(t, rpc.ErrCodeServerOverloaded, second.Code)
}
