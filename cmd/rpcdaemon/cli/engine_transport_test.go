// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package cli

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestEngineRESTHTTPTransports(t *testing.T) {
	previousHandler := AuthenticatedEngineRESTHandler
	AuthenticatedEngineRESTHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, r.Body)
	})
	t.Cleanup(func() { AuthenticatedEngineRESTHandler = previousHandler })

	for _, network := range []string{"tcp", "unix"} {
		t.Run(network, func(t *testing.T) {
			address := "127.0.0.1:0"
			if network == "unix" {
				// t.TempDir() paths can exceed the Unix socket path limit on macOS.
				tempDir, err := os.MkdirTemp("", "engine-")
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, dir.RemoveAll(tempDir)) })
				address = filepath.Join(tempDir, "rpc.sock")
			}
			ln, err := (&net.ListenConfig{}).Listen(t.Context(), network, address)
			require.NoError(t, err)
			cfg := &httpcfg.HttpCfg{AuthRpcListener: ln, JWTSecretPath: filepath.Join(t.TempDir(), "jwt.hex")}
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
			for _, protocol := range []int{1, 2} {
				t.Run("HTTP"+strconv.Itoa(protocol), func(t *testing.T) {
					transport := &http.Transport{
						Protocols: new(http.Protocols),
						DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
							return (&net.Dialer{}).DialContext(ctx, network, ln.Addr().String())
						},
					}
					transport.Protocols.SetHTTP1(protocol == 1)
					transport.Protocols.SetUnencryptedHTTP2(protocol == 2)
					t.Cleanup(transport.CloseIdleConnections)
					client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
					body := bytes.Repeat([]byte{0, 1, 0xff}, 400_000)
					for _, authorized := range []bool{true, false, true} {
						req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://localhost/engine/v1/payloads", bytes.NewReader(body))
						require.NoError(t, err)
						req.Header.Set("Content-Type", "application/octet-stream")
						req.Header.Set("Eth-Execution-Version", "amsterdam")
						if authorized {
							req.Header.Set("Authorization", "Bearer "+token)
						}
						resp, err := client.Do(req)
						require.NoError(t, err)
						got, err := io.ReadAll(resp.Body)
						require.NoError(t, resp.Body.Close())
						require.NoError(t, err)
						require.Equal(t, protocol, resp.ProtoMajor)
						if authorized {
							require.Equal(t, http.StatusOK, resp.StatusCode)
							require.Equal(t, body, got)
						} else {
							require.Equal(t, http.StatusForbidden, resp.StatusCode)
						}
					}
				})
			}
		})
	}
}
