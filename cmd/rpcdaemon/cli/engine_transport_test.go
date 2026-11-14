// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
			cfg := &httpcfg.HttpCfg{
				AuthRpcListener:    ln,
				AuthRpcVirtualHost: []string{"localhost"},
				JWTSecretPath:      filepath.Join(t.TempDir(), "jwt.hex"),
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
			wrongSecret := bytes.Clone(secret)
			wrongSecret[0] ^= 0xff
			invalidToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iat": time.Now().Unix()}).SignedString(wrongSecret)
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
					const payloadPath = "/engine/v1/payloads"
					const getPayloadPath = payloadPath + "/0x0102030405060708"
					for _, tc := range []struct {
						name, method, path, token, detail string
					}{
						{"authorized", http.MethodPost, payloadPath, token, ""},
						{"missing token", http.MethodPost, payloadPath, "", "missing token"},
						{"invalid signature", http.MethodPost, payloadPath, invalidToken, "signature is invalid"},
						{"get payload missing token", http.MethodGet, getPayloadPath, "", "missing token"},
						{"get payload invalid signature", http.MethodGet, getPayloadPath, invalidToken, "signature is invalid"},
						{"JSON-RPC missing token", http.MethodPost, "/", "", "missing token"},
						{"JSON-RPC invalid signature", http.MethodPost, "/", invalidToken, "signature is invalid"},
						{"JSON-RPC authorized", http.MethodPost, "/", token, ""},
						{"authorized after failures", http.MethodPost, payloadPath, token, ""},
					} {
						t.Run(tc.name, func(t *testing.T) {
							requestBody, contentType := body, "application/octet-stream"
							if tc.path == "/" {
								requestBody = []byte(`{"jsonrpc":"2.0","id":1,"method":"rpc_modules"}`)
								contentType = "application/json"
							}
							if tc.method == http.MethodGet {
								requestBody = nil
							}
							req, err := http.NewRequestWithContext(t.Context(), tc.method, "http://localhost"+tc.path, bytes.NewReader(requestBody))
							require.NoError(t, err)
							req.Header.Set("Content-Type", contentType)
							req.Header.Set("Eth-Execution-Version", "amsterdam")
							if tc.token != "" {
								req.Header.Set("Authorization", "Bearer "+tc.token)
							}
							resp, err := client.Do(req)
							require.NoError(t, err)
							got, err := io.ReadAll(resp.Body)
							require.NoError(t, resp.Body.Close())
							require.NoError(t, err)
							require.Equal(t, protocol, resp.ProtoMajor)
							if tc.detail == "" {
								require.Equal(t, http.StatusOK, resp.StatusCode)
								require.Equal(t, contentType, resp.Header.Get("Content-Type"))
								if tc.path == "/" {
									require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":{"rpc":"1.0"}}`, string(got))
								} else {
									require.Equal(t, body, got)
								}
								return
							}
							require.Equal(t, http.StatusForbidden, resp.StatusCode)
							if tc.path == "/" {
								require.Empty(t, resp.Header.Get("Cache-Control"))
								require.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
								require.Equal(t, tc.detail+"\n", string(got))
								return
							}
							require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
							require.Equal(t, "application/problem+json", resp.Header.Get("Content-Type"))
							var problem map[string]string
							require.NoError(t, json.Unmarshal(got, &problem))
							require.Equal(t, map[string]string{
								"type":   "/engine-api/errors/authentication-error",
								"detail": tc.detail,
							}, problem)
						})
					}
				})
			}
		})
	}
}
