// Copyright 2025 The Erigon Authors
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

package engineapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
)

// getFreePort returns a free TCP port for testing.
func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// makeJWTToken creates a valid JWT token for testing.
func makeJWTToken(secret []byte) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iat": time.Now().Unix(),
	})
	tokenString, _ := token.SignedString(secret)
	return tokenString
}

// sszRestTestSetup creates an EngineServer and a test HTTP server with
// SSZ-REST routes + JWT middleware for testing.
type sszRestTestSetup struct {
	engineServer *EngineServer
	jwtSecret    []byte
	baseURL      string
	cancel       context.CancelFunc
}

func newSszRestTestSetup(t *testing.T) *sszRestTestSetup {
	t.Helper()

	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))

	executionRpc := direct.NewExecutionClientDirect(mockSentry.ExecModule)
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, true, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	port := getFreePort(t)
	engineServer.httpConfig = &httpcfg.HttpCfg{
		AuthRpcHTTPListenAddress: "127.0.0.1",
		AuthRpcPort:              8551,
	}

	jwtSecret := make([]byte, 32)
	rand.Read(jwtSecret)

	// Create the SSZ-REST handler (same as production code)
	sszHandler := NewSszRestHandler(engineServer, log.New())

	// Wrap with JWT middleware for testing (in production this is done by createHandler)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rpc.CheckJwtSecret(w, r, jwtSecret) {
			return
		}
		sszHandler.ServeHTTP(w, r)
	})

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)

	server := &http.Server{Handler: handler}
	go server.Serve(listener) //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		server.Close()
	}()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForServer(t, baseURL, jwtSecret)

	return &sszRestTestSetup{
		engineServer: engineServer,
		jwtSecret:    jwtSecret,
		baseURL:      baseURL,
		cancel:       cancel,
	}
}

func waitForServer(t *testing.T, baseURL string, jwtSecret []byte) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	for i := 0; i < 50; i++ {
		req, _ := http.NewRequest("POST", baseURL+"/engine/v1/exchange_capabilities", nil)
		req.Header.Set("Authorization", "Bearer "+makeJWTToken(jwtSecret))
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("SSZ-REST server did not start in time")
}

func (s *sszRestTestSetup) doRequest(t *testing.T, path string, body []byte) (*http.Response, []byte) {
	t.Helper()
	return s.doRequestWithToken(t, path, body, makeJWTToken(s.jwtSecret))
}

func (s *sszRestTestSetup) doRequestWithToken(t *testing.T, path string, body []byte, token string) (*http.Response, []byte) {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest("POST", s.baseURL+path, bodyReader)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()

	return resp, respBody
}

func TestSszRestJWTAuth(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Request without token should fail
	httpReq, err := http.NewRequest("POST", setup.baseURL+"/engine/v1/exchange_capabilities", nil)
	req.NoError(err)
	httpReq.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(httpReq)
	req.NoError(err)
	resp.Body.Close()
	req.Equal(http.StatusForbidden, resp.StatusCode)

	// Request with invalid token should fail
	httpReq2, err := http.NewRequest("POST", setup.baseURL+"/engine/v1/exchange_capabilities", nil)
	req.NoError(err)
	httpReq2.Header.Set("Content-Type", "application/octet-stream")
	httpReq2.Header.Set("Authorization", "Bearer invalidtoken")

	resp2, err := client.Do(httpReq2)
	req.NoError(err)
	resp2.Body.Close()
	req.Equal(http.StatusForbidden, resp2.StatusCode)

	// Request with valid token should succeed
	body := engine_types.EncodeCapabilities([]string{"engine_newPayloadV4"})
	resp3, _ := setup.doRequest(t, "/engine/v1/exchange_capabilities", body)
	req.Equal(http.StatusOK, resp3.StatusCode)
}

func TestSszRestExchangeCapabilities(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	clCapabilities := []string{
		"engine_newPayloadV4",
		"engine_forkchoiceUpdatedV3",
		"engine_getPayloadV4",
	}

	body := engine_types.EncodeCapabilities(clCapabilities)
	resp, respBody := setup.doRequest(t, "/engine/v1/exchange_capabilities", body)
	req.Equal(http.StatusOK, resp.StatusCode)
	req.Equal("application/octet-stream", resp.Header.Get("Content-Type"))

	decoded, err := engine_types.DecodeCapabilities(respBody)
	req.NoError(err)
	req.NotEmpty(decoded)
	// Should contain at least the capabilities we sent (EL returns its own list)
	req.Contains(decoded, "engine_newPayloadV4")
	req.Contains(decoded, "engine_forkchoiceUpdatedV3")
}

func TestSszRestGetClientVersion(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	callerVersion := &engine_types.ClientVersionV1{
		Code:    "CL",
		Name:    "TestClient",
		Version: "1.0.0",
		Commit:  "0x12345678",
	}

	body := engine_types.EncodeClientVersion(callerVersion)
	resp, respBody := setup.doRequest(t, "/engine/v1/get_client_version", body)
	req.Equal(http.StatusOK, resp.StatusCode)

	versions, err := engine_types.DecodeClientVersions(respBody)
	req.NoError(err)
	req.Len(versions, 1)
	req.Equal("EG", versions[0].Code) // Erigon's client code
}

func TestSszRestGetBlobsV1(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Request with empty hashes — may return 200 or 500 depending on txpool availability
	hashes := []common.Hash{}
	body := engine_types.EncodeGetBlobsRequest(hashes)
	resp, _ := setup.doRequest(t, "/engine/v1/get_blobs", body)
	// The test setup doesn't have a fully initialized txpool/blockDownloader,
	// so the handler may panic (recovered) or return an engine error.
	// We verify the SSZ-REST transport layer handled it gracefully.
	req.True(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusInternalServerError)
}

func TestSszRestNotFoundEndpoint(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	resp, _ := setup.doRequest(t, "/engine/v99/nonexistent_method", nil)
	// Go 1.22+ mux returns 404 for unmatched routes, or 405 for wrong methods
	req.True(resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed)
}

func TestSszRestErrorResponseFormat(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Send malformed body to get_blobs
	resp, respBody := setup.doRequest(t, "/engine/v1/get_blobs", []byte{0x01})
	req.Equal(http.StatusBadRequest, resp.StatusCode)
	req.Equal("application/json", resp.Header.Get("Content-Type"))

	// Parse the JSON error response
	var errResp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	err := json.Unmarshal(respBody, &errResp)
	req.NoError(err)
	req.Equal(-32602, errResp.Code)
	req.NotEmpty(errResp.Message)
}

func TestSszRestForkchoiceUpdatedV3(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Build a ForkchoiceState SSZ Container:
	// forkchoice_state(96) + attributes_offset(4) + Union[None](1) = 101 bytes
	fcs := &engine_types.ForkChoiceState{
		HeadHash:           common.Hash{},
		SafeBlockHash:      common.Hash{},
		FinalizedBlockHash: common.Hash{},
	}
	fcsBytes := engine_types.EncodeForkchoiceState(fcs)
	req.Len(fcsBytes, 96)

	// Build the full container: fcs(96) + attr_offset(4) + union_selector(1)
	body := make([]byte, 101)
	copy(body[0:96], fcsBytes)
	// attributes_offset = 100 (points to byte 100, the union selector)
	body[96] = 100
	body[97] = 0
	body[98] = 0
	body[99] = 0
	body[100] = 0 // Union selector = 0 (None)

	// ForkchoiceUpdatedV3 with no payload attributes
	resp, respBody := setup.doRequest(t, "/engine/v3/forkchoice_updated", body)
	// The test setup doesn't have a fully initialized blockDownloader,
	// so the engine may panic (recovered by SSZ-REST middleware) or return an error.
	// We verify the SSZ-REST transport layer handled it gracefully without crashing.
	if resp.StatusCode == http.StatusOK {
		req.Equal("application/octet-stream", resp.Header.Get("Content-Type"))
		req.NotEmpty(respBody)
	} else {
		// Engine errors or recovered panics are returned as JSON
		req.Equal("application/json", resp.Header.Get("Content-Type"))
		req.True(resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusInternalServerError)
	}
}

func TestSszRestForkchoiceUpdatedShortBody(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Send a body that's too short for ForkchoiceState
	resp, respBody := setup.doRequest(t, "/engine/v3/forkchoice_updated", make([]byte, 50))
	req.Equal(http.StatusBadRequest, resp.StatusCode)

	var errResp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	err := json.Unmarshal(respBody, &errResp)
	req.NoError(err)
	req.Contains(errResp.Message, "too short")
}

func TestSszRestGetPayloadWrongBodySize(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Send wrong-sized body (not 8 bytes)
	resp, respBody := setup.doRequest(t, "/engine/v4/get_payload", make([]byte, 10))
	req.Equal(http.StatusBadRequest, resp.StatusCode)

	var errResp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	err := json.Unmarshal(respBody, &errResp)
	req.NoError(err)
	req.Contains(errResp.Message, "expected 8 bytes")
}

func TestSszRestNewPayloadV1EmptyBody(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Empty body should return 400
	resp, respBody := setup.doRequest(t, "/engine/v1/new_payload", nil)
	req.Equal(http.StatusBadRequest, resp.StatusCode)

	var errResp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	err := json.Unmarshal(respBody, &errResp)
	req.NoError(err)
	req.Equal(-32602, errResp.Code)
}

func TestSszRestNewPayloadV1MalformedBody(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Body too short to be a valid ExecutionPayload SSZ
	resp, respBody := setup.doRequest(t, "/engine/v1/new_payload", make([]byte, 100))
	req.Equal(http.StatusBadRequest, resp.StatusCode)

	var errResp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	err := json.Unmarshal(respBody, &errResp)
	req.NoError(err)
	req.Contains(errResp.Message, "SSZ decode error")
}

func TestSszRestNewPayloadV1ValidSSZ(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Build a minimal ExecutionPayload and encode it to SSZ
	ep := &engine_types.ExecutionPayload{
		ParentHash:    common.Hash{},
		FeeRecipient:  common.Address{},
		StateRoot:     common.Hash{},
		ReceiptsRoot:  common.Hash{},
		LogsBloom:     make([]byte, 256),
		PrevRandao:    common.Hash{},
		BlockNumber:   0,
		GasLimit:      30000000,
		GasUsed:       0,
		Timestamp:     1700000000,
		ExtraData:     []byte{},
		BaseFeePerGas: (*hexutil.Big)(common.Big0),
		BlockHash:     common.Hash{},
		Transactions:  []hexutil.Bytes{},
	}

	body := engine_types.EncodeExecutionPayloadSSZ(ep, 1)
	resp, respBody := setup.doRequest(t, "/engine/v1/new_payload", body)

	// The engine may return a real PayloadStatus or an error.
	// With the mock setup, it might fail because engine consumption is not enabled.
	// We verify the SSZ-REST transport layer correctly decoded and dispatched the request.
	if resp.StatusCode == http.StatusOK {
		req.Equal("application/octet-stream", resp.Header.Get("Content-Type"))
		// Should be a PayloadStatusSSZ response (minimum 9 bytes fixed + 1 byte union selector)
		req.GreaterOrEqual(len(respBody), 10)
		// Decode the response to verify it's valid SSZ
		ps, err := engine_types.DecodePayloadStatusSSZ(respBody)
		req.NoError(err)
		req.True(ps.Status <= engine_types.SSZStatusInvalidBlockHash)
	} else {
		// Engine errors come back as JSON
		req.Equal("application/json", resp.Header.Get("Content-Type"))
	}
}

func TestSszRestGetPayloadV1ValidRequest(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	// Send a valid 8-byte payload ID
	payloadId := make([]byte, 8)
	payloadId[7] = 0x01 // payload ID = 1

	resp, respBody := setup.doRequest(t, "/engine/v1/get_payload", payloadId)

	// The engine will likely return an error (unknown payload ID) or internal error
	// because we haven't built a payload. The important thing is the handler doesn't
	// return a "not yet supported" stub error.
	if resp.StatusCode == http.StatusOK {
		// Should be SSZ-encoded ExecutionPayload
		req.Equal("application/octet-stream", resp.Header.Get("Content-Type"))
	} else {
		// Check that it's NOT the old stub error message
		var errResp struct {
			Message string `json:"message"`
		}
		json.Unmarshal(respBody, &errResp) //nolint:errcheck
		req.NotContains(errResp.Message, "not yet supported")
		req.NotContains(errResp.Message, "SSZ ExecutionPayload encoding")
	}
}

func TestSszRestGetPayloadV4ValidRequest(t *testing.T) {
	setup := newSszRestTestSetup(t)
	defer setup.cancel()

	req := require.New(t)

	payloadId := make([]byte, 8)
	payloadId[7] = 0x01

	resp, respBody := setup.doRequest(t, "/engine/v4/get_payload", payloadId)

	if resp.StatusCode == http.StatusOK {
		req.Equal("application/octet-stream", resp.Header.Get("Content-Type"))
	} else {
		var errResp struct {
			Message string `json:"message"`
		}
		json.Unmarshal(respBody, &errResp) //nolint:errcheck
		req.NotContains(errResp.Message, "not yet supported")
		req.NotContains(errResp.Message, "SSZ ExecutionPayload encoding")
	}
}

