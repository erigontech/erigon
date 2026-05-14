// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package node

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// TestCorsHandler makes sure CORS are properly handled on the http server.
func TestCorsHandler(t *testing.T) {
	srv := createAndStartServer(t, &httpConfig{CorsAllowedOrigins: []string{"test", "test.com"}}, false, &wsConfig{})
	defer srv.stop()
	url := "http://" + srv.listenAddr()

	resp := rpcRequest(t, url, "origin", "test.com")
	defer resp.Body.Close()
	assert.Equal(t, "test.com", resp.Header.Get("Access-Control-Allow-Origin"))

	resp2 := rpcRequest(t, url, "origin", "bad")
	defer resp2.Body.Close()
	assert.Empty(t, resp2.Header.Get("Access-Control-Allow-Origin"))
}

// TestVhosts makes sure vhosts is properly handled on the http server.
func TestVhosts(t *testing.T) {
	srv := createAndStartServer(t, &httpConfig{Vhosts: []string{"test"}}, false, &wsConfig{})
	defer srv.stop()
	url := "http://" + srv.listenAddr()

	resp := rpcRequest(t, url, "host", "test")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := rpcRequest(t, url, "host", "bad")
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusForbidden, resp2.StatusCode)
}

// TestVhostsAny makes sure vhosts any is properly handled on the http server.
func TestVhostsAny(t *testing.T) {
	srv := createAndStartServer(t, &httpConfig{Vhosts: []string{"any"}}, false, &wsConfig{})
	defer srv.stop()
	url := "http://" + srv.listenAddr()

	resp := rpcRequest(t, url, "host", "test")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := rpcRequest(t, url, "host", "bad")
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

type originTest struct {
	spec    string
	expOk   []string
	expFail []string
}

// TestWebsocketOrigins makes sure the websocket origins are properly handled on the websocket server.
func TestWebsocketOrigins(t *testing.T) {
	tests := []originTest{
		{
			spec: "*", // allow all
			expOk: []string{"", "http://test", "https://test", "http://test:8540", "https://test:8540",
				"http://test.com", "https://foo.test", "http://testa", "http://atestb:8540", "https://atestb:8540"},
		},
		{
			spec:    "test",
			expOk:   []string{"http://test", "https://test", "http://test:8540", "https://test:8540"},
			expFail: []string{"http://test.com", "https://foo.test", "http://testa", "http://atestb:8540", "https://atestb:8540"},
		},
		// scheme tests
		{
			spec:  "https://test",
			expOk: []string{"https://test", "https://test:9999"},
			expFail: []string{
				"test",                                // no scheme, required by spec
				"http://test",                         // wrong scheme
				"http://test.foo", "https://a.test.x", // subdomain variatoins
				"http://testx:8540", "https://xtest:8540"},
		},
		// ip tests
		{
			spec:  "https://12.34.56.78",
			expOk: []string{"https://12.34.56.78", "https://12.34.56.78:8540"},
			expFail: []string{
				"http://12.34.56.78",     // wrong scheme
				"http://12.34.56.78:443", // wrong scheme
				"http://1.12.34.56.78",   // wrong 'domain name'
				"http://12.34.56.78.a",   // wrong 'domain name'
				"https://87.65.43.21", "http://87.65.43.21:8540", "https://87.65.43.21:8540"},
		},
		// port tests
		{
			spec:  "test:8540",
			expOk: []string{"http://test:8540", "https://test:8540"},
			expFail: []string{
				"http://test", "https://test", // spec says port required
				"http://test:8541", "https://test:8541", // wrong port
				"http://bad", "https://bad", "http://bad:8540", "https://bad:8540"},
		},
		// scheme and port
		{
			spec:  "https://test:8540",
			expOk: []string{"https://test:8540"},
			expFail: []string{
				"https://test",                          // missing port
				"http://test",                           // missing port, + wrong scheme
				"http://test:8540",                      // wrong scheme
				"http://test:8541", "https://test:8541", // wrong port
				"http://bad", "https://bad", "http://bad:8540", "https://bad:8540"},
		},
		// several allowed origins
		{
			spec: "localhost,http://127.0.0.1",
			expOk: []string{"localhost", "http://localhost", "https://localhost:8443",
				"http://127.0.0.1", "http://127.0.0.1:8080"},
			expFail: []string{
				"https://127.0.0.1", // wrong scheme
				"http://bad", "https://bad", "http://bad:8540", "https://bad:8540"},
		},
	}
	for _, tc := range tests {
		srv := createAndStartServer(t, &httpConfig{}, true, &wsConfig{Origins: common.CliString2Array(tc.spec)})
		url := fmt.Sprintf("ws://%v", srv.listenAddr())
		for _, origin := range tc.expOk {
			if err := wsRequest(t, url, origin); err != nil {
				t.Errorf("spec '%v', origin '%v': expected ok, got %v", tc.spec, origin, err)
			}
		}
		for _, origin := range tc.expFail {
			if err := wsRequest(t, url, origin); err == nil {
				t.Errorf("spec '%v', origin '%v': expected not to allow,  got ok", tc.spec, origin)
			}
		}
		srv.stop()
	}
}

// TestIsWebsocket tests if an incoming websocket upgrade request is handled properly.
func TestIsWebsocket(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)

	assert.False(t, isWebsocket(r))
	r.Header.Set("upgrade", "websocket")
	assert.False(t, isWebsocket(r))
	r.Header.Set("connection", "upgrade")
	assert.True(t, isWebsocket(r))
	r.Header.Set("connection", "upgrade,keep-alive")
	assert.True(t, isWebsocket(r))
	r.Header.Set("connection", " UPGRADE,keep-alive")
	assert.True(t, isWebsocket(r))
}

func Test_checkPath(t *testing.T) {
	tests := []struct {
		req      *http.Request
		prefix   string
		expected bool
	}{
		{
			req:      &http.Request{URL: &url.URL{Path: "/test"}},
			prefix:   "/test",
			expected: true,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: "/testing"}},
			prefix:   "/test",
			expected: true,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: "/"}},
			prefix:   "/test",
			expected: false,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: "/fail"}},
			prefix:   "/test",
			expected: false,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: "/"}},
			prefix:   "",
			expected: true,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: "/fail"}},
			prefix:   "",
			expected: false,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: "/"}},
			prefix:   "/",
			expected: true,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: "/testing"}},
			prefix:   "/",
			expected: true,
		},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, tt.expected, checkPath(tt.req, tt.prefix))
		})
	}
}

func createAndStartServer(t *testing.T, conf *httpConfig, ws bool, wsConf *wsConfig) *httpServer {
	t.Helper()

	// Ensure RpcConcurrencyLimit is always set so admission control wiring is exercised.
	// A high value avoids interfering with existing tests while still activating the path.
	if conf.RpcConcurrencyLimit == 0 {
		conf.RpcConcurrencyLimit = 1000
	}
	srv := newHTTPServer(testlog.Logger(t, log.LvlError), rpccfg.DefaultHTTPTimeouts)
	require.NoError(t, srv.enableRPC(nil, *conf, nil))
	if ws {
		require.NoError(t, srv.enableWS(nil, *wsConf, nil))
	}
	require.NoError(t, srv.setListenAddr("localhost", 0))
	require.NoError(t, srv.start())
	return srv
}

func createAndStartServerWithAllowList(t *testing.T, conf httpConfig, ws bool, wsConf wsConfig) *httpServer {
	t.Helper()

	srv := newHTTPServer(testlog.Logger(t, log.LvlError), rpccfg.DefaultHTTPTimeouts)

	allowList := rpc.AllowList(map[string]struct{}{"net_version": {}}) //don't allow RPC modules

	require.NoError(t, srv.enableRPC(nil, conf, allowList))
	if ws {
		require.NoError(t, srv.enableWS(nil, wsConf, allowList))
	}
	require.NoError(t, srv.setListenAddr("localhost", 0))
	require.NoError(t, srv.start())
	return srv
}

// wsRequest attempts to open a WebSocket connection to the given URL.
func wsRequest(t *testing.T, url, browserOrigin string) error {
	t.Helper()
	t.Logf("checking WebSocket on %s (origin %q)", url, browserOrigin)

	headers := make(http.Header)
	if browserOrigin != "" {
		headers.Set("Origin", browserOrigin)
	}
	//nolint
	conn, resp, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return err
	}
	return conn.Close(websocket.StatusNormalClosure, "")
}

func TestAllowList(t *testing.T) {
	srv := createAndStartServerWithAllowList(t, httpConfig{}, false, wsConfig{})
	defer srv.stop()

	assert.False(t, testCustomRequest(t, srv, "rpc_modules"))
}

func testCustomRequest(t *testing.T, srv *httpServer, method string) bool {
	body := bytes.NewReader(fmt.Appendf(nil, `{"jsonrpc":"2.0","id":1,"method":"%s"}`, method))
	req, _ := http.NewRequest("POST", "http://"+srv.listenAddr(), body)
	req.Header.Set("content-type", "application/json")

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return !strings.Contains(string(respBody), "error")
}

// rpcRequest performs a JSON-RPC request to the given URL.
func rpcRequest(t *testing.T, url string, extraHeaders ...string) *http.Response {
	t.Helper()

	// Create the request.
	body := bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"rpc_modules","params":[]}`))
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		t.Fatal("could not create http request:", err)
	}
	req.Header.Set("content-type", "application/json")

	// Apply extra headers.
	if len(extraHeaders)%2 != 0 {
		panic("odd extraHeaders length")
	}
	for i := 0; i < len(extraHeaders); i += 2 {
		key, value := extraHeaders[i], extraHeaders[i+1]
		if strings.EqualFold(key, "host") {
			req.Host = value
		} else {
			req.Header.Set(key, value)
		}
	}

	// Perform the request.
	t.Logf("checking RPC/HTTP on %s %v", url, extraHeaders)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestHTTP2H2C(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	srv := createAndStartServer(t, &httpConfig{}, false, &wsConfig{})
	defer srv.stop()

	// Create an HTTP/2 cleartext client.
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: transport}

	body := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"rpc_modules","params":[]}`)
	resp, err := client.Post("http://"+srv.listenAddr(), "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Validate protocol
	assert.Equal(t, "HTTP/2.0", resp.Proto, "expected HTTP/2.0 protocol")

	// Validate status
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Validate response body
	result, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(result), "jsonrpc", "expected JSON-RPC response")
}

// TestRPCAdmissionHandler verifies that rpcAdmissionHandler correctly limits
// concurrent requests and returns HTTP 503 when the limit is exceeded.
func TestRPCAdmissionHandler(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("disabled when limit is zero", func(t *testing.T) {
		h := newRPCAdmissionHandler(0, okHandler)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("allows requests under the limit", func(t *testing.T) {
		h := newRPCAdmissionHandler(5, okHandler)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("returns 503 when limit is exceeded", func(t *testing.T) {
		// Use a gate channel to hold inflight requests open long enough to
		// trigger the limit.
		gate := make(chan struct{})
		blockingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-gate
			w.WriteHeader(http.StatusOK)
		})

		const limit = 2
		h := newRPCAdmissionHandler(limit, blockingHandler)

		var wg sync.WaitGroup
		for i := 0; i < limit; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
			}()
		}

		// Give goroutines time to enter the handler and increment the counter.
		// We busy-wait on the inflight counter rather than sleeping.
		admission := h.(*rpcAdmissionHandler)
		for admission.inflight.Load() < limit {
			// spin
		}

		// Now the limit is reached — next request must be rejected.
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		// Release the held requests.
		close(gate)
		wg.Wait()
	})
}

// TestWsConnectionLimit verifies that WsConnectionLimit rejects excess WebSocket
// connections with HTTP 503 and allows new ones once a slot is freed.
func TestWsConnectionLimit(t *testing.T) {
	const limit = 1
	srv := createAndStartServer(t, &httpConfig{}, true, &wsConfig{
		Origins:           []string{"*"},
		WsConnectionLimit: limit,
	})
	defer srv.stop()
	url := fmt.Sprintf("ws://%v", srv.listenAddr())

	// First connection should succeed.
	conn1, resp1, err := websocket.Dial(context.Background(), url, nil)
	if err != nil && resp1 != nil {
		resp1.Body.Close()
	}
	require.NoError(t, err, "first connection should succeed")

	// Wait until the server increments its counter.
	require.Eventually(t, func() bool { return srv.wsLimiter.count.Load() == 1 },
		5*time.Second, 5*time.Millisecond)

	// While first connection is open the second must be rejected.
	_, resp, err2 := websocket.Dial(context.Background(), url, nil)
	require.Error(t, err2, "second connection should be rejected")
	if resp != nil {
		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		resp.Body.Close()
	}

	// Close first connection and wait for the counter to drop.
	require.NoError(t, conn1.Close(websocket.StatusNormalClosure, ""))
	require.Eventually(t, func() bool { return srv.wsLimiter.count.Load() == 0 },
		5*time.Second, 5*time.Millisecond)

	// A new connection should now succeed.
	conn3, resp3, err := websocket.Dial(context.Background(), url, nil)
	if err != nil && resp3 != nil {
		resp3.Body.Close()
	}
	require.NoError(t, err, "connection after limit released should succeed")
	require.NoError(t, conn3.Close(websocket.StatusNormalClosure, ""))
}

// TestNewWSConnectionLimiter tests the standalone NewWSConnectionLimiter handler.
func TestNewWSConnectionLimiter(t *testing.T) {
	// limit=0: handler passes through without restriction.
	passthrough := NewWSConnectionLimiter(0, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rr0 := httptest.NewRecorder()
	passthrough.ServeHTTP(rr0, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, rr0.Code)

	// Build a limiter with limit=1.
	// Use a channel to keep "connections" open until the test releases them.
	hold := make(chan struct{})
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hold
		w.WriteHeader(http.StatusOK)
	})
	limiter := NewWSConnectionLimiter(1, slow)

	// First request: should be accepted (blocks on hold).
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		limiter.ServeHTTP(httptest.NewRecorder(), req)
	}()

	// Give the goroutine time to increment the counter.
	require.Eventually(t, func() bool {
		return limiter.(*wsConnectionLimiter).count.Load() == 1
	}, 2*time.Second, time.Millisecond)

	// Second request: should be rejected with 503.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	limiter.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)

	// Release the first "connection".
	close(hold)
	require.Eventually(t, func() bool {
		return limiter.(*wsConnectionLimiter).count.Load() == 0
	}, 2*time.Second, time.Millisecond)
}
