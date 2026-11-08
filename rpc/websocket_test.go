// Copyright 2018 The go-ethereum Authors
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

package rpc

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestWebsocketClientHeaders(t *testing.T) {
	t.Parallel()

	endpoint, header, err := wsClientHeaders("wss://testuser:test-PASS_01@example.com:1234", "https://example.com")
	if err != nil {
		t.Fatalf("wsGetConfig failed: %s", err)
	}
	if endpoint != "wss://example.com:1234" {
		t.Fatal("User should have been stripped from the URL")
	}
	if header.Get("authorization") != "Basic dGVzdHVzZXI6dGVzdC1QQVNTXzAx" {
		t.Fatal("Basic auth header is incorrect")
	}
	if header.Get("origin") != "https://example.com" {
		t.Fatal("Origin not set")
	}
}

// This test checks that the server rejects connections from disallowed origins.
func TestWebsocketOriginCheck(t *testing.T) {
	t.Parallel()

	logger := log.New()
	var (
		srv     = newTestServer(logger)
		httpsrv = httptest.NewServer(srv.WebsocketHandler([]string{"http://example.com"}, nil, false, logger))
		wsURL   = "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	)
	defer srv.Stop()
	defer httpsrv.Close()

	client, err := DialWebsocket(context.Background(), wsURL, "http://ekzample.com", logger)
	if err == nil {
		client.Close()
		t.Fatal("no error for wrong origin")
	}
	var handshakeErr wsHandshakeError
	if !errors.As(err, &handshakeErr) {
		t.Fatalf("wrong error type %T: %v", err, err)
	}
	if handshakeErr.status != "403 Forbidden" {
		t.Fatalf("wrong HTTP status in error: %q", handshakeErr.status)
	}

	// Connections without origin header should work.
	client, err = DialWebsocket(context.Background(), wsURL, "", logger)
	if err != nil {
		t.Fatal("error for empty origin")
	}
	client.Close()
}

func TestWebsocketOriginCheckDoesNotLeakGoroutines(t *testing.T) {
	logger := log.New()
	validate := wsHandshakeValidator([]string{"http://localhost", "http://localhost:3000", "localhost"}, logger)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "http://localhost:3000")

	before := runtime.NumGoroutine()
	for range 128 {
		require.True(t, validate(req))
	}
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= before+8
	}, 5*time.Second, 10*time.Millisecond, "goroutines: before=%d now=%d", before, runtime.NumGoroutine())
}

// This test checks whether calls exceeding the request size limit are rejected.
func TestWebsocketLargeCall(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	logger := log.New()

	var (
		srv     = newTestServer(logger)
		httpsrv = httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
		wsURL   = "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	)
	defer srv.Stop()
	defer httpsrv.Close()

	client, clientErr := DialWebsocket(context.Background(), wsURL, "", logger)
	if clientErr != nil {
		t.Fatalf("can't dial: %v", clientErr)
	}
	defer client.Close()

	// This call sends slightly less than the limit and should work.
	var result echoResult
	arg := strings.Repeat("x", maxRequestContentLength-200)
	if err := client.Call(&result, "test_echo", arg, 1); err != nil {
		t.Fatalf("valid call didn't work: %v", err)
	}
	if result.String != arg {
		t.Fatal("wrong string echoed")
	}

	// This call sends twice the allowed size and shouldn't work.
	arg = strings.Repeat("x", maxRequestContentLength*2)
	if err := client.Call(&result, "test_echo", arg); err == nil {
		t.Fatal("no error for too large call")
	}
}

// TestWebsocketBatchKeepsRequestOrder checks the exact batch answer, because some clients
// match batch responses by position instead of by id.
func TestWebsocketBatchKeepsRequestOrder(t *testing.T) {
	t.Parallel()
	logger := log.New()

	srv := newTestServer(logger)
	defer srv.Stop()
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()

	batch := `[{"jsonrpc":"2.0","id":1,"method":"test_streamEcho","params":["one"]},` +
		`{"jsonrpc":"2.0","method":"test_echo","params":["notification",0]},` +
		`{"jsonrpc":"2.0","id":2,"method":"test_echo","params":["two",2]},` +
		`{"jsonrpc":"2.0","id":3,"method":"no_such_method"}]`
	if err := conn.Write(t.Context(), websocket.MessageText, []byte(batch)); err != nil {
		t.Fatal(err)
	}
	_, got, err := conn.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"jsonrpc":"2.0","id":1,"result":"one"},` +
		`{"jsonrpc":"2.0","id":2,"result":{"String":"two","Int":2,"Args":null}},` +
		`{"jsonrpc":"2.0","id":3,"error":{"code":-32601,"message":"the method no_such_method does not exist/is not available"}}]`
	if string(got) != want {
		t.Fatalf("batch answer:\n got  %s\n want %s", got, want)
	}
}

// This test checks that client handles WebSocket ping frames correctly.
func TestClientWebsocketPing(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	t.Parallel()
	logger := log.New()

	var (
		sendPing    = make(chan struct{})
		server      = wsPingTestServer(t, sendPing)
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	)
	defer cancel()
	defer func() { _ = server.Shutdown(ctx) }()

	client, err := DialContext(ctx, "ws://"+server.Addr, logger)
	if err != nil {
		t.Fatalf("client dial error: %v", err)
	}
	resultChan := make(chan int)
	sub, err := client.EthSubscribe(ctx, resultChan, "foo")
	if err != nil {
		t.Fatalf("client subscribe error: %v", err)
	}

	// Wait for the context's deadline to be reached before proceeding.
	// This is important for reproducing https://github.com/ethereum/go-ethereum/issues/19798
	<-ctx.Done()
	close(sendPing)

	// Wait for the subscription result.
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case err := <-sub.Err():
			t.Error("client subscription error:", err)
		case result := <-resultChan:
			t.Log("client got result:", result)
			return
		case <-timeout.C:
			t.Error("didn't get any result within the test timeout")
			return
		}
	}
}

// This checks that the websocket transport can deal with large messages.
func TestClientWebsocketLargeMessage(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger := log.New()
	var (
		srv     = NewServer(50, false /* traceRequests */, false /* debugSingleRequests */, true, logger, 100)
		httpsrv = httptest.NewServer(srv.WebsocketHandler(nil, nil, false, logger))
		wsURL   = "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	)
	defer srv.Stop()
	defer httpsrv.Close()

	respLength := wsMessageSizeLimit - 50
	if err := srv.RegisterName("test", largeRespService{respLength}); err != nil {
		t.Fatal(err)
	}

	c, err := DialWebsocket(context.Background(), wsURL, "", logger)
	if err != nil {
		t.Fatal(err)
	}

	var r string
	if err := c.Call(&r, "test_largeResp"); err != nil {
		t.Fatal("call failed:", err)
	}
	if len(r) != respLength {
		t.Fatalf("response has wrong length %d, want %d", len(r), respLength)
	}
}

// wsPingTestServer runs a WebSocket server which accepts a single subscription request.
// When a value arrives on sendPing, the server sends a ping frame, waits for a matching
// pong and finally delivers a single subscription result.
func wsPingTestServer(t *testing.T, sendPing <-chan struct{}) *http.Server {
	var srv http.Server
	shutdown := make(chan struct{})
	srv.RegisterOnShutdown(func() {
		close(shutdown)
	})
	srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Upgrade to WebSocket.
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("server WS upgrade error: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()

		// Handle the connection.
		wsPingTestHandler(t, conn, shutdown, sendPing)
	})

	// Start the server.
	listener, err := net.Listen("tcp", "127.0.0.1:0") //nolint:noctx
	if err != nil {
		t.Fatal("can't listen:", err)
	}
	srv.Addr = listener.Addr().String()
	go func() { _ = srv.Serve(listener) }()
	return &srv
}

func wsPingTestHandler(t *testing.T, conn *websocket.Conn, shutdown, sendPing <-chan struct{}) {
	// Canned responses for the eth_subscribe call in TestClientWebsocketPing.
	const (
		subResp   = `{"jsonrpc":"2.0","id":1,"result":"0x00"}`
		subNotify = `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x00","result":1}}`
	)

	// Handle subscribe request.
	if _, _, err := conn.Read(context.Background()); err != nil {
		t.Errorf("server read error: %v", err)
		return
	}
	if err := conn.Write(context.Background(), websocket.MessageText, []byte(subResp)); err != nil {
		t.Errorf("server write error: %v", err)
		return
	}

	// coder/websocket requires a concurrent conn.Read() for conn.Ping() to work:
	// pong frames are only processed (and the waiting Ping() unblocked) when the
	// read pump is actively reading. Run a background goroutine for this purpose.
	readCtx, readCancel := context.WithCancel(context.Background())
	defer readCancel()
	go func() {
		for {
			typ, msg, err := conn.Read(readCtx)
			if err != nil {
				return
			}
			t.Logf("server got message (%s): %q", typ, msg)
		}
	}()

	// Write messages.
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C

	for {
		select {
		case _, open := <-sendPing:
			if !open {
				sendPing = nil
			}
			t.Logf("server sending ping")
			pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				t.Logf("server ping error: %v", err)
				return
			}
			t.Logf("server got pong")
			timer.Reset(200 * time.Millisecond)
		case <-timer.C:
			t.Logf("server sending response")
			conn.Write(context.Background(), websocket.MessageText, []byte(subNotify)) //nolint:errcheck
		case <-shutdown:
			return
		}
	}
}

// dbOverloadSvc simulates a service whose method returns ErrReadTxLimitExceeded.
// It also records whether the handler context had kv.NonBlockingAcquire set.
type dbOverloadSvc struct {
	sawNonBlocking atomic.Bool
}

func (s *dbOverloadSvc) Overload(ctx context.Context) error {
	s.sawNonBlocking.Store(kv.IsNonBlockingAcquire(ctx))
	return kv.ErrReadTxLimitExceeded
}

// TestWebsocketNonBlockingAcquire verifies two things about the WS handler:
//  1. The context passed to method handlers has kv.WithNonBlockingAcquire tagged,
//     so a real BeginRo would use TryAcquire instead of blocking.
//  2. When a handler returns kv.ErrReadTxLimitExceeded, remapDBOverload converts it
//     to JSON-RPC -32005 and the client receives that error code (no HTTP 503 is
//     possible on an already-upgraded WebSocket connection).
func TestWebsocketNonBlockingAcquire(t *testing.T) {
	t.Parallel()
	logger := log.New()

	svc := &dbOverloadSvc{}
	srv := NewServer(50, false, false, true, logger, 100)
	if err := srv.RegisterName("db", svc); err != nil {
		t.Fatal(err)
	}
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer srv.Stop()
	defer httpsrv.Close()

	wsURL := "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	client, err := DialWebsocket(context.Background(), wsURL, "", logger)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	var result any
	callErr := client.Call(&result, "db_overload")

	// The handler context must have NonBlockingAcquire set.
	if !svc.sawNonBlocking.Load() {
		t.Error("handler context did not have kv.NonBlockingAcquire tagged")
	}

	// The client must receive a JSON-RPC -32005 (not a nil error, not a generic error).
	if callErr == nil {
		t.Fatal("expected -32005 error but Call succeeded")
	}
	var rpcErr Error
	if !errors.As(callErr, &rpcErr) {
		t.Fatalf("expected rpc.Error, got %T: %v", callErr, callErr)
	}
	if rpcErr.ErrorCode() != ErrCodeServerOverloaded {
		t.Errorf("expected error code %d (server overloaded), got %d", ErrCodeServerOverloaded, rpcErr.ErrorCode())
	}
}

// TestWebsocketServerGracefulClose verifies that when the server initiates a
// websocket closure, it explicitly sends a clean StatusNormalClosure (1000)
// rather than an abnormal closure.
func TestWebsocketServerGracefulClose(t *testing.T) {
	t.Parallel()
	logger := log.New()

	srv := newTestServer(logger)
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	wsURL := "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	defer srv.Stop()
	defer httpsrv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()

	if err := conn.Write(ctx, websocket.MessageText, []byte("invalid json")); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	for {
		_, _, err := conn.Read(ctx)
		if err != nil {
			status := websocket.CloseStatus(err)
			if status != websocket.StatusNormalClosure {
				t.Errorf("expected close status %v, got %v (err: %v)", websocket.StatusNormalClosure, status, err)
			}
			break
		}
	}
}

// A peer that stops reading must not hold a writer forever: once the socket buffers fill, a
// write fails at the codec's write timeout, and the connection is closed rather than left
// with half a frame on the wire.
func TestWebsocketWriteTimeoutClosesStalledConn(t *testing.T) {
	t.Parallel()

	codecs := make(chan *websocketCodec, 1)
	httpsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hw := &hijackRecorder{ResponseWriter: w}
		conn, err := websocket.Accept(hw, r, nil)
		if err != nil {
			return
		}
		wc := newWebsocketCodec(conn, hw.conn, nil, r.Host, r.Header, r.RemoteAddr)
		defer wc.Close()
		codecs <- wc
		// A hijacked request's context never ends, so wait for the connection itself.
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()
	conn.SetReadLimit(-1)

	wc := <-codecs
	wc.writeTimeout = 200 * time.Millisecond
	payload := rawResponse(`"` + strings.Repeat("x", 1<<20) + `"`)
	failed := make(chan struct{})
	var writeErr error
	go func() {
		for writeErr == nil {
			writeErr = wc.WriteJSON(context.Background(), payload)
		}
		close(failed)
	}()
	select {
	case <-failed:
		// The socket deadline surfaces as an i/o timeout; a context deadline would not.
		if !errors.Is(writeErr, os.ErrDeadlineExceeded) {
			t.Fatalf("the stalled write failed with %v, want the socket deadline", writeErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("writes to a peer that does not read never failed")
	}

	closed := make(chan struct{})
	go func() {
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				close(closed)
				return
			}
		}
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection with the cut-off write was not closed")
	}
}

func TestWebsocketPingNotRearmedAfterClose(t *testing.T) {
	t.Parallel()
	logger := log.New()

	srv := newTestServer(logger)
	defer srv.Stop()
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	wc := NewWebsocketCodec(conn, "", nil, "").(*websocketCodec)
	wc.Close()
	wc.resetPing() // a write or ping that finished before Close can reset after it
	if wc.pingTimer.Stop() {
		t.Fatal("ping timer re-armed on a closed codec")
	}
}

// An idle connection gets a ping, and each ping arms the timer for the next one.
func TestWebsocketIdlePing(t *testing.T) {
	t.Parallel()

	pinged := make(chan struct{}, 1)
	httpsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OnPingReceived: func(context.Context, []byte) bool {
				select {
				case pinged <- struct{}{}:
				default:
				}
				return true
			},
		})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		conn.Read(r.Context()) //nolint:errcheck
	}))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	wc := NewWebsocketCodec(conn, "", nil, "").(*websocketCodec)
	defer wc.Close()
	go conn.Read(context.Background()) //nolint:errcheck

	wc.pingTimer.Reset(time.Millisecond)
	select {
	case <-pinged:
	case <-time.After(5 * time.Second):
		t.Fatal("idle connection was not pinged")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !wc.pingTimer.Stop() {
		if time.Now().After(deadline) {
			t.Fatal("ping did not re-arm the timer")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWebsocketUnansweredPingClosesConn(t *testing.T) {
	t.Parallel()

	closed := make(chan struct{})
	httpsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OnPingReceived: func(context.Context, []byte) bool { return false },
		})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				close(closed)
				return
			}
		}
	}))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	wc := NewWebsocketCodec(conn, "", nil, "").(*websocketCodec)
	defer wc.Close()
	go conn.Read(context.Background()) //nolint:errcheck

	wc.pingTimer.Reset(time.Millisecond)
	select {
	case <-closed:
	case <-time.After(wsPingWriteTimeout + 5*time.Second):
		t.Fatal("the connection to a peer that does not answer pings was not closed")
	}
}

// A ping due while a slow write still has budget must not close the connection: coder's ping
// gives up on the write lock after 5s, well before the write's own deadline.
func TestWebsocketPingDuringSlowWriteKeepsConn(t *testing.T) {
	t.Parallel()

	codecs := make(chan *websocketCodec, 1)
	httpsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hw := &hijackRecorder{ResponseWriter: w}
		conn, err := websocket.Accept(hw, r, nil)
		if err != nil {
			return
		}
		wc := newWebsocketCodec(conn, hw.conn, nil, r.Host, r.Header, r.RemoteAddr)
		defer wc.Close()
		codecs <- wc
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()
	conn.SetReadLimit(-1)

	wc := <-codecs
	wc.writeTimeout = 3 * wsPingWriteTimeout
	payload := rawResponse(`"` + strings.Repeat("x", 32<<20) + `"`)
	wc.pingTimer.Reset(100 * time.Millisecond)
	writeErr := make(chan error, 1)
	go func() { writeErr <- wc.WriteJSON(context.Background(), payload) }()

	time.Sleep(wsPingWriteTimeout + 2*time.Second)
	_, data, err := conn.Read(t.Context())
	if err != nil {
		t.Fatalf("the connection was closed during a write that still had budget: %v", err)
	}
	if len(data) != len(payload) {
		t.Fatalf("got %d bytes, want %d", len(data), len(payload))
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write failed: %v", err)
	}
}

type writeCountingConn struct {
	net.Conn
	writes *atomic.Int64
}

func (c writeCountingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

func TestWebsocketCoalescedMessagesLeaveInOneWrite(t *testing.T) {
	t.Parallel()

	var writes atomic.Int64
	codecs := make(chan *websocketCodec, 1)
	httpsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hw := &hijackRecorder{ResponseWriter: w}
		conn, err := websocket.Accept(hw, r, nil)
		if err != nil {
			return
		}
		hw.conn.Conn = writeCountingConn{hw.conn.Conn, &writes}
		wc := newWebsocketCodec(conn, hw.conn, nil, r.Host, r.Header, r.RemoteAddr)
		defer wc.Close()
		codecs <- wc
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()

	wc := <-codecs
	before := writes.Load()
	err = wc.coalesce(func() {
		for i := range 3 {
			if err := wc.WriteJSON(context.Background(), rawResponse(strconv.Itoa(i))); err != nil {
				t.Error(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := writes.Load() - before; got != 1 {
		t.Errorf("3 coalesced messages took %d socket writes, want 1", got)
	}
	for i := range 3 {
		_, data, err := conn.Read(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != strconv.Itoa(i) {
			t.Fatalf("message %d is %q", i, data)
		}
	}

	msgs := []string{"0", strconv.Quote(strings.Repeat("x", 2*heldWriteLimit)), "2"}
	conn.SetReadLimit(int64(4 * heldWriteLimit))
	err = wc.coalesce(func() {
		for _, m := range msgs {
			if err := wc.WriteJSON(context.Background(), rawResponse(m)); err != nil {
				t.Error(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range msgs {
		_, data, err := conn.Read(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != want {
			t.Fatalf("message %d is %d bytes, want %d", i, len(data), len(want))
		}
	}
}

// peakService records the most calls it ran at once.
type peakService struct{ inflight, peak atomic.Int32 }

func (s *peakService) Hold() {
	n := s.inflight.Add(1)
	for p := s.peak.Load(); n > p && !s.peak.CompareAndSwap(p, n); p = s.peak.Load() {
	}
	time.Sleep(20 * time.Millisecond)
	s.inflight.Add(-1)
}

func TestWebsocketBatchUsesServerConcurrency(t *testing.T) {
	t.Parallel()
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	srv.batchConcurrency = 2
	svc := new(peakService)
	if err := srv.RegisterName("peak", svc); err != nil {
		t.Fatal(err)
	}
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()

	client, err := DialContext(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), logger)
	if err != nil {
		t.Fatalf("can't dial: %v", err)
	}
	defer client.Close()
	batch := make([]BatchElem, 8)
	for i := range batch {
		batch[i] = BatchElem{Method: "peak_hold", Result: new(any)}
	}
	if err := client.BatchCall(batch); err != nil {
		t.Fatal(err)
	}
	for _, e := range batch {
		if e.Error != nil {
			t.Fatal(e.Error)
		}
	}
	if peak := svc.peak.Load(); peak > 2 {
		t.Fatalf("websocket batch ran %d calls at once, server allows 2", peak)
	}
}

func TestWebsocketNotificationGetsNoReply(t *testing.T) {
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	hs := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer hs.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(hs.URL, "http"), nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	require.NoError(t, err)
	defer func() { _ = conn.CloseNow() }()

	for _, msg := range []string{
		`{"jsonrpc":"2.0","method":"test_echo","params":["x",1]}`,
		`{"jsonrpc":"2.0","method":"test_streamEcho","params":["x"]}`,
		`{"jsonrpc":"2.0","method":"test_nosuch","params":[]}`,
		`{"jsonrpc":"2.0","id":7,"method":"test_echo","params":["y",2]}`,
	} {
		require.NoError(t, conn.Write(ctx, websocket.MessageText, []byte(msg)))
	}
	_, frame, err := conn.Read(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `{"jsonrpc":"2.0","id":7,"result":{"String":"y","Int":2,"Args":null}}`, string(frame))
}

func TestWebsocketClientNotifyKeepsConnection(t *testing.T) {
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	hs := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer hs.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, err := DialWebsocket(ctx, "ws"+strings.TrimPrefix(hs.URL, "http"), "", logger)
	require.NoError(t, err)
	defer client.Close()

	sub, err := client.Subscribe(ctx, "nftest", make(chan int, 1), "someSubscription", 100, 0)
	require.NoError(t, err)
	require.NoError(t, client.Notify(ctx, "test_echo", "x", 1))

	var result echoResult
	require.NoError(t, client.CallContext(ctx, &result, "test_echo", "y", 2))
	require.Equal(t, echoResult{"y", 2, nil}, result)
	select {
	case err := <-sub.Err():
		t.Fatalf("subscription ended: %v", err)
	default:
	}
}

// TestWebsocketMessageBudget pins that a message outgrowing smallBodyLimit is charged to the
// server's ingress budget while it is read: when the budget is full the connection is dropped,
// and the charge is released once the message is read.
func TestWebsocketMessageBudget(t *testing.T) {
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()
	wsURL := "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	srv.ingress.limit = maxRequestContentLength
	require.True(t, srv.ingress.acquire(maxRequestContentLength)) // another body holds the whole budget

	call := func(size int) (string, error) {
		conn, resp, err := websocket.Dial(t.Context(), wsURL, nil)
		if err != nil {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			t.Fatalf("can't dial: %v", err)
		}
		defer func() { _ = conn.CloseNow() }()
		conn.SetReadLimit(int64(size) + 1024)
		require.NoError(t, conn.Write(t.Context(), websocket.MessageText, []byte(echoRequest(size))))
		_, data, err := conn.Read(t.Context())
		return string(data), err
	}
	resp, err := call(int(smallBodyLimit))
	require.NoError(t, err)
	require.Contains(t, resp, `"result"`)

	_, err = call(int(smallBodyLimit) + 1)
	require.Error(t, err) // over budget: the socket is dropped (CloseNow), not gracefully closed

	srv.ingress.release(maxRequestContentLength)
	resp, err = call(int(smallBodyLimit) + 1)
	require.NoError(t, err)
	require.Contains(t, resp, `"result"`)
	require.Eventually(t, func() bool { return srv.ingress.used.Load() == 0 }, 5*time.Second, time.Millisecond)
}

func TestWebsocketMessageBudgetDropsPartialFrame(t *testing.T) {
	const timeout = 5 * time.Second
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	srv.ingress.limit = maxRequestContentLength
	require.True(t, srv.ingress.acquire(maxRequestContentLength))
	defer srv.ingress.release(maxRequestContentLength)

	handlerDone := make(chan struct{})
	handler := srv.WebsocketHandler([]string{"*"}, nil, false, logger)
	httpsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		handler.ServeHTTP(w, r)
	}))
	defer httpsrv.Close()

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", httpsrv.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(timeout)))

	_, err = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", httpsrv.Listener.Addr())
	require.NoError(t, err)
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)

	const (
		frameHeaderSize     = 14
		declaredPayloadSize = 2 * smallBodyLimit
		sentPayloadSize     = smallBodyLimit + int64(maxBudgetedRead)
	)
	frame := make([]byte, frameHeaderSize+int(sentPayloadSize))
	frame[0], frame[1] = 0x81, 0xff
	binary.BigEndian.PutUint64(frame[2:10], uint64(declaredPayloadSize))
	_, err = conn.Write(frame)
	require.NoError(t, err)

	data, err := io.ReadAll(reader)
	if netErr, ok := errors.AsType[net.Error](err); ok {
		require.False(t, netErr.Timeout(), "the refused partial frame kept the socket open")
	} else {
		require.NoError(t, err)
	}
	require.Empty(t, data)
	select {
	case <-handlerDone:
	case <-time.After(timeout):
		t.Fatal("the WebSocket handler did not stop after dropping the socket")
	}
	require.Equal(t, int64(maxRequestContentLength), srv.ingress.used.Load())
}

// holdService keeps a call running until the test releases it.
type holdService struct {
	started chan struct{}
	release chan struct{}
}

func (s *holdService) Hold(string) {
	close(s.started)
	<-s.release
}

// TestWebsocketMessageBudgetHeldUntilServed pins that a large message stays charged, for its
// bytes past smallBodyLimit, while its call runs, and is released once the call is done.
func TestWebsocketMessageBudgetHeldUntilServed(t *testing.T) {
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	hold := &holdService{started: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, srv.RegisterName("hold", hold))
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()

	conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		t.Fatalf("can't dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()
	msg := `{"jsonrpc":"2.0","id":1,"method":"hold_hold","params":["` + strings.Repeat("x", int(smallBodyLimit)) + `"]}`
	require.NoError(t, conn.Write(t.Context(), websocket.MessageText, []byte(msg)))

	<-hold.started
	require.Equal(t, int64(len(msg))-smallBodyLimit, srv.ingress.used.Load())

	close(hold.release)
	_, _, err = conn.Read(t.Context())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return srv.ingress.used.Load() == 0 }, 5*time.Second, time.Millisecond)
}

// TestWebsocketStalledFragmentsHoldOnlyTheirBytes pins that peers who send the start of a
// large message and then stall hold only the bytes they sent, so they cannot lock the
// budget for the requests of other clients.
func TestWebsocketStalledFragmentsHoldOnlyTheirBytes(t *testing.T) {
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	wssrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer wssrv.Close()
	httpsrv := httptest.NewServer(srv)
	defer httpsrv.Close()

	const peers, past = 8, 2 * int(maxBudgetedRead) // enough past the free size to complete a budgeted read
	var conns []*websocket.Conn
	defer func() {
		for _, conn := range conns {
			_ = conn.CloseNow()
		}
	}()
	for range peers {
		conn, resp, err := websocket.Dial(t.Context(), "ws:"+strings.TrimPrefix(wssrv.URL, "http:"), nil)
		if err != nil {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			t.Fatalf("can't dial: %v", err)
		}
		conns = append(conns, conn)
		w, err := conn.Writer(t.Context(), websocket.MessageText)
		require.NoError(t, err)
		_, err = w.Write(make([]byte, int(smallBodyLimit)+past)) // a non-final fragment, then silence
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return srv.ingress.used.Load() > 0 }, 5*time.Second, time.Millisecond)
	require.LessOrEqual(t, srv.ingress.used.Load(), int64(peers*past))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpsrv.URL, strings.NewReader(echoRequest(int(smallBodyLimit)+1)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := httpsrv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestClientWebsocketMessageSizeLimit(t *testing.T) {
	logger := log.New()
	srv := NewServer(50, false /* traceRequests */, false /* debugSingleRequests */, true, logger, 100)
	defer srv.Stop()
	httpsrv := httptest.NewServer(srv.WebsocketHandler(nil, nil, false, logger))
	defer httpsrv.Close()
	wsURL := "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	require.NoError(t, srv.RegisterName("test", largeRespService{1024}))

	for _, tc := range []struct {
		limit   int64
		wantErr bool
	}{{limit: 512, wantErr: true}, {limit: 4096}} {
		c, err := DialOptions(context.Background(), wsURL, logger, WithWebsocketMessageSizeLimit(tc.limit))
		require.NoError(t, err)
		var r string
		err = c.Call(&r, "test_largeResp")
		c.Close()
		require.Equal(t, tc.wantErr, err != nil, "limit %d: %v", tc.limit, err)
	}
}
