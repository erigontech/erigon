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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

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
	var timer = time.NewTimer(0)
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

// A response past wsStreamThreshold leaves in several frames as it is encoded, and the client
// reads them back as one message; a smaller one stays one frame.
func TestWebsocketStreamsLargeResponse(t *testing.T) {
	t.Parallel()
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()
	host := strings.TrimPrefix(httpsrv.URL, "http://")

	item := strings.Repeat("x", 1000)
	for _, tc := range []struct {
		n        int
		streamed bool
	}{{300, false}, {3000, true}} {
		frames, msg := wsRawCall(t, host, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"test_streamRepeat","params":["%s",%d]}`, item, tc.n))
		if streamed := frames > 1; streamed != tc.streamed {
			t.Fatalf("a %d-byte response came in %d frame(s), streamed=%v, want %v", len(msg), frames, streamed, tc.streamed)
		}
		var resp struct{ Result []string }
		if err := json.Unmarshal(msg, &resp); err != nil {
			t.Fatalf("reassembled message is not JSON: %v", err)
		}
		if len(resp.Result) != tc.n || resp.Result[0] != item {
			t.Fatalf("got %d items, want %d", len(resp.Result), tc.n)
		}
	}
}

// wsRawCall sends one request over a hand-made websocket connection and returns how many
// frames the response came in, with their payloads joined.
func wsRawCall(t *testing.T, host, req string) (int, []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: " + host + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	// A client frame is masked; a zero mask leaves the payload as it is.
	frame := []byte{0x81, 0x80 | 126, byte(len(req) >> 8), byte(len(req)), 0, 0, 0, 0}
	if _, err := conn.Write(append(frame, req...)); err != nil {
		t.Fatal(err)
	}
	var msg []byte
	for frames := 1; ; frames++ {
		var h [2]byte
		if _, err := io.ReadFull(r, h[:]); err != nil {
			t.Fatal(err)
		}
		size := uint64(h[1] & 0x7f)
		switch size {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(r, ext[:]); err != nil {
				t.Fatal(err)
			}
			size = uint64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(r, ext[:]); err != nil {
				t.Fatal(err)
			}
			size = binary.BigEndian.Uint64(ext[:])
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil {
			t.Fatal(err)
		}
		msg = append(msg, payload...)
		if h[0]&0x80 != 0 {
			return frames, msg
		}
	}
}

// A streamed response that pauses longer than the write timeout still completes: the timeout
// bounds each write, not the whole generation. Calls answered while it holds the connection
// must not find their own timeout spent and take the connection down.
func TestWebsocketStreamOutlastsWriteTimeout(t *testing.T) {
	t.Parallel()
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	httpsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		codec := NewWebsocketCodec(conn, r.Host, r.Header, r.RemoteAddr).(*websocketCodec)
		codec.writeTimeout = 100 * time.Millisecond
		srv.ServeCodecWithContext(r.Context(), codec, 0)
	}))
	defer httpsrv.Close()

	client, err := DialWebsocket(t.Context(), "ws:"+strings.TrimPrefix(httpsrv.URL, "http:"), "", logger)
	if err != nil {
		t.Fatalf("can't dial: %v", err)
	}
	defer client.Close()

	item := strings.Repeat("x", 1000)
	streamed := make(chan error, 1)
	go func() {
		var got []string
		err := client.Call(&got, "test_streamPaused", item, 3000, 300)
		if err == nil && (len(got) != 3000 || got[0] != item) {
			err = fmt.Errorf("got %d items", len(got))
		}
		streamed <- err
	}()
	time.Sleep(100 * time.Millisecond) // the stream has passed the threshold and is paused

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Go(func() {
			var res echoResult
			errs <- client.Call(&res, "test_echo", "x", 1)
		})
	}
	if err := <-streamed; err != nil {
		t.Fatalf("streamed call: %v", err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("call queued behind the stream: %v", err)
		}
	}
}
