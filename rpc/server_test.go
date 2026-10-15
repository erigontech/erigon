// Copyright 2015 The go-ethereum Authors
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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestServerRegisterName(t *testing.T) {
	logger := log.New()
	server := NewServer(50, false /* traceRequests */, false /* debugSingleRequests */, true, logger, 100)
	service := new(testService)

	if err := server.RegisterName("test", service); err != nil {
		t.Fatalf("%v", err)
	}

	if len(server.services.services) != 2 {
		t.Fatalf("Expected 2 service entries, got %d", len(server.services.services))
	}

	svc, ok := server.services.services["test"]
	if !ok {
		t.Fatalf("Expected service calc to be registered")
	}

	wantCallbacks := 11
	if len(svc.callbacks) != wantCallbacks {
		t.Errorf("Expected %d callbacks for service 'service', got %d", wantCallbacks, len(svc.callbacks))
	}
}

func TestNewServerClampsZeroBatchConcurrency(t *testing.T) {
	server := NewServer(0, false /* traceRequests */, false /* debugSingleRequests */, true, log.New(), 100)
	if server.batchConcurrency != minBatchConcurrency {
		t.Fatalf("expected batch concurrency %d, got %d", minBatchConcurrency, server.batchConcurrency)
	}
}

func TestServer(t *testing.T) {
	logger := log.New()
	files, err := dir.ReadDir("testdata")
	if err != nil {
		t.Fatal("where'd my testdata go?")
	}
	for _, f := range files {
		if f.IsDir() || strings.HasPrefix(f.Name(), ".") {
			continue
		}
		path := filepath.Join("testdata", f.Name())
		name := strings.TrimSuffix(f.Name(), filepath.Ext(f.Name()))
		t.Run(name, func(t *testing.T) {
			runTestScript(t, path, logger)
		})
	}
}

func runTestScript(t *testing.T, file string, logger log.Logger) {
	server := newTestServer(logger)
	content, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	go server.ServeCodec(NewCodec(serverConn), 0)
	readbuf := bufio.NewReader(clientConn)
	for line := range strings.SplitSeq(string(content), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case len(line) == 0 || strings.HasPrefix(line, "//"):
			// skip comments, blank lines
			continue
		case strings.HasPrefix(line, "--> "):
			t.Log(line)
			// write to connection
			if err := clientConn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatalf("set write deadline error: %v", err)
			}
			if _, err := io.WriteString(clientConn, line[4:]+"\n"); err != nil {
				t.Fatalf("write error: %v", err)
			}
		case strings.HasPrefix(line, "<-- "):
			t.Log(line)
			want := line[4:]
			// read line from connection and compare text
			if err := clientConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatalf("set read deadline error: %v", err)
			}
			sent, err := readbuf.ReadString('\n')
			if err != nil {
				t.Fatalf("read error: %v", err)
			}
			sent = strings.TrimRight(sent, "\r\n")
			if sent != want {
				t.Errorf("wrong line from server\ngot:  %s\nwant: %s", sent, want)
			}
		default:
			panic("invalid line in test script: " + line)
		}
	}
}

// This test checks that responses are delivered for very short-lived connections that
// only carry a single request.
func TestServerShortLivedConn(t *testing.T) {
	logger := log.New()
	server := newTestServer(logger)
	defer server.Stop()

	listener, err := net.Listen("tcp", "127.0.0.1:0") //nolint:noctx
	if err != nil {
		t.Fatal("can't listen:", err)
	}
	defer listener.Close()
	go func() { _ = server.ServeListener(listener) }()

	var (
		request  = `{"jsonrpc":"2.0","id":1,"method":"rpc_modules"}` + "\n"
		wantResp = `{"jsonrpc":"2.0","id":1,"result":{"nftest":"1.0","rpc":"1.0","test":"1.0"}}` + "\n"
		deadline = time.Now().Add(10 * time.Second)
	)
	var dialer net.Dialer
	for range 20 {
		conn, err := dialer.DialContext(t.Context(), "tcp", listener.Addr().String())
		if err != nil {
			t.Fatal("can't dial:", err)
		}
		if err := conn.SetDeadline(deadline); err != nil {
			t.Fatal("set deadline error:", err)
		}
		// Write the request, then half-close the connection so the server stops reading.
		if _, err := conn.Write([]byte(request)); err != nil {
			t.Fatal("write error:", err)
		}
		if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatal("close write error:", err)
		}
		// Now try to get the response.
		buf := make([]byte, 2000)
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			t.Fatal("read error:", err)
		}
		if !bytes.Equal(buf[:n], []byte(wantResp)) {
			conn.Close()
			t.Fatalf("wrong response: %s", buf[:n])
		}
		conn.Close()
	}
}

// largestWriteConn records the largest single write to the socket: a transport that streams
// never hands over more than a threshold's worth at once.
type largestWriteConn struct {
	net.Conn
	largest atomic.Int64
}

func (c *largestWriteConn) Write(p []byte) (int, error) {
	for {
		prev := c.largest.Load()
		if int64(len(p)) <= prev || c.largest.CompareAndSwap(prev, int64(len(p))) {
			break
		}
	}
	return c.Conn.Write(p)
}

// A codec with no stream of its own (IPC, websocket) must still hand a large answer to the
// socket as it is produced, instead of holding the whole answer in memory first.
func TestLargeAnswerStreamsOverCodec(t *testing.T) {
	server := newTestServer(log.New())
	defer server.Stop()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	conn := &largestWriteConn{Conn: serverConn}
	go server.ServeCodec(NewCodec(conn), 0)

	const n = 400_000
	lines := make(chan string, 1)
	go func() {
		line, err := bufio.NewReaderSize(clientConn, 1<<20).ReadString('\n')
		require.NoError(t, err)
		lines <- line
	}()
	_, err := fmt.Fprintf(clientConn, `{"jsonrpc":"2.0","id":1,"method":"test_streamHexes","params":[%d]}`+"\n", n)
	require.NoError(t, err)

	var resp struct {
		Result []string `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(<-lines), &resp))
	require.Len(t, resp.Result, n)
	require.Less(t, conn.largest.Load(), int64(2*jsonstream.FlushThreshold), "the answer reached the socket in one piece")
}
