package rpc

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
)

func TestBatchLimit_WebSocket_Exceeded(t *testing.T) {
	t.Parallel()
	logger := log.New()

	// Create server with batch limit
	srv := newTestServer(logger)
	srv.SetBatchLimit(10) // Set limit to 10

	// Start HTTP server with WebSocket support
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	wsURL := "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")
	defer srv.Stop()
	defer httpsrv.Close()

	// Connect WebSocket client
	client, err := DialWebsocket(context.Background(), wsURL, "", logger)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer client.Close()

	// Create batch exceeding limit (20 > 10)
	var batch []BatchElem
	for i := 0; i < 20; i++ {
		batch = append(batch, BatchElem{
			Method: "test_echo",
			Args:   []interface{}{"hello"},
			Result: new(echoResult),
		})
	}

	// Send batch request
	err = client.BatchCall(batch)

	// With the current implementation, conn.close is called on batch limit exceeded
	// This should result in a connection error (websocket close)
	if err == nil {
		t.Fatal("expected connection error due to batch limit exceeded, got nil")
	}

	// The error should be a websocket close error or EOF
	// The specific batch limit error message is not propagated through the websocket close
	if !strings.Contains(err.Error(), "websocket:") && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("expected websocket close error, got: %v", err)
	}

	// Verify the connection is closed by trying another request
	err2 := client.Call(nil, "test_echo", "test")
	if err2 == nil {
		t.Fatal("expected error on closed connection, got nil")
	}
}
