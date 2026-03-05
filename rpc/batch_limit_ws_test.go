package rpc

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestBatchLimit_WebSocket_Exceeded(t *testing.T) {
	t.Parallel()
	logger := log.New()

	const (
		customBatchLimit = 10
		batchSize        = 20
	)

	// Create server with custom batch limit
	srv := newTestServer(logger)
	defer srv.Stop()

	srv.SetBatchLimit(customBatchLimit)

	// Start an HTTP server with WebSocket support
	httpsrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer httpsrv.Close()

	wsURL := "ws:" + strings.TrimPrefix(httpsrv.URL, "http:")

	// Connect WebSocket client
	client, err := DialWebsocket(context.Background(), wsURL, "", logger)
	if err != nil {
		t.Fatalf("failed to dial websocket: %v", err)
	}
	defer client.Close()

	// Create batch exceeding limit (20 > 10)
	var batch []BatchElem
	for i := 0; i < batchSize; i++ {
		batch = append(batch, BatchElem{
			Method: "test_echo",
			Args:   []any{"hello"},
			Result: new(echoResult),
		})
	}

	// Send batch request
	err = client.BatchCall(batch)

	// With the current implementation, conn.close is called on batch limit exceeded
	// This should result in a connection error (websocket close)
	if err != nil {
		t.Fatal("expected connection error due to batch limit exceeded, got nil")
	}

	for i, be := range batch {
		if be.Error == nil {
			t.Errorf("expected error at %d in %v, got nil", i, be.Method)
		} else {
			// It's far from ideal to check the error message, but the best we can do waiting for std error codes
			expectedErrorPrefix := fmt.Sprintf("batch limit %d exceeded", customBatchLimit)
			if i == 0 && !strings.HasPrefix(be.Error.Error(), expectedErrorPrefix) {
				t.Errorf("error at %d in %v: expected prefix: %v got: %v", i, be.Method, expectedErrorPrefix, be.Error)
			}
		}
	}
}
