package mcp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ListenAndServe must expose both MCP HTTP transports on one port: streamable
// HTTP at /mcp and the legacy SSE pair at /sse + /message.
func TestListenAndServeServesBothTransports(t *testing.T) {
	addr := freeAddr(t)
	srv := NewStandaloneMCPServer(nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe(ctx, addr) }()

	client := &http.Client{Timeout: 2 * time.Second}

	initBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}`
	var resp *http.Response
	var err error
	for range 100 {
		req, reqErr := http.NewRequest(http.MethodPost, "http://"+addr+"/mcp", strings.NewReader(initBody))
		require.NoError(t, reqErr)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), `"ErigonMCP"`)

	sseResp, err := client.Get("http://" + addr + "/sse")
	require.NoError(t, err)
	defer sseResp.Body.Close()
	require.Equal(t, http.StatusOK, sseResp.StatusCode)
	require.Equal(t, "text/event-stream", sseResp.Header.Get("Content-Type"))

	cancel()
	require.NoError(t, <-done)
}
