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

	slashReq, err := http.NewRequest(http.MethodPost, "http://"+addr+"/mcp/", strings.NewReader(initBody))
	require.NoError(t, err)
	slashReq.Header.Set("Content-Type", "application/json")
	slashReq.Header.Set("Accept", "application/json, text/event-stream")
	slashResp, err := client.Do(slashReq)
	require.NoError(t, err)
	require.NoError(t, slashResp.Body.Close())
	require.Equal(t, http.StatusOK, slashResp.StatusCode, "trailing-slash /mcp/ must reach the streamable handler")

	cancel()
	require.NoError(t, <-done)
}

// Shutdown must not stall on open event streams: the request contexts are
// tied to the server context, so cancelling it ends the streams promptly.
func TestListenAndServeShutsDownWithOpenStream(t *testing.T) {
	addr := freeAddr(t)
	srv := NewStandaloneMCPServer(nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe(ctx, addr) }()

	probe := &http.Client{Timeout: 2 * time.Second}
	var err error
	for range 100 {
		var resp *http.Response
		resp, err = probe.Get("http://" + addr + "/sse")
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, err)

	streamReq, err := http.NewRequest(http.MethodGet, "http://"+addr+"/mcp", nil)
	require.NoError(t, err)
	streamReq.Header.Set("Accept", "text/event-stream")
	// Bounds the header wait without a request deadline that would end the
	// stream on its own and mask a stalled shutdown.
	streamTransport := &http.Transport{ResponseHeaderTimeout: 2 * time.Second}
	defer streamTransport.CloseIdleConnections()
	streamResp, err := streamTransport.RoundTrip(streamReq)
	require.NoError(t, err)
	defer streamResp.Body.Close()
	require.Equal(t, http.StatusOK, streamResp.StatusCode)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("ListenAndServe did not return within 4s with an open /mcp event stream")
	}
}
