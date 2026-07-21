package mcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/erigontech/erigon/common/log/v3"
)

// Version is the MCP server version reported to clients.
const Version = "0.0.2"

// ErigonMCPServer exposes Erigon's JSON-RPC APIs, logs and metrics over MCP.
// All chain tools dispatch through a single rpcCaller: a remote rpc.Client
// when proxying a node over HTTP, or an in-process one when embedded in
// erigon / opening a datadir directly.
type ErigonMCPServer struct {
	mcpServer      *server.MCPServer
	client         rpcCaller
	metricsEnabled bool
	logTools
}

// NewErigonMCPServer creates an MCP server dispatching chain tools through
// client. metricsEnabled should be set only when the process hosts the node's
// own Prometheus registry (embedded mode); otherwise metrics tools report
// that they are unavailable.
func NewErigonMCPServer(client rpcCaller, logDir string, metricsEnabled bool) *ErigonMCPServer {
	e := &ErigonMCPServer{
		client:         client,
		metricsEnabled: metricsEnabled,
		logTools:       logTools{logDir: logDir},
	}

	e.mcpServer = server.NewMCPServer(
		"ErigonMCP",
		Version,
		server.WithResourceCapabilities(true, true),
		server.WithToolCapabilities(true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
		server.WithRecovery(),
	)

	for _, c := range rpcToolCalls() {
		e.mcpServer.AddTool(c.tool(), e.rpcToolHandler(c))
	}
	registerMetricsTools(e)
	registerLogTools(e)
	registerPrompts(e.mcpServer)
	e.registerResources()

	return e
}

// Serve starts MCP server in stdio mode with its own signal handling.
func (e *ErigonMCPServer) Serve() error {
	return server.ServeStdio(e.mcpServer)
}

// ServeContext starts MCP server in stdio mode using the provided context
// for lifecycle management, avoiding duplicate signal handlers.
func (e *ErigonMCPServer) ServeContext(ctx context.Context) error {
	return server.NewStdioServer(e.mcpServer).Listen(ctx, os.Stdin, os.Stdout)
}

// ListenAndServe starts the MCP HTTP endpoint, shutting it down when ctx is
// cancelled. Streamable HTTP is served at /mcp, the legacy SSE pair at
// /sse + /message.
func (e *ErigonMCPServer) ListenAndServe(ctx context.Context, addr string) error {
	sse := server.NewSSEServer(e.mcpServer)
	streamable := server.NewStreamableHTTPServer(e.mcpServer, server.WithEndpointPath("/mcp"))
	mux := http.NewServeMux()
	mux.Handle("/mcp", streamable)
	mux.Handle("/mcp/", streamable)
	mux.Handle("/", sse)
	// BaseContext ties every request context to ctx: open event streams (GET
	// /mcp, /sse) end on cancellation instead of stalling Shutdown until its
	// deadline.
	httpServer := &http.Server{Addr: addr, Handler: mux, BaseContext: func(net.Listener) context.Context { return ctx }}

	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("[MCP] recovered from panic", "panic", r)
				errCh <- fmt.Errorf("mcp http server panicked: %v", r)
			}
		}()
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		sse.CloseSessions()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			_ = httpServer.Close()
		}
		// Join the serve goroutine; this also surfaces a bind/serve error
		// that raced with the cancellation instead of dropping it.
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
