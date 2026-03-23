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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

const (
	wsPingInterval     = 60 * time.Second
	wsPingWriteTimeout = 5 * time.Second
	wsMessageSizeLimit = 32 * 1024 * 1024
)

// WebsocketHandler returns a handler that serves JSON-RPC to WebSocket connections.
//
// allowedOrigins should be a comma-separated list of allowed origin URLs.
// To allow connections with any origin, pass "*".
func (s *Server) WebsocketHandler(allowedOrigins []string, jwtSecret []byte, compression bool, logger log.Logger) http.Handler {
	validateOrigin := wsHandshakeValidator(allowedOrigins, logger)

	compressionMode := websocket.CompressionDisabled
	if compression {
		compressionMode = websocket.CompressionContextTakeover
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if jwtSecret != nil && !CheckJwtSecret(w, r, jwtSecret) {
			return
		}
		// Validate origin before upgrading. Rejecting here avoids the cost of
		// the WebSocket handshake for disallowed origins.
		if !validateOrigin(r) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // origin already validated above
			CompressionMode:    compressionMode,
		})
		if err != nil {
			logger.Warn("WebSocket upgrade failed", "err", err)
			return
		}
		codec := NewWebsocketCodec(conn, r.Host, r.Header, r.RemoteAddr)
		// Tag the connection context so BeginRo fails fast (ErrReadTxLimitExceeded)
		// instead of blocking indefinitely when the DB semaphore is full.
		// r.Context() remains valid for the lifetime of the WebSocket session because
		// the HTTP handler goroutine blocks inside ServeCodecWithContext until the
		// connection closes.
		ctx := kv.WithNonBlockingAcquire(r.Context())
		s.ServeCodecWithContext(ctx, codec, 0)
	})
}

// wsHandshakeValidator returns a handler that verifies the origin during the
// websocket upgrade process. When a '*' is specified as an allowed origins all
// connections are accepted.
func wsHandshakeValidator(allowedOrigins []string, logger log.Logger) func(*http.Request) bool {
	origins := mapset.NewSet[string]()
	allowAllOrigins := false

	for _, origin := range allowedOrigins {
		if origin == "*" {
			allowAllOrigins = true
		}
		if origin != "" {
			origins.Add(origin)
		}
	}
	// allow localhost if no allowedOrigins are specified.
	if len(origins.ToSlice()) == 0 {
		origins.Add("http://localhost")
		if hostname, err := os.Hostname(); err == nil {
			origins.Add("http://" + hostname)
		}
	}
	logger.Trace(fmt.Sprintf("Allowed origin(s) for WS RPC interface %v", origins.ToSlice()))

	f := func(req *http.Request) bool {
		// Skip origin verification if no Origin header is present. The origin check
		// is supposed to protect against browser based attacks. Browsers always set
		// Origin. Non-browser software can put anything in origin and checking it doesn't
		// provide additional security.
		if _, ok := req.Header["Origin"]; !ok {
			return true
		}
		// Verify origin against whitelist.
		origin := strings.ToLower(req.Header.Get("Origin"))
		if allowAllOrigins || originIsAllowed(origins, origin, logger) {
			return true
		}
		logger.Warn("Rejected WebSocket connection", "origin", origin)
		return false
	}

	return f
}

type wsHandshakeError struct {
	err    error
	status string
}

func (e wsHandshakeError) Error() string {
	s := e.err.Error()
	if e.status != "" {
		s += " (HTTP status " + e.status + ")"
	}
	return s
}

func (e wsHandshakeError) Unwrap() error {
	return e.err
}

func originIsAllowed(allowedOrigins mapset.Set[string], browserOrigin string, logger log.Logger) bool {
	it := allowedOrigins.Iterator()
	for origin := range it.C {
		if ruleAllowsOrigin(origin, browserOrigin, logger) {
			return true
		}
	}
	return false
}

func ruleAllowsOrigin(allowedOrigin string, browserOrigin string, logger log.Logger) bool {
	var (
		allowedScheme, allowedHostname, allowedPort string
		browserScheme, browserHostname, browserPort string
		err                                         error
	)
	allowedScheme, allowedHostname, allowedPort, err = parseOriginURL(allowedOrigin)
	if err != nil {
		logger.Warn("Error parsing allowed origin specification", "spec", allowedOrigin, "err", err)
		return false
	}
	browserScheme, browserHostname, browserPort, err = parseOriginURL(browserOrigin)
	if err != nil {
		logger.Warn("Error parsing browser 'Origin' field", "Origin", browserOrigin, "err", err)
		return false
	}
	if allowedScheme != "" && allowedScheme != browserScheme {
		return false
	}
	if allowedHostname != "" && allowedHostname != browserHostname {
		return false
	}
	if allowedPort != "" && allowedPort != browserPort {
		return false
	}
	return true
}

func parseOriginURL(origin string) (string, string, string, error) {
	parsedURL, err := url.Parse(strings.ToLower(origin))
	if err != nil {
		return "", "", "", err
	}
	var scheme, hostname, port string
	if strings.Contains(origin, "://") {
		scheme = parsedURL.Scheme
		hostname = parsedURL.Hostname()
		port = parsedURL.Port()
	} else {
		scheme = ""
		hostname = parsedURL.Scheme
		port = parsedURL.Opaque
		if hostname == "" {
			hostname = origin
		}
	}
	return scheme, hostname, port, nil
}

// DialWebsocket creates a new RPC client that communicates with a JSON-RPC server
// that is listening on the given endpoint.
//
// The context is used for the initial connection establishment. It does not
// affect subsequent interactions with the client.
func DialWebsocket(ctx context.Context, endpoint, origin string, logger log.Logger) (*Client, error) {
	endpoint, header, err := wsClientHeaders(endpoint, origin)
	if err != nil {
		return nil, err
	}
	return newClient(ctx, func(ctx context.Context) (ServerCodec, error) {
		conn, resp, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: header})
		if err != nil {
			// Only close resp.Body on error; on success the connection owns it.
			hErr := wsHandshakeError{err: err}
			if resp != nil {
				hErr.status = resp.Status
				resp.Body.Close()
			}
			return nil, hErr
		}
		return NewWebsocketCodec(conn, endpoint, header, endpoint), nil
	}, logger)
}

func wsClientHeaders(endpoint, origin string) (string, http.Header, error) {
	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return endpoint, nil, err
	}
	header := make(http.Header)
	if origin != "" {
		header.Add("origin", origin)
	}
	if endpointURL.User != nil {
		b64auth := base64.StdEncoding.EncodeToString([]byte(endpointURL.User.String()))
		header.Add("authorization", "Basic "+b64auth)
		endpointURL.User = nil
	}
	return endpointURL.String(), header, nil
}

// wsConnAdapter adapts coder/websocket.Conn to satisfy the deadlineCloser interface
// used by jsonCodec. Write deadlines set by jsonCodec are stored and applied as
// context deadlines on the underlying coder write calls.
type wsConnAdapter struct {
	conn     *websocket.Conn
	mu       sync.Mutex
	deadline time.Time
}

func (a *wsConnAdapter) Close() error {
	return a.conn.CloseNow()
}

func (a *wsConnAdapter) SetWriteDeadline(t time.Time) error {
	a.mu.Lock()
	a.deadline = t
	a.mu.Unlock()
	return nil
}

func (a *wsConnAdapter) encode(v any) error {
	a.mu.Lock()
	dl := a.deadline
	a.mu.Unlock()

	ctx := context.Background()
	if !dl.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, dl)
		defer cancel()
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return a.conn.Write(ctx, websocket.MessageText, data)
}

func (a *wsConnAdapter) decode(v any) error {
	// Uses context.Background() — dead connections are detected via the ping loop.
	_, data, err := a.conn.Read(context.Background())
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

type websocketCodec struct {
	*jsonCodec
	conn *websocket.Conn
	info PeerInfo

	wg        sync.WaitGroup
	pingReset chan struct{}
}

// NewWebsocketCodec wraps a coder websocket connection as a ServerCodec.
// remoteAddr should be r.RemoteAddr on the server side, or the endpoint URL on the client side.
func NewWebsocketCodec(conn *websocket.Conn, host string, req http.Header, remoteAddr string) ServerCodec {
	conn.SetReadLimit(wsMessageSizeLimit)
	adapter := &wsConnAdapter{conn: conn}
	wc := &websocketCodec{
		jsonCodec: NewFuncCodec(adapter, adapter.encode, adapter.decode).(*jsonCodec),
		conn:      conn,
		pingReset: make(chan struct{}, 1),
		info: PeerInfo{
			Transport:  "ws",
			RemoteAddr: remoteAddr,
		},
	}
	// Fill in connection details.
	wc.info.HTTP.Host = host
	if req != nil {
		wc.info.HTTP.Origin = req.Get("Origin")
		wc.info.HTTP.UserAgent = req.Get("User-Agent")
	}
	// Start pinger.
	wc.wg.Add(1)
	go wc.pingLoop()
	return wc
}

func (wc *websocketCodec) Close() {
	wc.jsonCodec.Close()
	wc.wg.Wait()
}

func (wc *websocketCodec) peerInfo() PeerInfo {
	return wc.info
}

func (wc *websocketCodec) WriteJSON(ctx context.Context, v any) error {
	err := wc.jsonCodec.WriteJSON(ctx, v)
	if err == nil {
		// Notify pingLoop to delay the next idle ping.
		select {
		case wc.pingReset <- struct{}{}:
		default:
		}
	}
	return err
}

// pingLoop sends periodic ping frames when the connection is idle.
func (wc *websocketCodec) pingLoop() {
	timer := time.NewTimer(wsPingInterval)
	defer wc.wg.Done()
	defer timer.Stop()

	for {
		select {
		case <-wc.closed():
			return
		case <-wc.pingReset:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(wsPingInterval)
		case <-timer.C:
			pingCtx, cancel := context.WithTimeout(context.Background(), wsPingWriteTimeout)
			wc.conn.Ping(pingCtx) //nolint:errcheck
			cancel()
			timer.Reset(wsPingInterval)
		}
	}
}
