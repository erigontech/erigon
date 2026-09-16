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

package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

type HttpEndpointConfig struct {
	Timeouts rpccfg.HTTPTimeouts
	HTTPS    bool
	CertFile string
	KeyFile  string
	Listener net.Listener // optional pre-created listener; if set, StartHTTPEndpoint uses it instead of creating a new one
}

// StartHTTPEndpoint starts the HTTP RPC endpoint.
func StartHTTPEndpoint(urlEndpoint string, cfg *HttpEndpointConfig, handler http.Handler) (*http.Server, net.Addr, error) {
	// start the HTTP listener
	var (
		listener net.Listener
		err      error
	)
	if cfg.Listener != nil {
		listener = cfg.Listener
	} else {
		var socketUrl *url.URL
		socketUrl, err = url.Parse(urlEndpoint)
		if err != nil {
			return nil, nil, fmt.Errorf("malformed http listen url %s: %w", urlEndpoint, err)
		}
		if listener, err = net.Listen(socketUrl.Scheme, socketUrl.Host+socketUrl.EscapedPath()); err != nil { //nolint:noctx
			return nil, nil, err
		}
	}
	// make sure timeout values are meaningful
	CheckTimeouts(&cfg.Timeouts)
	// create the http2 server for handling h2c
	h2 := &http2.Server{}
	// enable h2c support
	handler = h2c.NewHandler(handler, h2)
	if !cfg.HTTPS { // an ALPN-negotiated h2 connection is handed to the HTTP/2 server with the state hooks skipped, so it would never be uncorked
		listener = corkListener{listener}
	}
	// Bundle the http server
	httpSrv := &http.Server{
		Handler:           handler,
		ReadTimeout:       cfg.Timeouts.ReadTimeout,
		WriteTimeout:      cfg.Timeouts.WriteTimeout,
		IdleTimeout:       cfg.Timeouts.IdleTimeout,
		ReadHeaderTimeout: cfg.Timeouts.ReadTimeout,
		ConnState: func(conn net.Conn, state http.ConnState) {
			c, ok := conn.(*corkConn)
			if !ok {
				return
			}
			switch state {
			case http.StateIdle, http.StateClosed:
				c.flush()
			case http.StateHijacked: // the handler owns the connection now and expects its writes to reach the wire
				c.uncork()
			}
		},
	}
	// start the HTTP server
	go func() {
		var serveErr error
		if cfg.HTTPS {
			serveErr = httpSrv.ServeTLS(listener, cfg.CertFile, cfg.KeyFile)
			if serveErr != nil && !isIgnoredHttpServerError(serveErr) {
				log.Warn("Failed to serve https endpoint", "err", serveErr)
			}
		} else {
			serveErr = httpSrv.Serve(listener)
			if serveErr != nil && !isIgnoredHttpServerError(serveErr) {
				log.Warn("Failed to serve http endpoint", "err", serveErr)
			}
		}
	}()
	return httpSrv, listener.Addr(), err
}

// corkFlushBytes bounds a corked connection: a streamed answer still reaches the client in pieces of
// this size, and a connection cannot hold more than this before its buffer is handed to the socket.
const corkFlushBytes = int(256 * datasize.KB)

// corkListener holds a response in one buffer so it leaves as one write syscall. net/http writes the
// headers, the body and the chunk terminator separately, and only marks the connection idle once the
// whole response is written, which is where the buffer is flushed.
type corkListener struct{ net.Listener }

func (l corkListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &corkConn{Conn: conn}, nil
}

type corkConn struct {
	net.Conn
	mu       sync.Mutex
	buf      []byte
	uncorked bool
}

func (c *corkConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.uncorked {
		return c.Conn.Write(p)
	}
	c.buf = append(c.buf, p...)
	if len(c.buf) >= corkFlushBytes {
		if err := c.flushLocked(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Read flushes first: the peer may be waiting for what is buffered before it sends anything more - a
// TLS handshake record, a "100 Continue", or the previous response on a keep-alive connection.
func (c *corkConn) Read(p []byte) (int, error) {
	c.flush()
	return c.Conn.Read(p)
}

func (c *corkConn) Close() error {
	c.flush()
	return c.Conn.Close()
}

func (c *corkConn) flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushLocked()
}

func (c *corkConn) uncork() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uncorked = true
	_ = c.flushLocked() // the handler owns the connection; a failed flush surfaces on its next write
}

func (c *corkConn) flushLocked() error {
	if len(c.buf) == 0 {
		return nil
	}
	_, err := c.Conn.Write(c.buf)
	c.buf = c.buf[:0]
	return err
}

func isIgnoredHttpServerError(serveErr error) bool {
	return errors.Is(serveErr, context.Canceled) || errors.Is(serveErr, common.ErrStopped) || errors.Is(serveErr, http.ErrServerClosed)
}

// checkModuleAvailability checks that all names given in modules are actually
// available API services. It assumes that the MetadataApi module ("rpc") is always available;
// the registration of this "rpc" module happens in NewServer() and is thus common to all endpoints.
func checkModuleAvailability(modules []string, apis []rpc.API) (bad, available []string) {
	availableSet := make(map[string]struct{})
	for _, api := range apis {
		if _, ok := availableSet[api.Namespace]; !ok {
			availableSet[api.Namespace] = struct{}{}
			available = append(available, api.Namespace)
		}
	}
	for _, name := range modules {
		if _, ok := availableSet[name]; !ok && name != rpc.MetadataApi {
			bad = append(bad, name)
		}
	}
	return bad, available
}

// CheckTimeouts ensures that timeout values are meaningful
func CheckTimeouts(timeouts *rpccfg.HTTPTimeouts) {
	if timeouts.ReadTimeout < time.Second {
		if timeouts.ReadTimeout > 0 {
			log.Warn("Sanitizing invalid HTTP read timeout", "provided", timeouts.ReadTimeout, "updated", rpccfg.DefaultHTTPTimeouts.ReadTimeout)
		} else {
			log.Debug("Sanitizing invalid HTTP read timeout", "provided", timeouts.ReadTimeout, "updated", rpccfg.DefaultHTTPTimeouts.ReadTimeout)
		}
		timeouts.ReadTimeout = rpccfg.DefaultHTTPTimeouts.ReadTimeout
	}
	if timeouts.WriteTimeout < time.Second {
		if timeouts.WriteTimeout > 0 {
			log.Warn("Sanitizing invalid HTTP write timeout", "provided", timeouts.WriteTimeout, "updated", rpccfg.DefaultHTTPTimeouts.WriteTimeout)
		} else {
			log.Debug("Sanitizing invalid HTTP write timeout", "provided", timeouts.WriteTimeout, "updated", rpccfg.DefaultHTTPTimeouts.WriteTimeout)
		}
		timeouts.WriteTimeout = rpccfg.DefaultHTTPTimeouts.WriteTimeout
	}
	if timeouts.IdleTimeout < time.Second {
		if timeouts.IdleTimeout > 0 {
			log.Warn("Sanitizing invalid HTTP idle timeout", "provided", timeouts.IdleTimeout, "updated", rpccfg.DefaultHTTPTimeouts.IdleTimeout)
		} else {
			log.Debug("Sanitizing invalid HTTP idle timeout", "provided", timeouts.IdleTimeout, "updated", rpccfg.DefaultHTTPTimeouts.IdleTimeout)
		}
		timeouts.IdleTimeout = rpccfg.DefaultHTTPTimeouts.IdleTimeout
	}
}
