// Copyright 2022 The go-ethereum Authors
// (original work)
// Copyright 2026 The Erigon Authors
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
	"net/http"

	"github.com/coder/websocket"
)

// ClientOption is a configuration option for the RPC client.
type ClientOption interface {
	applyOption(*clientConfig)
}

type clientConfig struct {
	// HTTP settings
	httpClient  *http.Client
	httpHeaders http.Header
	httpAuth    HTTPAuth

	// WebSocket options
	wsDialOptions      *websocket.DialOptions
	wsMessageSizeLimit *int64 // nil = default, 0 = no limit

	// RPC handler options
	batchItemLimit int
}

func (cfg *clientConfig) initHeaders() {
	if cfg.httpHeaders == nil {
		cfg.httpHeaders = make(http.Header)
	}
}

func (cfg *clientConfig) setHeader(key, value string) {
	cfg.initHeaders()
	cfg.httpHeaders.Set(key, value)
}

type optionFunc func(*clientConfig)

func (fn optionFunc) applyOption(opt *clientConfig) {
	fn(opt)
}

// WithWebsocketDialOptions configures the websocket.DialOptions used by the RPC client.
// Headers set by WithHeader and WithHeaders take precedence over opts.HTTPHeader.
func WithWebsocketDialOptions(opts websocket.DialOptions) ClientOption {
	return optionFunc(func(cfg *clientConfig) {
		cfg.wsDialOptions = &opts
	})
}

// WithWebsocketMessageSizeLimit configures the websocket message size limit used by the RPC
// client. Passing a limit of 0 means no limit.
func WithWebsocketMessageSizeLimit(messageSizeLimit int64) ClientOption {
	return optionFunc(func(cfg *clientConfig) {
		cfg.wsMessageSizeLimit = &messageSizeLimit
	})
}

// WithHeader configures HTTP headers set by the RPC client. Headers set using this option
// will be used for both HTTP and WebSocket connections.
func WithHeader(key, value string) ClientOption {
	return optionFunc(func(cfg *clientConfig) {
		cfg.setHeader(key, value)
	})
}

// WithHeaders configures HTTP headers set by the RPC client. Headers set using this
// option will be used for both HTTP and WebSocket connections.
func WithHeaders(headers http.Header) ClientOption {
	return optionFunc(func(cfg *clientConfig) {
		cfg.initHeaders()
		for k, vs := range headers {
			cfg.httpHeaders[k] = vs
		}
	})
}

// WithHTTPClient configures the http.Client used by the RPC client.
func WithHTTPClient(c *http.Client) ClientOption {
	return optionFunc(func(cfg *clientConfig) {
		cfg.httpClient = c
	})
}

// WithHTTPAuth configures HTTP request authentication. The given provider will be called
// whenever a request is made. Note that only one authentication provider can be active at
// any time.
func WithHTTPAuth(a HTTPAuth) ClientOption {
	if a == nil {
		panic("nil auth")
	}
	return optionFunc(func(cfg *clientConfig) {
		cfg.httpAuth = a
	})
}

// A HTTPAuth function is called by the client whenever a HTTP request is sent.
// The function must be safe for concurrent use.
//
// Usually, HTTPAuth functions will call h.Set("authorization", "...") to add
// auth information to the request.
type HTTPAuth func(h http.Header) error

// WithBatchItemLimit changes the maximum number of items allowed in batch requests.
//
// Note: this option applies when processing incoming batch requests. It does not affect
// batch requests sent by the client.
func WithBatchItemLimit(limit int) ClientOption {
	return optionFunc(func(cfg *clientConfig) {
		cfg.batchItemLimit = limit
	})
}
