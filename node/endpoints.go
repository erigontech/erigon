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
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

type HttpEndpointConfig struct {
	Timeouts rpccfg.HTTPTimeouts
	HTTPS    bool
	CertFile string
	KeyFile  string
}

// StartHTTPEndpoint starts the HTTP RPC endpoint.
func StartHTTPEndpoint(urlEndpoint string, cfg *HttpEndpointConfig, handler http.Handler) (*http.Server, net.Addr, error) {
	// start the HTTP listener
	var (
		listener net.Listener
		err      error
	)
	socketUrl, err := url.Parse(urlEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("malformatted http listen url %s: %w", urlEndpoint, err)
	}
	if listener, err = net.Listen(socketUrl.Scheme, socketUrl.Host+socketUrl.EscapedPath()); err != nil {
		return nil, nil, err
	}
	// make sure timeout values are meaningful
	CheckTimeouts(&cfg.Timeouts)
	// create the http2 server for handling h2c
	h2 := &http2.Server{}
	// enable h2c support
	handler = h2c.NewHandler(handler, h2)
	// Bundle the http server
	httpSrv := &http.Server{
		Handler:           handler,
		ReadTimeout:       cfg.Timeouts.ReadTimeout,
		WriteTimeout:      cfg.Timeouts.WriteTimeout,
		IdleTimeout:       cfg.Timeouts.IdleTimeout,
		ReadHeaderTimeout: cfg.Timeouts.ReadTimeout,
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

func isIgnoredHttpServerError(serveErr error) bool {
	return errors.Is(serveErr, context.Canceled) || errors.Is(serveErr, common.ErrStopped) || errors.Is(serveErr, http.ErrServerClosed)
}

// StartHTTPSEndpoint starts the HTTPS RPC endpoint.
func StartHTTPSEndpoint(urlEndpoint string,
	keyFile string, certFile string,
	timeouts rpccfg.HTTPTimeouts, handler http.Handler,
) (*http.Server, net.Addr, error) {
	// start the HTTP listener
	var (
		listener net.Listener
		err      error
	)
	socketUrl, err := url.Parse(urlEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("malformatted http listen url %s: %w", urlEndpoint, err)
	}
	if listener, err = net.Listen(socketUrl.Scheme, socketUrl.Host+socketUrl.EscapedPath()); err != nil {
		return nil, nil, err
	}
	// make sure timeout values are meaningful
	CheckTimeouts(&timeouts)
	// create the http2 server for handling h2c
	h2 := &http2.Server{}
	// enable h2c support
	handler = h2c.NewHandler(handler, h2)
	// Bundle the http server
	httpSrv := &http.Server{
		Handler:           handler,
		ReadTimeout:       timeouts.ReadTimeout,
		WriteTimeout:      timeouts.WriteTimeout,
		IdleTimeout:       timeouts.IdleTimeout,
		ReadHeaderTimeout: timeouts.ReadTimeout,
	}
	// start the HTTP server
	go func() {
		serveErr := httpSrv.ServeTLS(listener, certFile, keyFile)
		if serveErr != nil &&
			!(errors.Is(serveErr, context.Canceled) || errors.Is(serveErr, common.ErrStopped) || errors.Is(serveErr, http.ErrServerClosed)) {
			log.Warn("Failed to serve http endpoint", "err", serveErr)
		}
	}()
	return httpSrv, listener.Addr(), err
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
		log.Warn("Sanitizing invalid HTTP read timeout", "provided", timeouts.ReadTimeout, "updated", rpccfg.DefaultHTTPTimeouts.ReadTimeout)
		timeouts.ReadTimeout = rpccfg.DefaultHTTPTimeouts.ReadTimeout
	}
	if timeouts.WriteTimeout < time.Second {
		log.Warn("Sanitizing invalid HTTP write timeout", "provided", timeouts.WriteTimeout, "updated", rpccfg.DefaultHTTPTimeouts.WriteTimeout)
		timeouts.WriteTimeout = rpccfg.DefaultHTTPTimeouts.WriteTimeout
	}
	if timeouts.IdleTimeout < time.Second {
		log.Warn("Sanitizing invalid HTTP idle timeout", "provided", timeouts.IdleTimeout, "updated", rpccfg.DefaultHTTPTimeouts.IdleTimeout)
		timeouts.IdleTimeout = rpccfg.DefaultHTTPTimeouts.IdleTimeout
	}
}
