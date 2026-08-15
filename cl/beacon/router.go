// Copyright 2024 The Erigon Authors
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

package beacon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/handler"
	"github.com/erigontech/erigon/common/log/v3"
)

type LayeredBeaconHandler struct {
	ArchiveApi *handler.ApiHandler
}

func ListenAndServe(ctx context.Context, beaconHandler *LayeredBeaconHandler, routerCfg beacon_router_configuration.RouterConfiguration) error {
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, routerCfg.Protocol, routerCfg.Address)
	if err != nil {
		return fmt.Errorf("failed to start listening on %s: %w", routerCfg.Address, err)
	}
	defer listener.Close()
	mux := chi.NewRouter()

	mux.Use(cors.Handler(
		cors.Options{
			AllowedOrigins:   routerCfg.AllowedOrigins,
			AllowedMethods:   routerCfg.AllowedMethods,
			AllowCredentials: routerCfg.AllowCredentials,
			MaxAge:           4,
		},
	))

	mux.HandleFunc("/*", func(w http.ResponseWriter, r *http.Request) {
		nfw := &notFoundNoWriter{ResponseWriter: w, r: r} //nolint:govet
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, chi.NewRouteContext()))
		if isNotFound(nfw.code) || nfw.code == 0 {
			start := time.Now()
			beaconHandler.ArchiveApi.ServeHTTP(w, r)
			log.Trace("[Beacon API] Request", "uri", r.URL.String(), "path", r.URL.Path, "time", time.Since(start))
		} else {
			log.Warn("[Beacon API] Request to unavailable endpoint, check --beacon.api flag", "uri", r.URL.String(), "path", r.URL.Path)
		}
	})
	mux.NotFound(func(w http.ResponseWriter, r *http.Request) {
		log.Warn("[Beacon API] Not found", "method", r.Method, "path", r.URL.Path)
		http.Error(w, "Not found", http.StatusNotFound)
	})

	server := &http.Server{
		Handler:      mux,
		ReadTimeout:  routerCfg.ReadTimeTimeout,
		IdleTimeout:  routerCfg.IdleTimeout,
		WriteTimeout: routerCfg.WriteTimeout,
	}

	// No BaseContext from ctx: cancelling ctx is what starts the shutdown, so
	// handing it to each request would cancel every in-flight one instead of
	// letting the grace period below drain them.
	serveDone := make(chan struct{})
	defer close(serveDone)
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		case <-serveDone:
		}
	}()

	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("failed to serve on %s: %w", routerCfg.Address, err)
	}
	log.Info("[Beacon API] Listening", "addr", routerCfg.Address)
	return nil
}
