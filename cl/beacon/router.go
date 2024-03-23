package beacon

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/ledgerwatch/erigon/cl/beacon/beacon_router_configuration"
	"github.com/ledgerwatch/erigon/cl/beacon/handler"
	"github.com/ledgerwatch/log/v3"
)

type LayeredBeaconHandler struct {
	ArchiveApi *handler.ApiHandler
}

func ListenAndServe(beaconHandler *LayeredBeaconHandler, routerCfg beacon_router_configuration.RouterConfiguration) error {
	listener, err := net.Listen(routerCfg.Protocol, routerCfg.Address)
	if err != nil {
		return err
	}
	defer listener.Close()
	mux := chi.NewRouter()

	mux.Use(cors.Handler(
		cors.Options{
			AllowedOrigins:   routerCfg.AllowedOrigins,
			AllowedMethods:   routerCfg.AllowedMethods,
			AllowCredentials: routerCfg.AllowCredentials,
			MaxAge:           4,
		}))

	mux.HandleFunc("/*", func(w http.ResponseWriter, r *http.Request) {
		nfw := &notFoundNoWriter{ResponseWriter: w, r: r}
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, chi.NewRouteContext()))
		if isNotFound(nfw.code) || nfw.code == 0 {
			start := time.Now()
			beaconHandler.ArchiveApi.ServeHTTP(w, r)
			log.Debug("[Beacon API] Request", "uri", r.URL.String(), "path", r.URL.Path, "time", time.Since(start))
		} else {
			log.Warn("[Beacon API] Request to unavaiable endpoint, check --beacon.api flag", "uri", r.URL.String(), "path", r.URL.Path)
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
		WriteTimeout: routerCfg.IdleTimeout,
	}
	if err != nil {
		log.Warn("[Beacon API] Failed to start listening", "addr", routerCfg.Address, "err", err)
	}

	if err := server.Serve(listener); err != nil {
		log.Warn("[Beacon API] failed to start serving", "addr", routerCfg.Address, "err", err)
		return err
	}
	log.Info("[Beacon API] Listening", "addr", routerCfg.Address)
	return nil
}
