package beacon

import (
	"net"
	"net/http"
	"time"

	"github.com/ledgerwatch/erigon/cl/beacon/handler"
	"github.com/ledgerwatch/log/v3"
)

// TODO(enriavil1): Make this configurable via flags
type RouterConfiguration struct {
	Protocol string
	Address  string

	ReadTimeTimeout time.Duration
	IdleTimeout     time.Duration
	WriteTimeout    time.Duration
}

func ListenAndServe(api *handler.ApiHandler, routerCfg *RouterConfiguration) {
	listener, err := net.Listen(routerCfg.Protocol, routerCfg.Address)
	server := &http.Server{
		Handler:      api,
		ReadTimeout:  routerCfg.ReadTimeTimeout,
		IdleTimeout:  routerCfg.IdleTimeout,
		WriteTimeout: routerCfg.IdleTimeout,
	}
	if err != nil {
		log.Warn("[Beacon API] Failed to start listening", "addr", routerCfg.Address, "err", err)
	}

	if err := server.Serve(listener); err != nil {
		log.Warn("[Beacon API] failed to start serving", "addr", routerCfg.Address, "err", err)
	}
}
