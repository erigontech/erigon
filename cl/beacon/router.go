package beacon

import (
	"fmt"
	"net"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beacon_router_configuration"
	"github.com/ledgerwatch/erigon/cl/beacon/handler"
	"github.com/ledgerwatch/log/v3"
)

func ListenAndServe(api *handler.ApiHandler, routerCfg beacon_router_configuration.RouterConfiguration) {
	listener, err := net.Listen(routerCfg.Protocol, routerCfg.Address)
	fmt.Println(routerCfg.Address, routerCfg.Protocol)
	server := &http.Server{
		Handler:      newBeaconMiddleware(api),
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
