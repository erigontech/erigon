// Hook go-metrics into expvar
// on any /debug/metrics request, load all vars from the registry into expvar, and execute regular expvar handler
package exp

import (
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/metrics/prometheus"
	"github.com/ledgerwatch/log/v3"
)

// Setup starts a dedicated metrics server at the given address.
// This function enables metrics reporting separate from pprof.
func Setup(address string, logger log.Logger) *http.ServeMux {
	prometheusMux := http.NewServeMux()

	prometheusMux.Handle("/debug/metrics/prometheus", prometheus.Handler(metrics.DefaultRegistry))

	promServer := &http.Server{
		Addr:    address,
		Handler: prometheusMux,
	}

	go func() {
		if err := promServer.ListenAndServe(); err != nil {
			log.Error("Failure in running Prometheus server", "err", err)
		}
	}()

	log.Info("Enabling metrics export to prometheus", "path", fmt.Sprintf("http://%s/debug/metrics/prometheus", address))

	return prometheusMux
}
