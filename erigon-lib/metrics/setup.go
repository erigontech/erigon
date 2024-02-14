package metrics

import (
	"fmt"
	"net/http"

	"github.com/ledgerwatch/log/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var EnabledExpensive = false

// Setup starts a dedicated metrics server at the given address.
// This function enables metrics reporting separate from pprof.
func Setup(address string, logger log.Logger) *http.ServeMux {
	prometheus.DefaultRegisterer.MustRegister(defaultSet)

	prometheusMux := http.NewServeMux()
	prometheusMux.Handle("/debug/metrics/prometheus", promhttp.Handler())

	promServer := &http.Server{
		Addr:    address,
		Handler: prometheusMux,
	}

	go func() {
		if err := promServer.ListenAndServe(); err != nil {
			logger.Error("Failure in running Prometheus server", "err", err)
		}
	}()

	logger.Info("Enabling metrics export to prometheus", "path", fmt.Sprintf("http://%s/debug/metrics/prometheus", address))
	return prometheusMux
}
