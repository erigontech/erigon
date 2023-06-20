// Hook go-metrics into expvar
// on any /debug/metrics request, load all vars from the registry into expvar, and execute regular expvar handler
package exp

import (
	"fmt"
	"net/http"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/log/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// Setup starts a dedicated metrics server at the given address.
// This function enables metrics reporting separate from pprof.
func Setup(address string, logger log.Logger) {
	http.HandleFunc("/debug/metrics/prometheus", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		metrics2.WritePrometheus(w, true)
		contentType := expfmt.Negotiate(r.Header)
		enc := expfmt.NewEncoder(w, contentType)
		mf, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			return
		}
		for _, m := range mf {
			enc.Encode(m)
		}
	})
	//m.Handle("/debug/metrics", ExpHandler(metrics.DefaultRegistry))
	//http.Handle("/debug/metrics/prometheus2", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))
	logger.Info("Starting metrics server", "addr",
		fmt.Sprintf("http://%s/debug/metrics/prometheus", address),
	)
	go func() {
		if err := http.ListenAndServe(address, nil); err != nil { // nolint:gosec
			logger.Error("Failure in running metrics server", "err", err)
		}
	}()
}
