package metrics

import (
	"fmt"
	"net/http"
	"sort"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/log/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// Handler returns an HTTP handler which dump metrics in Prometheus format.
// Output format can be cheched here: https://o11y.tools/metricslint/
func Handler(reg Registry) http.Handler {
	prometheus.DefaultRegisterer.MustRegister(defaultSet)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Gather and pre-sort the metrics to avoid random listings
		var names []string
		reg.Each(func(name string, i interface{}) {
			names = append(names, name)
		})
		sort.Strings(names)

		w.Header().Set("Access-Control-Allow-Origin", "*")

		metrics2.WritePrometheus(w, false)

		contentType := expfmt.Negotiate(r.Header)
		enc := expfmt.NewEncoder(w, contentType)
		mf, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			return
		}
		for _, m := range mf {
			enc.Encode(m)
		}

		// Aggregate all the metris into a Prometheus collector
		c := newCollector()
		c.buff.WriteRune('\n')

		var typeName string
		var prevTypeName string

		for _, name := range names {
			i := reg.Get(name)

			typeName = stripLabels(name)

			switch m := i.(type) {
			case *metrics2.Counter:
				if m.IsGauge() {
					c.writeGauge(name, m.Get(), typeName != prevTypeName)
				} else {
					c.writeCounter(name, m.Get(), typeName != prevTypeName)
				}
			case *metrics2.Gauge:
				c.writeGauge(name, m, typeName != prevTypeName)
			case *metrics2.FloatCounter:
				c.writeFloatCounter(name, m, typeName != prevTypeName)
			case *metrics2.Histogram:
				c.writeHistogram(name, m, typeName != prevTypeName)
			case *metrics2.Summary:
				c.writeTimer(name, m, typeName != prevTypeName)
			default:
				log.Warn("Unknown Prometheus metric type", "type", fmt.Sprintf("%T", i))
			}

			prevTypeName = typeName
		}
		w.Header().Add("Content-Type", "text/plain")
		w.Header().Add("Content-Length", fmt.Sprint(c.buff.Len()))
		w.Write(c.buff.Bytes())
	})
}
