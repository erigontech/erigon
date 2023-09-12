// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package prometheus exposes go-metrics into a Prometheus format.
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
func Handler(reg Registry) http.Handler {
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

		for _, name := range names {
			i := reg.Get(name)

			switch m := i.(type) {
			case *metrics2.Counter:
				if m.IsGauge() {
					c.writeGauge(name, m.Get())
				} else {
					c.writeCounter(name, m.Get())
				}
			case *metrics2.Gauge:
				c.addGauge(name, m)
			case *metrics2.FloatCounter:
				c.addFloatCounter(name, m)
			case *metrics2.Histogram:
				c.addHistogram(name, m)
			case *metrics2.Summary:
				c.addTimer(name, m)
			default:
				log.Warn("Unknown Prometheus metric type", "type", fmt.Sprintf("%T", i))
			}
		}
		w.Header().Add("Content-Type", "text/plain")
		w.Header().Add("Content-Length", fmt.Sprint(c.buff.Len()))
		w.Write(c.buff.Bytes())
	})
}
