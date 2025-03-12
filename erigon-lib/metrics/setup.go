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

package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
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
		Addr:              address,
		Handler:           prometheusMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := promServer.ListenAndServe(); err != nil {
			logger.Error("Failure in running Prometheus server", "err", err)
		}
	}()

	logger.Info("Enabling metrics export to prometheus", "path", fmt.Sprintf("http://%s/debug/metrics/prometheus", address))
	return prometheusMux
}
