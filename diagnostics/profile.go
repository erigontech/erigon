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

package diagnostics

import (
	"fmt"
	"net/http"
	"runtime/pprof"
	"strings"

	"github.com/erigontech/erigon/diagnostics/diaglib"
)

func SetupProfileAccess(metricsMux *http.ServeMux, diag *diaglib.DiagnosticClient) {
	if metricsMux == nil {
		return
	}

	//handle all pprof, supported: goroutine, threadcreate, heap, allocs, block, mutex
	metricsMux.HandleFunc("/pprof/", func(w http.ResponseWriter, r *http.Request) {
		profile := strings.TrimPrefix(r.URL.Path, "/pprof/")
		writePprofProfile(w, profile)
	})
}

func writePprofProfile(w http.ResponseWriter, profile string) {
	p := pprof.Lookup(profile)
	if p == nil {
		http.Error(w, "Unknown profile: "+profile, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/profile")
	err := p.WriteTo(w, 0)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to write profile: %v", err), http.StatusInternalServerError)
		return
	}
}
