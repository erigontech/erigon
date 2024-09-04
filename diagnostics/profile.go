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

	diaglib "github.com/erigontech/erigon-lib/diagnostics"
)

func SetupProfileAccess(metricsMux *http.ServeMux, diag *diaglib.DiagnosticClient) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/heap-profile", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "aplication/profile")
		writeHeapProfile(w)
	})

	metricsMux.HandleFunc("/allocs-profile", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "aplication/profile")
		writeAllocsProfile(w)
	})
}

func writeHeapProfile(w http.ResponseWriter) {
	err := pprof.Lookup("heap").WriteTo(w, 0)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to write profile: %v", err), http.StatusInternalServerError)
		return
	}
}

func writeAllocsProfile(w http.ResponseWriter) {
	err := pprof.Lookup("allocs").WriteTo(w, 0)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to write profile: %v", err), http.StatusInternalServerError)
		return
	}
}
