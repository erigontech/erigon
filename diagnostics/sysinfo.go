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
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/pprof"

	diaglib "github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/sysutils"
)

func SetupSysInfoAccess(metricsMux *http.ServeMux, diag *diaglib.DiagnosticClient) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/hardware-info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeHardwareInfo(w, diag)
	})

	metricsMux.HandleFunc("/cpu-usage", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeCPUUsage(w)
	})

	metricsMux.HandleFunc("/processes-info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeProcessesInfo(w)
	})

	metricsMux.HandleFunc("/memory-info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeMemoryInfo(w)
	})

	metricsMux.HandleFunc("/heap-profile", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "aplication/profile")
		writeHeapProfile(w)
	})
}

func writeHeapProfile(w http.ResponseWriter) {
	err := pprof.Lookup("heap").WriteTo(w, 0)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to write profile: %v", err), http.StatusInternalServerError)
		return
	}
}

func writeHardwareInfo(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	diag.HardwareInfoJson(w)
}

func writeCPUUsage(w http.ResponseWriter) {
	cpuusage := sysutils.CPUUsage()
	json.NewEncoder(w).Encode(cpuusage)
}

func writeProcessesInfo(w http.ResponseWriter) {
	processes := sysutils.GetProcessesInfo()
	json.NewEncoder(w).Encode(processes)
}

func writeMemoryInfo(w http.ResponseWriter) {
	totalMemory := sysutils.TotalMemoryUsage()
	json.NewEncoder(w).Encode(totalMemory)
}
