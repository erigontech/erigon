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
	"net/http"

	"github.com/erigontech/erigon/diagnostics/diaglib"
)

func SetupStagesAccess(metricsMux *http.ServeMux, diag *diaglib.DiagnosticClient) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/snapshot-sync", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeStages(w, diag)
	})

	metricsMux.HandleFunc("/snapshot-files-list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeFilesList(w, diag)
	})

	metricsMux.HandleFunc("/resources-usage", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeResourcesUsage(w, diag)
	})

	metricsMux.HandleFunc("/network-speed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeNetworkSpeed(w, diag)
	})

	metricsMux.HandleFunc("/sync-stages", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeSyncStages(w, diag)
	})
}

func writeNetworkSpeed(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	diag.NetworkSpeedJson(w)
}

func writeResourcesUsage(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	diag.ResourcesUsageJson(w)
}

func writeStages(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	diag.SyncStatsJson(w)
}

func writeFilesList(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	diag.SnapshotFilesListJson(w)
}

func writeSyncStages(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	diag.SyncStagesJson(w)
}
