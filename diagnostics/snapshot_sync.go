package diagnostics

import (
	"encoding/json"
	"net/http"

	diaglib "github.com/ledgerwatch/erigon-lib/diagnostics"
)

func SetupStagesAccess(metricsMux *http.ServeMux, diag *diaglib.DiagnosticClient) {
	metricsMux.HandleFunc("/snapshot-sync", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeStages(w, diag)
	})

	metricsMux.HandleFunc("/snapshot-files-list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeFilesList(w, diag)
	})

	metricsMux.HandleFunc("/hardware-info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeHardwareInfo(w, diag)
	})
}

func writeStages(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	json.NewEncoder(w).Encode(diag.SyncStatistics())
}

func writeFilesList(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	json.NewEncoder(w).Encode(diag.SnapshotFilesList())
}

func writeHardwareInfo(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	json.NewEncoder(w).Encode(diag.HardwareInfo())
}
