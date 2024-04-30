package diagnostics

import (
	"encoding/json"
	"net/http"

	diaglib "github.com/ledgerwatch/erigon-lib/diagnostics"
)

func SetupHeadersAccess(metricsMux *http.ServeMux, diag *diaglib.DiagnosticClient) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/headers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeHeaders(w, diag)
	})
}

func writeHeaders(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	json.NewEncoder(w).Encode(diag.GetHeaders())
}
