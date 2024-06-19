package diagnostics

import (
	"encoding/json"
	"net/http"

	diaglib "github.com/ledgerwatch/erigon-lib/diagnostics"
)

func SetupBodiesAccess(metricsMux *http.ServeMux, diag *diaglib.DiagnosticClient) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/bodies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeBodies(w, diag)
	})

}

func writeBodies(w http.ResponseWriter, diag *diaglib.DiagnosticClient) {
	json.NewEncoder(w).Encode(diag.GetBodiesInfo())
}
