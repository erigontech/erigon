package diagnostics

import (
	"encoding/json"
	"net/http"
	"time"
)

type BlockHeaderResponse struct {
	Max     time.Duration   `json:"max"`
	Min     time.Duration   `json:"min"`
	Average float64         `json:"average"`
	Data    []time.Duration `json:"data"`
}

func SetupBlockMetricsAccess(metricsMux *http.ServeMux, diag *DiagnosticClient) {
	metricsMux.HandleFunc("/block-header", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeHeader(w, diag)
	})
}

func writeHeader(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().Header
	m, n, average := raw.Stats()

	res := BlockHeaderResponse{
		Max:     m,
		Min:     n,
		Average: average,
		Data:    raw.Items,
	}

	json.NewEncoder(w).Encode(res)
}
