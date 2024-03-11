package diagnostics

import (
	"encoding/json"
	"net/http"
	"time"
)

type BlockHeaderResponse struct {
	Max     time.Duration   `json:"max"`
	Min     time.Duration   `json:"min"`
	Average time.Duration   `json:"average"`
	Data    []time.Duration `json:"data"`
}

func SetupBlockMetricsAccess(metricsMux *http.ServeMux, diag *DiagnosticClient) {
	metricsMux.HandleFunc("/block-header", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeHeader(w, diag)
	})

	metricsMux.HandleFunc("/block-body", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeBody(w, diag)
	})

	metricsMux.HandleFunc("/block-execution-start", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeExecutionStart(w, diag)
	})

	metricsMux.HandleFunc("/block-execution-end", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeExecutionEnd(w, diag)
	})

	metricsMux.HandleFunc("/block-producer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeProducer(w, diag)
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

func writeBody(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().Bodies
	m, n, average := raw.Stats()

	res := BlockHeaderResponse{
		Max:     m,
		Min:     n,
		Average: average,
		Data:    raw.Items,
	}

	json.NewEncoder(w).Encode(res)
}

func writeExecutionStart(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().ExecutionStart
	m, n, average := raw.Stats()

	res := BlockHeaderResponse{
		Max:     m,
		Min:     n,
		Average: average,
		Data:    raw.Items,
	}

	json.NewEncoder(w).Encode(res)
}

func writeExecutionEnd(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().ExecutionEnd
	m, n, average := raw.Stats()

	res := BlockHeaderResponse{
		Max:     m,
		Min:     n,
		Average: average,
		Data:    raw.Items,
	}

	json.NewEncoder(w).Encode(res)
}

func writeProducer(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().Production
	m, n, average := raw.Stats()

	res := BlockHeaderResponse{
		Max:     m,
		Min:     n,
		Average: average,
		Data:    raw.Items,
	}

	json.NewEncoder(w).Encode(res)
}
