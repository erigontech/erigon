package diagnostics

import (
	"container/list"
	"encoding/json"
	"net/http"
	"time"
)

type BlockMetricsResponse struct {
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

func stats(list *list.List) BlockMetricsResponse {
	var slice []time.Duration
	var maxValue, minValue, sum time.Duration
	for e := list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(time.Duration)
		if !ok {
			continue
		}

		slice = append(slice, v)
		sum += v

		if maxValue < v {
			maxValue = v
		}

		if minValue == 0 || minValue > v {
			minValue = v
		}
	}

	average := sum / time.Duration(list.Len())

	return BlockMetricsResponse{
		Max:     maxValue,
		Min:     minValue,
		Average: average,
		Data:    slice,
	}
}

func writeHeader(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().Header
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeBody(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().Header
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeExecutionStart(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().Header
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeExecutionEnd(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().Header
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeProducer(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().Header
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
