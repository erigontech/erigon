package diagnostics

import (
	"container/list"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ledgerwatch/erigon-lib/metrics"
)

type MilliSeconds time.Duration

func (m MilliSeconds) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(m).Milliseconds())
}

var (
	averageHeaderDelayGauge         = metrics.NewGauge(`average_delay{type="header"}`)
	averageBodyDelayGauge           = metrics.NewGauge(`average_delay{type="body"}`)
	averageExecutionStartDelayGauge = metrics.NewGauge(`average_delay{type="execution_start"}`)
	averageExectionEndDelayGauge    = metrics.NewGauge(`average_delay{type="execution_end"}`)
	averageProductionDelayGauge     = metrics.NewGauge(`average_delay{type="production"}`)
)

type BlockMetricsResponse struct {
	Max     MilliSeconds   `json:"max"`
	Min     MilliSeconds   `json:"min"`
	Average MilliSeconds   `json:"average"`
	Data    []MilliSeconds `json:"data"`
}

func SetupBlockMetricsAccess(metricsMux *http.ServeMux, diag *DiagnosticClient) {
	metricsMux.HandleFunc("/block-header", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeHeaderDelays(w, diag)
	})

	metricsMux.HandleFunc("/block-body", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeBodyDelays(w, diag)
	})

	metricsMux.HandleFunc("/block-execution-start", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeExecutionStartDelays(w, diag)
	})

	metricsMux.HandleFunc("/block-execution-end", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeExecutionEndDelays(w, diag)
	})

	metricsMux.HandleFunc("/block-producer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeProducerDelays(w, diag)
	})
}

func stats(list *list.List) BlockMetricsResponse {
	if list.Len() == 0 {
		return BlockMetricsResponse{
			Max:     0,
			Min:     0,
			Average: 0,
			Data:    []MilliSeconds{},
		}
	}

	var slice []MilliSeconds
	var maxValue, minValue, sum MilliSeconds
	for e := list.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(time.Duration)
		if !ok {
			continue
		}

		m := MilliSeconds(v)

		slice = append(slice, m)
		sum += m

		if maxValue < m {
			maxValue = m
		}

		if minValue == 0 || minValue > m {
			minValue = m
		}
	}

	average := sum / MilliSeconds(list.Len())

	return BlockMetricsResponse{
		Max:     maxValue,
		Min:     minValue,
		Average: average,
		Data:    slice,
	}
}

func writeHeaderDelays(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().HeaderDelays
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeBodyDelays(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().BodyDelays
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeExecutionStartDelays(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().ExecutionStartDelays
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeExecutionEndDelays(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().ExecutionEndDelays
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeProducerDelays(w http.ResponseWriter, diag *DiagnosticClient) {
	raw := diag.BlockMetrics().ProductionDelays
	res := stats(raw)

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
