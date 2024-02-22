package diagnostics

import (
	"encoding/json"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common/mem"
)

func SetupMemAccess(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/mem", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeMem(w)
	})
}

func writeMem(w http.ResponseWriter) {
	memStats, err := mem.ReadVirtualMemStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(memStats); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
