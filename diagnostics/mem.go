package diagnostics

import (
	"encoding/json"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"net/http"
	"time"
)

func SetupMemAccess(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/mem", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeMem(w)
	})

	// update prometheus memory stats at least every 30 seconds
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			dbg.GetMemUsage()
			<-ticker.C
		}
	}()
}

func writeMem(w http.ResponseWriter) {
	memStats, err := dbg.GetMemUsage()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(memStats)
}
