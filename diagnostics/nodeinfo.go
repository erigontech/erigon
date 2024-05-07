package diagnostics

import (
	"encoding/json"
	"net/http"

	"github.com/ledgerwatch/erigon/turbo/node"
)

func SetupNodeInfoAccess(metricsMux *http.ServeMux, node *node.ErigonNode) {
	metricsMux.HandleFunc("/nodeinfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeNodeInfo(w, node)
	})
}

func writeNodeInfo(w http.ResponseWriter, node *node.ErigonNode) {
	reply, err := node.Backend().NodesInfo(0)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(reply)
}
