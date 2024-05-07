package diagnostics

import (
	"encoding/json"
	"net/http"

	"github.com/ledgerwatch/erigon/turbo/node"
)

func SetupBootnodesAccess(metricsMux *http.ServeMux, node *node.ErigonNode) {
	metricsMux.HandleFunc("/bootnodes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		bootnodes := node.Node().Config().P2P.BootstrapNodesV5

		btNodes := make([]string, 0, len(bootnodes))

		for _, bootnode := range bootnodes {
			btNodes = append(btNodes, bootnode.String())
		}

		json.NewEncoder(w).Encode(btNodes)
	})
}
