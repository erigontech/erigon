// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package diagnostics

import (
	"encoding/json"
	"net/http"

	"github.com/erigontech/erigon/turbo/node"
)

func SetupBootnodesAccess(metricsMux *http.ServeMux, node *node.ErigonNode) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/bootnodes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		bootnodes := node.Node().Config().P2P.BootstrapNodesV5

		btNodes := make([]string, 0, len(bootnodes))

		for _, bootnode := range bootnodes {
			btNodes = append(btNodes, bootnode.String())
		}

		json.NewEncoder(w).Encode(btNodes)
	})
}
