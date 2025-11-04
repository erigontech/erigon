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

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/turbo/node"
)

type PeerNetworkInfo struct {
	LocalAddress  string            `json:"localAddress"`  // Local endpoint of the TCP data connection
	RemoteAddress string            `json:"remoteAddress"` // Remote endpoint of the TCP data connection
	Inbound       bool              `json:"inbound"`
	Trusted       bool              `json:"trusted"`
	Static        bool              `json:"static"`
	BytesIn       uint64            `json:"bytesIn"`
	BytesOut      uint64            `json:"bytesOut"`
	CapBytesIn    map[string]uint64 `json:"capBytesIn"`
	CapBytesOut   map[string]uint64 `json:"capBytesOut"`
	TypeBytesIn   map[string]uint64 `json:"typeBytesIn"`
	TypeBytesOut  map[string]uint64 `json:"typeBytesOut"`
}

type PeerResponse struct {
	ENR           string                 `json:"enr,omitempty"` // Ethereum Node Record
	Enode         string                 `json:"enode"`         // Node URL
	ID            string                 `json:"id"`            // Unique node identifier
	Name          string                 `json:"name"`          // Name of the node, including client type, version, OS, custom data
	ErrorCount    int                    `json:"errorCount"`    // Number of errors
	LastSeenError string                 `json:"lastSeenError"` // Last seen error
	Type          string                 `json:"type"`          // Type of connection
	Caps          []string               `json:"caps"`          // Protocols advertised by this peer
	Network       PeerNetworkInfo        `json:"network"`
	Protocols     map[string]interface{} `json:"protocols"` // Sub-protocol specific metadata fields
}

func SetupPeersAccess(ctxclient *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode, diag *diaglib.DiagnosticClient) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writePeers(w, ctxclient, node, diag)
	})
}

func writePeers(w http.ResponseWriter, ctx *cli.Context, node *node.ErigonNode, diag *diaglib.DiagnosticClient) {
	allPeers := peers(diag)
	filteredPeers := filterPeersWithoutBytesIn(allPeers)

	json.NewEncoder(w).Encode(filteredPeers)
}

func peers(diag *diaglib.DiagnosticClient) []*PeerResponse {

	statisticsArray := diag.Peers()
	peers := make([]*PeerResponse, 0, len(statisticsArray))

	for key, value := range statisticsArray {
		peer := PeerResponse{
			ENR:   "", //TODO: find a way how to get missing data
			Enode: "",
			ID:    key,
			Name:  value.PeerName,
			Type:  value.PeerType,
			Caps:  []string{},
			Network: PeerNetworkInfo{
				LocalAddress:  "",
				RemoteAddress: "",
				Inbound:       false,
				Trusted:       false,
				Static:        false,
				BytesIn:       value.BytesIn,
				BytesOut:      value.BytesOut,
				CapBytesIn:    value.CapBytesIn,
				CapBytesOut:   value.CapBytesOut,
				TypeBytesIn:   value.TypeBytesIn,
				TypeBytesOut:  value.TypeBytesOut,
			},
			Protocols: nil,
		}

		peers = append(peers, &peer)
	}

	return peers
}

func filterPeersWithoutBytesIn(peers []*PeerResponse) []*PeerResponse {
	filteredPeers := make([]*PeerResponse, 0, len(peers))

	for _, peer := range peers {
		if peer.Network.BytesIn > 0 {
			filteredPeers = append(filteredPeers, peer)
		}
	}

	return filteredPeers
}
