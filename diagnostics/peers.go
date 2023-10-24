package diagnostics

import (
	"encoding/json"
	"net/http"

	diagnint "github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/urfave/cli/v2"
)

type PeerNetworkInfo struct {
	LocalAddress  string         `json:"localAddress"`  // Local endpoint of the TCP data connection
	RemoteAddress string         `json:"remoteAddress"` // Remote endpoint of the TCP data connection
	Inbound       bool           `json:"inbound"`
	Trusted       bool           `json:"trusted"`
	Static        bool           `json:"static"`
	BytesIn       int            `json:"bytesIn"`
	BytesOut      int            `json:"bytesOut"`
	CapBytesIn    map[string]int `json:"capBytesIn"`
	CapBytesOut   map[string]int `json:"capBytesOut"`
	TypeBytesIn   map[string]int `json:"typeBytesIn"`
	TypeBytesOut  map[string]int `json:"typeBytesOut"`
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

func SetupPeersAccess(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) {
	metricsMux.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writePeers(w, ctx, node)
	})
}

func writePeers(w http.ResponseWriter, ctx *cli.Context, node *node.ErigonNode) {
	sentinelPeers, err := sentinelPeers(node)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sentryPeers, err := sentryPeers(node)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	allPeers := append(sentryPeers, sentinelPeers...)

	json.NewEncoder(w).Encode(allPeers)
}

func sentinelPeers(node *node.ErigonNode) ([]*PeerResponse, error) {
	if diag, ok := node.Backend().Sentinel().(diagnint.PeerStatisticsGetter); ok {

		statisticsArray := diag.GetPeersStatistics()
		peers := make([]*PeerResponse, 0, len(statisticsArray))

		for key, value := range statisticsArray {
			peer := PeerResponse{
				ENR:   "", //TODO: find a way how to get missing data
				Enode: "",
				ID:    key,
				Name:  "",
				Type:  "Sentinel",
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

		return peers, nil
	} else {
		return []*PeerResponse{}, nil
	}
}

func sentryPeers(node *node.ErigonNode) ([]*PeerResponse, error) {

	reply := node.Backend().DiagnosticsPeersData()

	peers := make([]*PeerResponse, 0, len(reply))

	for _, rpcPeer := range reply {
		var bytesIn = 0
		var bytesOut = 0

		var capBytesIn = map[string]int{}
		var capBytesOut = map[string]int{}

		var typeBytesIn = map[string]int{}
		var typeBytesOut = map[string]int{}

		if rpcPeer.Network.Inbound {
			bytesIn = rpcPeer.BytesTransfered
			capBytesIn = rpcPeer.CapBytesTransfered
			typeBytesIn = rpcPeer.TypeBytesTransfered
		} else {
			bytesOut = rpcPeer.BytesTransfered
			capBytesOut = rpcPeer.CapBytesTransfered
			typeBytesOut = rpcPeer.TypeBytesTransfered
		}

		peer := PeerResponse{
			ENR:   rpcPeer.ENR,
			Enode: rpcPeer.Enode,
			ID:    rpcPeer.ID,
			Name:  rpcPeer.Name,
			Type:  "Sentry",
			Caps:  rpcPeer.Caps,
			Network: PeerNetworkInfo{
				LocalAddress:  rpcPeer.Network.LocalAddress,
				RemoteAddress: rpcPeer.Network.RemoteAddress,
				Inbound:       rpcPeer.Network.Inbound,
				Trusted:       rpcPeer.Network.Trusted,
				Static:        rpcPeer.Network.Static,
				BytesIn:       bytesIn,
				BytesOut:      bytesOut,
				CapBytesIn:    capBytesIn,
				CapBytesOut:   capBytesOut,
				TypeBytesIn:   typeBytesIn,
				TypeBytesOut:  typeBytesOut,
			},
			Protocols: nil,
		}

		peers = append(peers, &peer)
	}

	return peers, nil
}
