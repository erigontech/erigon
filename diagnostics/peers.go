package diagnostics

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/urfave/cli/v2"
)

func SetupPeersAccess(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) {
	metricsMux.HandleFunc("/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writePeers(w, ctx, node)
	})
}

func writePeers(w http.ResponseWriter, ctx *cli.Context, node *node.ErigonNode) {
	reply, err := node.Backend().Peers(context.Background())

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	peers := make([]*p2p.PeerInfo, 0, len(reply.Peers))

	for _, rpcPeer := range reply.Peers {
		peer := p2p.PeerInfo{
			ENR:   rpcPeer.Enr,
			Enode: rpcPeer.Enode,
			ID:    rpcPeer.Id,
			Name:  rpcPeer.Name,
			Caps:  rpcPeer.Caps,
			Network: struct {
				LocalAddress  string `json:"localAddress"`
				RemoteAddress string `json:"remoteAddress"`
				Inbound       bool   `json:"inbound"`
				Trusted       bool   `json:"trusted"`
				Static        bool   `json:"static"`
			}{
				LocalAddress:  rpcPeer.ConnLocalAddr,
				RemoteAddress: rpcPeer.ConnRemoteAddr,
				Inbound:       rpcPeer.ConnIsInbound,
				Trusted:       rpcPeer.ConnIsTrusted,
				Static:        rpcPeer.ConnIsStatic,
			},
			Protocols: nil,
		}

		peers = append(peers, &peer)
	}

	json.NewEncoder(w).Encode(peers)
}
