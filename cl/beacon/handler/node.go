package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
)

/*
"peer_id": "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N",
"enr": "enr:-IS4QHCYrYZbAKWCBRlAy5zzaDZXJBGkcnh4MHcBFZntXNFrdvJjX04jRzjzCBOonrkTfj499SZuOh8R33Ls8RRcy5wBgmlkgnY0gmlwhH8AAAGJc2VjcDI1NmsxoQPKY0yuDUmstAHYpMa2_oxVtw0RW_QAdpzBQA8yWM0xOIN1ZHCCdl8",
"last_seen_p2p_address": "/ip4/7.7.7.7/tcp/4242/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N",
"state": "disconnected",
"direction": "inbound"
*/
type peer struct {
	PeerID             string `json:"peer_id"`
	State              string `json:"state"`
	Enr                string `json:"enr"`
	LastSeenP2PAddress string `json:"last_seen_p2p_address"`
	Direction          string `json:"direction"`
	AgentVersion       string `json:"agent_version"`
}

func (a *ApiHandler) GetEthV1NodeHealth(w http.ResponseWriter, r *http.Request) {
	syncingStatus, err := beaconhttp.Uint64FromQueryParams(r, "syncing_status")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	syncingCode := http.StatusOK
	if syncingStatus != nil {
		syncingCode = int(*syncingStatus)
	}
	if a.syncedData.Syncing() {
		w.WriteHeader(syncingCode)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) GetEthV1NodeVersion(w http.ResponseWriter, r *http.Request) {
	// Get OS and Arch
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"data": map[string]interface{}{
			"version": fmt.Sprintf("Caplin/%s %s/%s", a.version, runtime.GOOS, runtime.GOARCH),
		},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *ApiHandler) GetEthV1NodePeerCount(w http.ResponseWriter, r *http.Request) {
	ret, err := a.sentinel.GetPeers(r.Context(), &sentinel.EmptyMessage{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// all fields should be converted to string
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"data": map[string]interface{}{
			"connected":     strconv.FormatUint(ret.Connected, 10),
			"disconnected":  strconv.FormatUint(ret.Disconnected, 10),
			"connecting":    strconv.FormatUint(ret.Connecting, 10),
			"disconnecting": strconv.FormatUint(ret.Disconnecting, 10),
		},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *ApiHandler) GetEthV1NodePeersInfos(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	direction := r.URL.Query().Get("direction")

	var directionIn, stateIn *string
	if state != "" {
		stateIn = &state
	}
	if direction != "" {
		directionIn = &direction
	}

	ret, err := a.sentinel.PeersInfo(r.Context(), &sentinel.PeersInfoRequest{
		Direction: directionIn,
		State:     stateIn,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	peers := make([]peer, 0, len(ret.Peers))
	for i := range ret.Peers {
		peers = append(peers, peer{
			PeerID:             ret.Peers[i].Pid,
			State:              ret.Peers[i].State,
			Enr:                ret.Peers[i].Enr,
			LastSeenP2PAddress: ret.Peers[i].Address,
			Direction:          ret.Peers[i].Direction,
			AgentVersion:       ret.Peers[i].AgentVersion,
		})
	}
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"data": peers,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *ApiHandler) GetEthV1NodePeerInfos(w http.ResponseWriter, r *http.Request) {
	pid, err := beaconhttp.StringFromRequest(r, "peer_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ret, err := a.sentinel.PeersInfo(r.Context(), &sentinel.PeersInfoRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// find the peer with matching enr
	for _, p := range ret.Peers {
		if p.Pid == pid {
			if err := json.NewEncoder(w).Encode(map[string]interface{}{
				"data": peer{
					PeerID:             p.Pid,
					State:              p.State,
					Enr:                p.Enr,
					LastSeenP2PAddress: p.Address,
					Direction:          p.Direction,
					AgentVersion:       p.AgentVersion,
				},
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
	}
	http.Error(w, "peer not found", http.StatusNotFound)
}

func (a *ApiHandler) GetEthV1NodeIdentity(w http.ResponseWriter, r *http.Request) {
	id, err := a.sentinel.Identity(r.Context(), &sentinel.EmptyMessage{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"data": map[string]interface{}{
			"peer_id":             id.Pid,
			"enr":                 id.Enr,
			"p2p_addresses":       id.P2PAddresses,
			"discovery_addresses": id.DiscoveryAddresses,
			"metadata": map[string]interface{}{
				"seq":      strconv.FormatUint(id.Metadata.Seq, 10),
				"attnets":  id.Metadata.Attnets,
				"syncnets": id.Metadata.Syncnets,
			},
		},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}
