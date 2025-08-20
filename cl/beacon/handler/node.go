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

package handler

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strconv"

	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
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
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
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

func (a *ApiHandler) GetEthV1NodeVersion(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	// Get OS and Arch
	return newBeaconResponse(map[string]interface{}{
		"version": fmt.Sprintf("Caplin/%s %s/%s", a.version, runtime.GOOS, runtime.GOARCH),
	}), nil
}

func (a *ApiHandler) GetEthV1NodePeerCount(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ret, err := a.sentinel.GetPeers(r.Context(), &sentinel.EmptyMessage{})
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}

	// all fields should be converted to string
	return newBeaconResponse(map[string]interface{}{
		"connected":     strconv.FormatUint(ret.Connected, 10),
		"disconnected":  strconv.FormatUint(ret.Disconnected, 10),
		"connecting":    strconv.FormatUint(ret.Connecting, 10),
		"disconnecting": strconv.FormatUint(ret.Disconnecting, 10),
	}), nil
}

func (a *ApiHandler) GetEthV1NodePeersInfos(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
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

	return newBeaconResponse(peers), nil
}

func (a *ApiHandler) GetEthV1NodePeerInfos(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	pid, err := beaconhttp.StringFromRequest(r, "peer_id")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	ret, err := a.sentinel.PeersInfo(r.Context(), &sentinel.PeersInfoRequest{})
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	// find the peer with matching enr
	for _, p := range ret.Peers {
		if p.Pid == pid {
			return newBeaconResponse(peer{
				PeerID:             p.Pid,
				State:              p.State,
				Enr:                p.Enr,
				LastSeenP2PAddress: p.Address,
				Direction:          p.Direction,
				AgentVersion:       p.AgentVersion,
			}), nil
		}
	}

	return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("peer not found"))
}

func (a *ApiHandler) GetEthV1NodeIdentity(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	id, err := a.sentinel.Identity(r.Context(), &sentinel.EmptyMessage{})
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}

	return newBeaconResponse(map[string]interface{}{
		"peer_id":             id.Pid,
		"enr":                 id.Enr,
		"p2p_addresses":       id.P2PAddresses,
		"discovery_addresses": id.DiscoveryAddresses,
		"metadata": map[string]interface{}{
			"seq":      strconv.FormatUint(id.Metadata.Seq, 10),
			"attnets":  id.Metadata.Attnets,
			"syncnets": id.Metadata.Syncnets,
		},
	}), nil
}

func (a *ApiHandler) GetEthV1NodeSyncing(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	currentSlot := a.ethClock.GetCurrentSlot()

	return newBeaconResponse(
		map[string]interface{}{
			"head_slot":     strconv.FormatUint(a.syncedData.HeadSlot(), 10),
			"sync_distance": strconv.FormatUint(currentSlot-a.syncedData.HeadSlot(), 10),
			"is_syncing":    a.syncedData.Syncing(),
			"is_optimistic": false, // needs to change
			"el_offline":    false,
		}), nil
}
