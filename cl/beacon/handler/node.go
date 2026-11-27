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
	"slices"
	"strconv"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

/*
"peer_id": "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N",
"enr": "enr:-IS4QHCYrYZbAKWCBRlAy5zzaDZXJBGkcnh4MHcBFZntXNFrdvJjX04jRzjzCBOonrkTfj499SZuOh8R33Ls8RRcy5wBgmlkgnY0gmlwhH8AAAGJc2VjcDI1NmsxoQPKY0yuDUmstAHYpMa2_oxVtw0RW_QAdpzBQA8yWM0xOIN1ZHCCdl8",
"last_seen_p2p_address": "/ip4/7.7.7.7/tcp/4242/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N",
"state": "disconnected",
"direction": "inbound"
*/
type peer struct {
	PeerID             string  `json:"peer_id"`
	State              string  `json:"state"`
	Enr                *string `json:"enr"`
	LastSeenP2PAddress string  `json:"last_seen_p2p_address"`
	Direction          string  `json:"direction"`
	AgentVersion       string  `json:"agent_version"`
}

func (a *ApiHandler) GetEthV1NodeHealth(w http.ResponseWriter, r *http.Request) {
	syncingStatus, err := beaconhttp.Uint64FromQueryParams(r, "syncing_status")
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	syncingCode := http.StatusPartialContent
	if syncingStatus != nil {
		if *syncingStatus < 100 || *syncingStatus > 599 {
			beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("syncing_status must be between 100 and 599, got %d", *syncingStatus)).WriteTo(w)
			return
		}
		syncingCode = int(*syncingStatus)
	}
	if a.headLagsBehind() {
		w.WriteHeader(syncingCode)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) GetEthV1NodeVersion(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	// Get OS and Arch
	return newBeaconResponse(map[string]any{
		"version": fmt.Sprintf("Caplin/%s %s/%s", a.version, runtime.GOOS, runtime.GOARCH),
	}), nil
}

func (a *ApiHandler) GetEthV1NodePeerCount(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ret, err := a.sentinel.GetPeers(r.Context(), &sentinelproto.EmptyMessage{})
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}

	// all fields should be converted to string
	return newBeaconResponse(map[string]any{
		"connected":     strconv.FormatUint(ret.Connected, 10),
		"disconnected":  strconv.FormatUint(ret.Disconnected, 10),
		"connecting":    strconv.FormatUint(ret.Connecting, 10),
		"disconnecting": strconv.FormatUint(ret.Disconnecting, 10),
	}), nil
}

var (
	peerStates     = []string{"disconnected", "connecting", "connected", "disconnecting"}
	peerDirections = []string{"inbound", "outbound"}
)

// peerFilter reads a repeated query parameter. An empty filter matches every value.
func peerFilter(r *http.Request, name string, allowed []string) (map[string]struct{}, error) {
	values := r.URL.Query()[name]
	filter := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !slices.Contains(allowed, value) {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid %s: %q", name, value))
		}
		filter[value] = struct{}{}
	}
	return filter, nil
}

func matchesPeerFilter(filter map[string]struct{}, value string) bool {
	_, ok := filter[value]
	return len(filter) == 0 || ok
}

func newPeer(p *sentinelproto.Peer) peer {
	var enr *string
	if p.Enr != "" {
		enr = &p.Enr
	}
	return peer{
		PeerID:             p.Pid,
		State:              p.State,
		Enr:                enr,
		LastSeenP2PAddress: p.Address,
		Direction:          p.Direction,
		AgentVersion:       p.AgentVersion,
	}
}

func (a *ApiHandler) GetEthV1NodePeersInfos(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	states, err := peerFilter(r, "state", peerStates)
	if err != nil {
		return nil, err
	}
	directions, err := peerFilter(r, "direction", peerDirections)
	if err != nil {
		return nil, err
	}

	ret, err := a.sentinel.PeersInfo(r.Context(), &sentinelproto.PeersInfoRequest{})
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	peers := make([]peer, 0, len(ret.Peers))
	for _, p := range ret.Peers {
		if matchesPeerFilter(states, p.State) && matchesPeerFilter(directions, p.Direction) {
			peers = append(peers, newPeer(p))
		}
	}

	return newBeaconResponse(peers), nil
}

func (a *ApiHandler) GetEthV1NodePeerInfos(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	pid, err := beaconhttp.StringFromRequest(r, "peer_id")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	ret, err := a.sentinel.PeersInfo(r.Context(), &sentinelproto.PeersInfoRequest{})
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	for _, p := range ret.Peers {
		if p.Pid == pid {
			return newBeaconResponse(newPeer(p)), nil
		}
	}

	return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("peer not found"))
}

func (a *ApiHandler) GetEthV1NodeIdentity(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	id, err := a.sentinel.Identity(r.Context(), &sentinelproto.EmptyMessage{})
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}

	metadata := map[string]any{
		"seq_number": strconv.FormatUint(id.Metadata.Seq, 10),
		"attnets":    "0x" + id.Metadata.Attnets,
		"syncnets":   "0x" + id.Metadata.Syncnets,
	}
	if a.ethClock.GetCurrentEpoch() >= a.beaconChainCfg.FuluForkEpoch {
		metadata["custody_group_count"] = strconv.FormatUint(a.peerDas.StateReader().GetAdvertisedCgc(), 10)
	}
	return newBeaconResponse(map[string]any{
		"peer_id":             id.Pid,
		"enr":                 id.Enr,
		"p2p_addresses":       id.P2PAddresses,
		"discovery_addresses": id.DiscoveryAddresses,
		"metadata":            metadata,
	}), nil
}

func (a *ApiHandler) GetEthV1NodeSyncing(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	currentSlot := a.ethClock.GetCurrentSlot()
	headSlot := a.syncedData.HeadSlot()
	var syncDistance uint64
	if currentSlot > headSlot {
		syncDistance = currentSlot - headSlot
	}

	return newBeaconResponse(
		map[string]any{
			"head_slot":     strconv.FormatUint(headSlot, 10),
			"sync_distance": strconv.FormatUint(syncDistance, 10),
			"is_syncing":    a.headLagsBehind(),
			"is_optimistic": false, // needs to change
			"el_offline":    false,
		},
	), nil
}
