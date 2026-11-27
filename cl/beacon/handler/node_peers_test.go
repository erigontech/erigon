// Copyright 2026 The Erigon Authors
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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type peersSentinelClient struct {
	sentinelproto.SentinelClient
	peers []*sentinelproto.Peer
}

func (c peersSentinelClient) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{Peers: c.peers}, nil
}

func testPeers() []*sentinelproto.Peer {
	return []*sentinelproto.Peer{
		{Pid: "dialed", Enr: "enr:-dialed", State: "connected", Direction: "outbound", AgentVersion: "a"},
		{Pid: "inbound", State: "connected", Direction: "inbound", AgentVersion: "b"},
		{Pid: "gone", State: "disconnected", Direction: "inbound", AgentVersion: "c"},
	}
}

func peerIDs(t *testing.T, resp *beaconhttp.BeaconResponse) []string {
	t.Helper()
	ids := []string{}
	for _, p := range resp.Data.([]peer) {
		ids = append(ids, p.PeerID)
	}
	return ids
}

func getPeers(t *testing.T, query string) (*beaconhttp.BeaconResponse, error) {
	t.Helper()
	a := &ApiHandler{sentinel: peersSentinelClient{peers: testPeers()}}
	return a.GetEthV1NodePeersInfos(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/node/peers"+query, http.NoBody))
}

func TestNodePeersListsPeersWithoutENR(t *testing.T) {
	resp, err := getPeers(t, "")
	require.NoError(t, err)
	require.Equal(t, []string{"dialed", "inbound", "gone"}, peerIDs(t, resp))

	encoded, err := json.Marshal(resp.Data.([]peer)[1])
	require.NoError(t, err)
	require.JSONEq(t, `{"peer_id":"inbound","enr":null,"last_seen_p2p_address":"","state":"connected","direction":"inbound","agent_version":"b"}`, string(encoded))
}

func TestNodePeersFiltersByEveryRequestedValue(t *testing.T) {
	for _, test := range []struct {
		query string
		want  []string
	}{
		{query: "?direction=inbound&direction=outbound", want: []string{"dialed", "inbound", "gone"}},
		{query: "?state=connected&state=disconnected&direction=inbound", want: []string{"inbound", "gone"}},
		{query: "?state=connected", want: []string{"dialed", "inbound"}},
		{query: "?state=connecting", want: []string{}},
	} {
		t.Run(test.query, func(t *testing.T) {
			resp, err := getPeers(t, test.query)
			require.NoError(t, err)
			require.Equal(t, test.want, peerIDs(t, resp))
		})
	}
}

func TestNodePeersRejectsUnknownFilterValues(t *testing.T) {
	for _, query := range []string{"?state=online", "?direction=sideways"} {
		_, err := getPeers(t, query)
		var endpointErr *beaconhttp.EndpointError
		require.ErrorAs(t, err, &endpointErr, query)
		require.Equal(t, http.StatusBadRequest, endpointErr.Code, query)
	}
}

func TestNodePeerFindsPeerWithoutENR(t *testing.T) {
	a := &ApiHandler{sentinel: peersSentinelClient{peers: testPeers()}}
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("peer_id", "inbound")
	req := httptest.NewRequestWithContext(context.WithValue(t.Context(), chi.RouteCtxKey, routeCtx), http.MethodGet, "/eth/v1/node/peers/inbound", http.NoBody)

	resp, err := a.GetEthV1NodePeerInfos(httptest.NewRecorder(), req)

	require.NoError(t, err)
	require.Equal(t, "inbound", resp.Data.(peer).PeerID)
	require.Nil(t, resp.Data.(peer).Enr)
}
