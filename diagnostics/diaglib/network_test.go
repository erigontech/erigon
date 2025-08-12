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

package diaglib_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/diagnostics/diaglib"
)

var mockInboundPeerStats = diaglib.PeerStatistics{
	PeerType:     "Sentinel",
	BytesIn:      10,
	CapBytesIn:   map[string]uint64{"msgCap1": 10},
	TypeBytesIn:  map[string]uint64{"msgType1": 10},
	BytesOut:     0,
	CapBytesOut:  map[string]uint64{},
	TypeBytesOut: map[string]uint64{},
}

var mockOutboundPeerStats = diaglib.PeerStatistics{
	PeerType:     "Sentinel",
	BytesIn:      0,
	CapBytesIn:   map[string]uint64{},
	TypeBytesIn:  map[string]uint64{},
	BytesOut:     10,
	CapBytesOut:  map[string]uint64{"msgCap1": 10},
	TypeBytesOut: map[string]uint64{"msgType1": 10},
}

var mockInboundUpdMsg = diaglib.PeerStatisticMsgUpdate{
	PeerName: "",
	PeerType: "Sentinel",
	PeerID:   "test1",
	Inbound:  true,
	MsgType:  "msgType1",
	MsgCap:   "msgCap1",
	Bytes:    10,
}

var mockOutboundUpdMsg = diaglib.PeerStatisticMsgUpdate{
	PeerName: "",
	PeerType: "Sentinel",
	PeerID:   "test1",
	Inbound:  false,
	MsgType:  "msgType1",
	MsgCap:   "msgCap1",
	Bytes:    10,
}

func TestPeerStatisticsFromMsgUpdate(t *testing.T) {
	//test handing inbound message
	inboundPeerStats := diaglib.PeerStatisticsFromMsgUpdate(mockInboundUpdMsg, nil)
	require.Equal(t, mockInboundPeerStats, inboundPeerStats)

	inboundPeerStats = diaglib.PeerStatisticsFromMsgUpdate(mockInboundUpdMsg, inboundPeerStats)

	require.Equal(t, diaglib.PeerStatistics{
		PeerType:     "Sentinel",
		BytesIn:      20,
		CapBytesIn:   map[string]uint64{"msgCap1": 20},
		TypeBytesIn:  map[string]uint64{"msgType1": 20},
		BytesOut:     0,
		CapBytesOut:  map[string]uint64{},
		TypeBytesOut: map[string]uint64{},
	}, inboundPeerStats)

	//test handing outbound message
	outboundPeerStats := diaglib.PeerStatisticsFromMsgUpdate(mockOutboundUpdMsg, nil)
	require.Equal(t, mockOutboundPeerStats, outboundPeerStats)

	outboundPeerStats = diaglib.PeerStatisticsFromMsgUpdate(mockOutboundUpdMsg, outboundPeerStats)

	require.Equal(t, diaglib.PeerStatistics{
		PeerType:     "Sentinel",
		BytesIn:      0,
		CapBytesIn:   map[string]uint64{},
		TypeBytesIn:  map[string]uint64{},
		BytesOut:     20,
		CapBytesOut:  map[string]uint64{"msgCap1": 20},
		TypeBytesOut: map[string]uint64{"msgType1": 20},
	}, outboundPeerStats)

}

func TestAddPeer(t *testing.T) {
	var peerStats = diaglib.NewPeerStats(100)

	peerStats.AddPeer("test1", mockInboundUpdMsg)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, mockInboundPeerStats, peerStats.GetPeerStatistics("test1"))
}

func TestUpdatePeer(t *testing.T) {
	peerStats := diaglib.NewPeerStats(1000)

	peerStats.AddPeer("test1", mockInboundUpdMsg)
	peerStats.UpdatePeer("test1", mockInboundUpdMsg, mockInboundPeerStats)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, diaglib.PeerStatistics{
		PeerType:     "Sentinel",
		BytesIn:      20,
		CapBytesIn:   map[string]uint64{"msgCap1": 20},
		TypeBytesIn:  map[string]uint64{"msgType1": 20},
		BytesOut:     0,
		CapBytesOut:  map[string]uint64{},
		TypeBytesOut: map[string]uint64{},
	}, peerStats.GetPeerStatistics("test1"))
}

func TestAddOrUpdatePeer(t *testing.T) {
	peerStats := diaglib.NewPeerStats(100)

	peerStats.AddOrUpdatePeer("test1", mockInboundUpdMsg)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, mockInboundPeerStats, peerStats.GetPeerStatistics("test1"))

	peerStats.AddOrUpdatePeer("test1", mockInboundUpdMsg)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, diaglib.PeerStatistics{
		PeerType:     "Sentinel",
		BytesIn:      20,
		CapBytesIn:   map[string]uint64{"msgCap1": 20},
		TypeBytesIn:  map[string]uint64{"msgType1": 20},
		BytesOut:     0,
		CapBytesOut:  map[string]uint64{},
		TypeBytesOut: map[string]uint64{},
	}, peerStats.GetPeerStatistics("test1"))

	peerStats.AddOrUpdatePeer("test2", mockInboundUpdMsg)
	require.Equal(t, 2, peerStats.GetPeersCount())
}

func TestGetPeers(t *testing.T) {
	peerStats := diaglib.NewPeerStats(10)

	peerStats.AddOrUpdatePeer("test1", mockInboundUpdMsg)
	peerStats.AddOrUpdatePeer("test2", mockInboundUpdMsg)
	peerStats.AddOrUpdatePeer("test3", mockInboundUpdMsg)

	peers := peerStats.GetPeers()
	require.Len(t, peers, 3)
	require.True(t, peers["test1"].Equal(mockInboundPeerStats))
}

func TestRemovePeersWhichExceedLimit(t *testing.T) {
	limit := 100
	peerStats := diaglib.NewPeerStats(limit)

	for i := 1; i < 105; i++ {
		pid := "test" + strconv.Itoa(i)
		peerStats.AddOrUpdatePeer(pid, mockInboundUpdMsg)
	}
	require.Equal(t, 100, peerStats.GetPeersCount())

	peerStats.RemovePeersWhichExceedLimit(limit)

	require.Equal(t, limit, peerStats.GetPeersCount())

	limit = 1000
	peerStats.RemovePeersWhichExceedLimit(limit)

	require.Equal(t, 100, peerStats.GetPeersCount())
}

func TestRemovePeer(t *testing.T) {
	limit := 10
	peerStats := diaglib.NewPeerStats(limit)

	for i := 1; i < 11; i++ {
		pid := "test" + strconv.Itoa(i)
		peerStats.AddOrUpdatePeer(pid, mockInboundUpdMsg)
	}
	require.Equal(t, 10, peerStats.GetPeersCount())

	peerStats.RemovePeer("test1")

	require.Equal(t, limit-1, peerStats.GetPeersCount())

	firstPeerStats := peerStats.GetPeerStatistics("test1")
	require.True(t, firstPeerStats.Equal(diaglib.PeerStatistics{}))
}

func TestAddingPeersAboveTheLimit(t *testing.T) {
	limit := 100
	peerStats := diaglib.NewPeerStats(limit)

	for i := 1; i < 105; i++ {
		pid := "test" + strconv.Itoa(i)
		peerStats.AddOrUpdatePeer(pid, mockInboundUpdMsg)
	}

	require.Equal(t, limit, peerStats.GetPeersCount())

	peerStats.AddOrUpdatePeer("test105", mockInboundUpdMsg)

	require.Equal(t, limit, peerStats.GetPeersCount())
}
