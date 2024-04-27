package diagnostics_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/stretchr/testify/require"
)

var mockInboundPeerStats = diagnostics.PeerStatistics{
	PeerType:     "Sentinel",
	BytesIn:      10,
	CapBytesIn:   map[string]uint64{"msgCap1": 10},
	TypeBytesIn:  map[string]uint64{"msgType1": 10},
	BytesOut:     0,
	CapBytesOut:  map[string]uint64{},
	TypeBytesOut: map[string]uint64{},
}

var mockOutboundPeerStats = diagnostics.PeerStatistics{
	PeerType:     "Sentinel",
	BytesIn:      0,
	CapBytesIn:   map[string]uint64{},
	TypeBytesIn:  map[string]uint64{},
	BytesOut:     10,
	CapBytesOut:  map[string]uint64{"msgCap1": 10},
	TypeBytesOut: map[string]uint64{"msgType1": 10},
}

var mockInboundUpdMsg = diagnostics.PeerStatisticMsgUpdate{
	PeerType: "Sentinel",
	PeerID:   "test1",
	Inbound:  true,
	MsgType:  "msgType1",
	MsgCap:   "msgCap1",
	Bytes:    10,
}

var mockOutboundUpdMsg = diagnostics.PeerStatisticMsgUpdate{
	PeerType: "Sentinel",
	PeerID:   "test1",
	Inbound:  false,
	MsgType:  "msgType1",
	MsgCap:   "msgCap1",
	Bytes:    10,
}

func TestPeerStatisticsFromMsgUpdate(t *testing.T) {
	//test handing inbound message
	inboundPeerStats := diagnostics.PeerStatisticsFromMsgUpdate(mockInboundUpdMsg, nil)
	require.Equal(t, mockInboundPeerStats, inboundPeerStats)

	inboundPeerStats = diagnostics.PeerStatisticsFromMsgUpdate(mockInboundUpdMsg, inboundPeerStats)

	require.Equal(t, diagnostics.PeerStatistics{
		PeerType:     "Sentinel",
		BytesIn:      20,
		CapBytesIn:   map[string]uint64{"msgCap1": 20},
		TypeBytesIn:  map[string]uint64{"msgType1": 20},
		BytesOut:     0,
		CapBytesOut:  map[string]uint64{},
		TypeBytesOut: map[string]uint64{},
	}, inboundPeerStats)

	//test handing outbound message
	outboundPeerStats := diagnostics.PeerStatisticsFromMsgUpdate(mockOutboundUpdMsg, nil)
	require.Equal(t, mockOutboundPeerStats, outboundPeerStats)

	outboundPeerStats = diagnostics.PeerStatisticsFromMsgUpdate(mockOutboundUpdMsg, outboundPeerStats)

	require.Equal(t, diagnostics.PeerStatistics{
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
	var peerStats = diagnostics.NewPeerStats(100)

	peerStats.AddPeer("test1", mockInboundUpdMsg)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, mockInboundPeerStats, peerStats.GetPeerStatistics("test1"))
}

func TestUpdatePeer(t *testing.T) {
	peerStats := diagnostics.NewPeerStats(1000)

	peerStats.AddPeer("test1", mockInboundUpdMsg)
	peerStats.UpdatePeer("test1", mockInboundUpdMsg, mockInboundPeerStats)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, diagnostics.PeerStatistics{
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
	peerStats := diagnostics.NewPeerStats(100)

	peerStats.AddOrUpdatePeer("test1", mockInboundUpdMsg)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, mockInboundPeerStats, peerStats.GetPeerStatistics("test1"))

	peerStats.AddOrUpdatePeer("test1", mockInboundUpdMsg)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, diagnostics.PeerStatistics{
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
	peerStats := diagnostics.NewPeerStats(10)

	peerStats.AddOrUpdatePeer("test1", mockInboundUpdMsg)
	peerStats.AddOrUpdatePeer("test2", mockInboundUpdMsg)
	peerStats.AddOrUpdatePeer("test3", mockInboundUpdMsg)

	peers := peerStats.GetPeers()
	require.Equal(t, 3, len(peers))
	require.Equal(t, &mockInboundPeerStats, peers["test1"])
}

func TestLastUpdated(t *testing.T) {
	peerStats := diagnostics.NewPeerStats(1000)

	peerStats.AddOrUpdatePeer("test1", mockInboundUpdMsg)
	require.NotEmpty(t, peerStats.GetLastUpdate("test1"))

	for i := 1; i < 20; i++ {
		pid := "test" + strconv.Itoa(i)
		peerStats.AddOrUpdatePeer(pid, mockInboundUpdMsg)
		//wait for 1 milisecond to make sure that the last update time is different
		time.Sleep(10 * time.Millisecond)
	}

	require.True(t, peerStats.GetLastUpdate("test2").After(peerStats.GetLastUpdate("test1")))

	oldestPeers := peerStats.GetOldestUpdatedPeersWithSize(10)

	// we have 100 peers, but we should get only 10 oldest
	require.Equal(t, len(oldestPeers), 10)
	// the oldest peer should be test1
	require.Equal(t, "test1", oldestPeers[0].PeerID)

	// update test1 to
	peerStats.AddOrUpdatePeer("test1", mockInboundUpdMsg)
	oldestPeers = peerStats.GetOldestUpdatedPeersWithSize(10)

	// the oldest peer should not be test1
	require.NotEqual(t, "test1", oldestPeers[0].PeerID)
}

func TestRemovePeersWhichExceedLimit(t *testing.T) {
	limit := 100
	peerStats := diagnostics.NewPeerStats(limit)

	for i := 1; i < 105; i++ {
		pid := "test" + strconv.Itoa(i)
		peerStats.AddOrUpdatePeer(pid, mockInboundUpdMsg)
	}

	peerStats.RemovePeersWhichExceedLimit(limit)

	require.Equal(t, limit, peerStats.GetPeersCount())

	limit = 1000
	peerStats.RemovePeersWhichExceedLimit(limit)

	require.Equal(t, 100, peerStats.GetPeersCount())
}

func TestAddingPeersAboveTheLimit(t *testing.T) {
	limit := 100
	peerStats := diagnostics.NewPeerStats(limit)

	for i := 1; i < 105; i++ {
		pid := "test" + strconv.Itoa(i)
		peerStats.AddOrUpdatePeer(pid, mockInboundUpdMsg)
	}

	require.Equal(t, limit, peerStats.GetPeersCount())

	peerStats.AddOrUpdatePeer("test105", mockInboundUpdMsg)

	require.Equal(t, limit, peerStats.GetPeersCount())
}
