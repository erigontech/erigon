package diagnostics_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/stretchr/testify/require"
)

var testPeerStats = diagnostics.PeerStatistics{
	PeerType:     "Sentinel",
	BytesIn:      10,
	CapBytesIn:   map[string]uint64{"msgCap1": 10},
	TypeBytesIn:  map[string]uint64{"msgType1": 10},
	BytesOut:     0,
	CapBytesOut:  map[string]uint64{},
	TypeBytesOut: map[string]uint64{},
}

var testUpdMsg = diagnostics.PeerStatisticMsgUpdate{
	PeerType: "Sentinel",
	PeerID:   "test1",
	Inbound:  true,
	MsgType:  "msgType1",
	MsgCap:   "msgCap1",
	Bytes:    10,
}

func TestPeerStatisticsFromMsgUpdate(t *testing.T) {
	ps := diagnostics.PeerStatisticsFromMsgUpdate(testUpdMsg, nil)
	require.Equal(t, testPeerStats, ps)

	ps1 := diagnostics.PeerStatisticsFromMsgUpdate(testUpdMsg, ps)

	require.Equal(t, diagnostics.PeerStatistics{
		PeerType:     "Sentinel",
		BytesIn:      20,
		CapBytesIn:   map[string]uint64{"msgCap1": 20},
		TypeBytesIn:  map[string]uint64{"msgType1": 20},
		BytesOut:     0,
		CapBytesOut:  map[string]uint64{},
		TypeBytesOut: map[string]uint64{},
	}, ps1)
}

func TestAddPeer(t *testing.T) {
	var peerStats = diagnostics.NewPeerStats(100)

	peerStats.AddPeer("test1", testUpdMsg)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, testPeerStats, peerStats.GetPeerStatistics("test1"))
}

func TestUpdatePeer(t *testing.T) {
	peerStats := diagnostics.NewPeerStats(1000)

	peerStats.AddPeer("test1", testUpdMsg)
	peerStats.UpdatePeer("test1", testUpdMsg, testPeerStats)
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

	peerStats.AddOrUpdatePeer("test1", testUpdMsg)
	require.Equal(t, 1, peerStats.GetPeersCount())

	require.Equal(t, testPeerStats, peerStats.GetPeerStatistics("test1"))

	peerStats.AddOrUpdatePeer("test1", testUpdMsg)
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

	peerStats.AddOrUpdatePeer("test2", testUpdMsg)
	require.Equal(t, 2, peerStats.GetPeersCount())
}

func TestLastUpdated(t *testing.T) {
	peerStats := diagnostics.NewPeerStats(1000)

	peerStats.AddOrUpdatePeer("test1", testUpdMsg)
	require.NotEmpty(t, peerStats.GetLastUpdate("test1"))

	for i := 1; i < 20; i++ {
		pid := "test" + strconv.Itoa(i)
		peerStats.AddOrUpdatePeer(pid, testUpdMsg)
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
	peerStats.AddOrUpdatePeer("test1", testUpdMsg)
	oldestPeers = peerStats.GetOldestUpdatedPeersWithSize(10)

	// the oldest peer should not be test1
	require.NotEqual(t, "test1", oldestPeers[0].PeerID)
}

func TestRemovePeersWhichExceedLimit(t *testing.T) {
	limit := 100
	peerStats := diagnostics.NewPeerStats(limit)

	for i := 1; i < 105; i++ {
		pid := "test" + strconv.Itoa(i)
		peerStats.AddOrUpdatePeer(pid, testUpdMsg)
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
		peerStats.AddOrUpdatePeer(pid, testUpdMsg)
	}

	require.Equal(t, limit, peerStats.GetPeersCount())

	peerStats.AddOrUpdatePeer("test105", testUpdMsg)

	require.Equal(t, limit, peerStats.GetPeersCount())
}
