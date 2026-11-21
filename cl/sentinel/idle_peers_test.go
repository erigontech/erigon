package sentinel

import (
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func TestSelectIdlePeersTakesOnlyPeersIdleForTheWholeWindow(t *testing.T) {
	var coverage [attestationSubnetCount]int
	candidates := []idleCandidate{
		{pid: "useful", idleFor: 59 * time.Minute},
		{pid: "idle", idleFor: time.Hour},
	}
	require.Equal(t, []peer.ID{"idle"}, selectIdlePeers(candidates, coverage, time.Hour, 2))
}

func TestSelectIdlePeersEvictsTheLongestIdleFirstAndAtMostTheLimit(t *testing.T) {
	var coverage [attestationSubnetCount]int
	candidates := []idleCandidate{
		{pid: "a", idleFor: 2 * time.Hour},
		{pid: "b", idleFor: 4 * time.Hour},
		{pid: "c", idleFor: 3 * time.Hour},
	}
	require.Equal(t, []peer.ID{"b", "c"}, selectIdlePeers(candidates, coverage, time.Hour, 2))
}

func TestSelectIdlePeersKeepsAPeerWhoseSubnetWouldDropBelowTheFloor(t *testing.T) {
	var coverage [attestationSubnetCount]int
	coverage[3] = minimumPeersPerSubnet
	coverage[4] = minimumPeersPerSubnet + 1
	candidates := []idleCandidate{
		{pid: "needed", idleFor: 5 * time.Hour, subnets: []int{3}},
		{pid: "first", idleFor: 4 * time.Hour, subnets: []int{4}},
		{pid: "second", idleFor: 3 * time.Hour, subnets: []int{4}},
	}
	require.Equal(t, []peer.ID{"first"}, selectIdlePeers(candidates, coverage, time.Hour, 2),
		"the second peer of subnet 4 is kept once the first one takes it down to the floor")
}

// idleTestSentinel is a sentinel with the subnet-coverage dependencies: a clock and a gossipsub with
// no subscriptions, so no peer is needed by a subnet.
func idleTestSentinel(t *testing.T, local host.Host, lastUseful map[peer.ID]time.Time) *Sentinel {
	t.Helper()
	s := testSentinel(t, local)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().CurrentForkDigest().Return(common.Bytes4{1, 2, 3, 4}, nil).AnyTimes()
	s.ethClock = clock
	ps, err := pubsub.NewGossipSub(t.Context(), local)
	require.NoError(t, err)
	s.p2p = stubP2P{host: local, ps: ps, lastUseful: lastUseful}
	return s
}

func TestEvictIdlePeersRunsOnlyAtThePeerCap(t *testing.T) {
	local, remote := connectedPair(t)
	now := time.Now()
	s := idleTestSentinel(t, local, map[peer.ID]time.Time{remote.ID(): now.Add(-2 * peerIdleWindow)})

	s.cfg.MaxPeerCount = 2
	s.evictIdlePeers(now)
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()), "below the cap no peer is evicted")

	s.cfg.MaxPeerCount = 1
	s.evictIdlePeers(now)
	waitDisconnected(t, local, remote.ID())
}

func TestEvictIdlePeersKeepsAUsefulPeerAndAStaticPeer(t *testing.T) {
	local, remote := connectedPair(t)
	now := time.Now()
	s := idleTestSentinel(t, local, map[peer.ID]time.Time{remote.ID(): now.Add(-peerIdleWindow / 2)})
	s.cfg.MaxPeerCount = 1
	s.evictIdlePeers(now)
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()))

	s = idleTestSentinel(t, local, map[peer.ID]time.Time{remote.ID(): now.Add(-2 * peerIdleWindow)})
	s.cfg.MaxPeerCount = 1
	s.staticPeers = map[peer.ID]struct{}{remote.ID(): {}}
	s.evictIdlePeers(now)
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()))
}

func TestStaticPeerIDsReadsMultiaddrPeers(t *testing.T) {
	_, remote := connectedPair(t)
	addr := remote.Addrs()[0].String() + "/p2p/" + remote.ID().String()
	ids := staticPeerIDs([]string{addr, "not a peer"})
	require.Equal(t, map[peer.ID]struct{}{remote.ID(): {}}, ids)
}
