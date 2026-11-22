package sentinel

import (
	"fmt"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func TestSelectIdlePeersTakesOnlyPeersIdleForTheWholeWindow(t *testing.T) {
	var coverage [attestationSubnetCount]int
	candidates := []idleCandidate{
		{pid: "useful", idleFor: 59 * time.Minute},
		{pid: "idle", idleFor: time.Hour},
	}
	require.Equal(t, []peer.ID{"idle"}, pids(selectIdlePeers(candidates, coverage, time.Hour, 2, false)))
}

func TestSelectIdlePeersEvictsTheLongestIdleFirstAndAtMostTheLimit(t *testing.T) {
	var coverage [attestationSubnetCount]int
	candidates := []idleCandidate{
		{pid: "a", idleFor: 2 * time.Hour},
		{pid: "b", idleFor: 4 * time.Hour},
		{pid: "c", idleFor: 3 * time.Hour},
	}
	require.Equal(t, []peer.ID{"b", "c"}, pids(selectIdlePeers(candidates, coverage, time.Hour, 2, false)))
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
	require.Equal(t, []peer.ID{"first"}, pids(selectIdlePeers(candidates, coverage, time.Hour, 2, false)),
		"the second peer of subnet 4 is kept once the first one takes it down to the floor")
}

func TestSelectIdlePeersTakesOnlyInboundPeersWhenOnlyTheInboundShareIsFull(t *testing.T) {
	var coverage [attestationSubnetCount]int
	candidates := []idleCandidate{
		{pid: "outbound", idleFor: 3 * time.Hour},
		{pid: "inbound", idleFor: 2 * time.Hour, inbound: true},
	}
	require.Equal(t, []peer.ID{"inbound"}, pids(selectIdlePeers(candidates, coverage, time.Hour, 2, true)))
}

func TestIdleEvictionScopeRunsWhenNewPeersAreRefused(t *testing.T) {
	for _, tc := range []struct {
		counts             peerCounts
		evict, inboundOnly bool
	}{
		{peerCounts{inbound: 7, outbound: 1}, false, true},
		{peerCounts{inbound: 8, outbound: 1}, true, true},
		{peerCounts{inbound: 6, outbound: 4}, true, false},
	} {
		evict, inboundOnly := idleEvictionScope(tc.counts, 10)
		require.Equal(t, tc.evict, evict, "%+v", tc.counts)
		if evict {
			require.Equal(t, tc.inboundOnly, inboundOnly, "%+v", tc.counts)
		}
	}
}

func pids(candidates []idleCandidate) []peer.ID {
	ids := make([]peer.ID, 0, len(candidates))
	for _, c := range candidates {
		ids = append(ids, c.pid)
	}
	return ids
}

func TestSubnetsOfConnectedPeersIgnoresTopicPeersThatAreGone(t *testing.T) {
	var subnetToPeers [attestationSubnetCount][]peer.ID
	subnetToPeers[1] = []peer.ID{"a", "gone"}
	subnetToPeers[2] = []peer.ID{"a", "b"}
	coverage, peerToSubnets := subnetsOfConnectedPeers(&subnetToPeers, []peer.ID{"a", "b"})
	require.Equal(t, 1, coverage[1])
	require.Equal(t, 2, coverage[2])
	require.Equal(t, []int{1, 2}, peerToSubnets["a"])
	require.Equal(t, []int{2}, peerToSubnets["b"])
	require.NotContains(t, peerToSubnets, peer.ID("gone"))
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

func TestEvictIdlePeersFreesAnInboundSlotWhenOnlyTheInboundShareIsFull(t *testing.T) {
	local, remote := connectedPair(t)
	now := time.Now()
	// For remote, local is an inbound peer. A cap of 2 leaves 1 inbound slot, now taken.
	s := idleTestSentinel(t, remote, map[peer.ID]time.Time{local.ID(): now.Add(-2 * peerIdleWindow)})
	s.cfg.MaxPeerCount = 2
	s.evictIdlePeers(now)
	waitDisconnected(t, remote, local.ID())
}

func TestEvictIdlePeersKeepsThePeerASubnetNeeds(t *testing.T) {
	local, remote := connectedPair(t)
	now := time.Now()
	s := idleTestSentinel(t, local, map[peer.ID]time.Time{remote.ID(): now.Add(-2 * peerIdleWindow)})
	topicName := fmt.Sprintf("/eth2/%x/%s/%s", common.Bytes4{1, 2, 3, 4}, gossip.TopicNameBeaconAttestation(0), gossip.SSZSnappyCodec)
	remotePS, err := pubsub.NewGossipSub(t.Context(), remote)
	require.NoError(t, err)
	topic, err := remotePS.Join(topicName)
	require.NoError(t, err)
	_, err = topic.Subscribe()
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(s.p2p.Pubsub().ListPeers(topicName)) == 1 }, 5*time.Second, 10*time.Millisecond)

	s.cfg.MaxPeerCount = 1
	s.evictIdlePeers(now)
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()), "the only peer of subnet 0 stays")
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
