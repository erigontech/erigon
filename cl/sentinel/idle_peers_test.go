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

// creditedDuringPassP2P reports a peer as idle on the first lookup and as just credited on the later ones.
type creditedDuringPassP2P struct {
	stubP2P
	lookups map[peer.ID]int
	idle    time.Time
	fresh   time.Time
}

func (s *creditedDuringPassP2P) PeerLastUseful(pid peer.ID) (time.Time, bool) {
	s.lookups[pid]++
	if s.lookups[pid] == 1 {
		return s.idle, true
	}
	return s.fresh, true
}

func TestEvictIdlePeersKeepsAPeerCreditedDuringThePass(t *testing.T) {
	local, remote := connectedPair(t)
	now := time.Now()
	s := idleTestSentinel(t, local, nil)
	s.p2p = &creditedDuringPassP2P{stubP2P: s.p2p.(stubP2P), lookups: map[peer.ID]int{}, idle: now.Add(-2 * peerIdleWindow), fresh: now}
	s.cfg.MaxPeerCount = 1
	s.evictIdlePeers(now)
	require.Equal(t, network.Connected, local.Network().Connectedness(remote.ID()))
}

type closedConn struct {
	network.Conn
}

func (closedConn) IsClosed() bool { return true }

// closingNetwork still lists the connections of a closing peer, as libp2p does until it removes them.
type closingNetwork struct {
	network.Network
	closing peer.ID
}

func (n closingNetwork) ConnsToPeer(pid peer.ID) []network.Conn {
	conns := n.Network.ConnsToPeer(pid)
	if pid == n.closing {
		for i := range conns {
			conns[i] = closedConn{conns[i]}
		}
	}
	return conns
}

type closingHost struct {
	host.Host
	net closingNetwork
}

func (h closingHost) Network() network.Network { return h.net }

func withClosingPeer(s *Sentinel, local host.Host, closing peer.ID, lastUseful map[peer.ID]time.Time) {
	s.p2p = stubP2P{host: closingHost{Host: local, net: closingNetwork{Network: local.Network(), closing: closing}}, ps: s.p2p.Pubsub(), lastUseful: lastUseful}
}

var subnet0Topic = fmt.Sprintf("/eth2/%x/%s/%s", common.Bytes4{1, 2, 3, 4}, gossip.TopicNameBeaconAttestation(0), gossip.SSZSnappyCodec)

func connectSubnet0Peers(t *testing.T, local host.Host, n int) []peer.ID {
	t.Helper()
	remotes := make([]peer.ID, 0, n)
	for range n {
		remote := connectOutbound(t, local)
		remotePS, err := pubsub.NewGossipSub(t.Context(), remote)
		require.NoError(t, err)
		topic, err := remotePS.Join(subnet0Topic)
		require.NoError(t, err)
		_, err = topic.Subscribe()
		require.NoError(t, err)
		remotes = append(remotes, remote.ID())
	}
	return remotes
}

func subnet0Sentinel(t *testing.T, local host.Host, remotes []peer.ID, lastUseful map[peer.ID]time.Time) *Sentinel {
	t.Helper()
	s := idleTestSentinel(t, local, lastUseful)
	require.Eventually(t, func() bool { return len(s.p2p.Pubsub().ListPeers(subnet0Topic)) == len(remotes) }, 5*time.Second, 10*time.Millisecond)
	return s
}

func TestPeerCountsByDirectionSkipsAPeerWhoseConnectionsAreClosed(t *testing.T) {
	local := newListeningHost(t)
	open, closing := connectOutbound(t, local), connectOutbound(t, local)
	s := testSentinel(t, local)
	s.p2p = stubP2P{host: closingHost{Host: local, net: closingNetwork{Network: local.Network(), closing: closing.ID()}}}
	require.Equal(t, peerCounts{outbound: 1}, s.peerCountsByDirection(""))
	require.Equal(t, peerCounts{}, s.peerCountsByDirection(open.ID()))
}

func TestEvictIdlePeersCountsOnlyConnectedPeersInSubnetCoverage(t *testing.T) {
	local := newListeningHost(t)
	now := time.Now()
	remotes := connectSubnet0Peers(t, local, minimumPeersPerSubnet+1)
	lastUseful := map[peer.ID]time.Time{}
	for _, pid := range remotes {
		lastUseful[pid] = now
	}
	idle, closing := remotes[0], remotes[len(remotes)-1]
	lastUseful[idle] = now.Add(-2 * peerIdleWindow)
	s := subnet0Sentinel(t, local, remotes, lastUseful)
	withClosingPeer(s, local, closing, lastUseful)

	s.cfg.MaxPeerCount = 1
	s.evictIdlePeers(now)
	require.Equal(t, network.Connected, local.Network().Connectedness(idle), "a closing peer does not count toward subnet 0")
}

func TestPruneExcessPeersCountsOnlyConnectedPeersInSubnetCoverage(t *testing.T) {
	local := newListeningHost(t)
	remotes := connectSubnet0Peers(t, local, 2)
	closing, last := remotes[0], remotes[1]
	s := subnet0Sentinel(t, local, remotes, nil)
	withClosingPeer(s, local, closing, nil)

	s.cfg.MaxPeerCount = 0
	s.pruneExcessPeers()
	require.Equal(t, network.Connected, local.Network().Connectedness(last), "a closing peer does not count toward subnet 0")
}
