package sentinel

import (
	"cmp"
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	// peerIdleWindow is how long a peer may go without giving us anything useful before it can be
	// evicted while new peers are refused. Peers that deliver first copies at all rarely go quiet for this long.
	peerIdleWindow = time.Hour
	// maxIdleEvictionsPerPass bounds the churn of one pruning pass.
	maxIdleEvictionsPerPass = 2
)

type idleCandidate struct {
	pid     peer.ID
	idleFor time.Duration
	inbound bool
	subnets []int
}

// selectIdlePeers returns up to limit peers idle for at least window, the longest idle first,
// skipping any peer whose removal would take one of its subnets below minimumPeersPerSubnet.
// With inboundOnly, only inbound peers are selected.
func selectIdlePeers(candidates []idleCandidate, coverage [attestationSubnetCount]int, window time.Duration, limit int, inboundOnly bool) []idleCandidate {
	candidates = slices.DeleteFunc(slices.Clone(candidates), func(c idleCandidate) bool {
		return c.idleFor < window || inboundOnly && !c.inbound
	})
	slices.SortFunc(candidates, func(a, b idleCandidate) int { return cmp.Compare(b.idleFor, a.idleFor) })
	var selected []idleCandidate
	for _, c := range candidates {
		if len(selected) == limit {
			break
		}
		if slices.ContainsFunc(c.subnets, func(subnet int) bool { return coverage[subnet] <= minimumPeersPerSubnet }) {
			continue
		}
		for _, subnet := range c.subnets {
			coverage[subnet]--
		}
		selected = append(selected, c)
	}
	return selected
}

func staticPeerIDs(nodes []string) map[peer.ID]struct{} {
	ids := make(map[peer.ID]struct{})
	for _, node := range nodes {
		addr, err := p2p.ParseStaticPeer(node)
		if err != nil {
			continue
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			continue
		}
		ids[info.ID] = struct{}{}
	}
	return ids
}

// idleEvictionScope reports whether new peers are refused, and whether only evicting an inbound peer
// frees a slot for them: below the total cap, only the inbound share can be full.
func idleEvictionScope(c peerCounts, maxPeers int) (evict, inboundOnly bool) {
	return !canAcceptInbound(c, maxPeers), c.total() < maxPeers
}

// evictIdlePeers frees slots held by idle peers when new peers would be refused, so new peers that
// would be useful can connect. When only the inbound share is full, only inbound peers are evicted,
// as only they free an inbound slot. Static peers are never evicted.
func (s *Sentinel) evictIdlePeers(now time.Time) {
	evict, inboundOnly := idleEvictionScope(s.peerCountsByDirection(""), int(s.cfg.MaxPeerCount))
	if !evict {
		return
	}
	peers := s.p2p.Host().Network().Peers()
	connected := make([]peer.ID, 0, len(peers))
	candidates := make([]idleCandidate, 0, len(peers))
	for _, pid := range peers {
		conns := openConns(s.p2p.Host().Network(), pid)
		if len(conns) == 0 {
			continue
		}
		connected = append(connected, pid)
		if _, static := s.staticPeers[pid]; static {
			continue
		}
		if last, ok := s.p2p.PeerLastUseful(pid); ok {
			candidates = append(candidates, idleCandidate{pid: pid, idleFor: now.Sub(last), inbound: peerIsInbound(conns)})
		}
	}
	_, subnetToPeers := s.getSubnetCoverageWithPeers()
	coverage, peerToSubnets := subnetsOfConnectedPeers(&subnetToPeers, connected)
	for i := range candidates {
		candidates[i].subnets = peerToSubnets[candidates[i].pid]
	}
	for _, c := range selectIdlePeers(candidates, coverage, peerIdleWindow, maxIdleEvictionsPerPass, inboundOnly) {
		// A peer can be credited or reconnect while the subnet coverage is read.
		if last, ok := s.p2p.PeerLastUseful(c.pid); !ok || now.Sub(last) < peerIdleWindow {
			continue
		}
		log.Debug("[Sentinel] Evicting idle peer", "peer", c.pid, "idleFor", c.idleFor.Round(time.Second))
		s.closePeer(c.pid)
		s.p2p.Host().Peerstore().RemovePeer(c.pid)
	}
}
