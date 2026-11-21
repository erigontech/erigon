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
	// peerIdleWindow is how long a peer may go without giving us anything we kept before it can be
	// evicted at the peer cap. Peers that deliver first copies at all rarely go quiet for this long.
	peerIdleWindow = time.Hour
	// maxIdleEvictionsPerPass bounds the churn of one pruning pass.
	maxIdleEvictionsPerPass = 2
)

type idleCandidate struct {
	pid     peer.ID
	idleFor time.Duration
	subnets []int
}

// selectIdlePeers returns up to limit peers idle for at least window, the longest idle first,
// skipping any peer whose removal would take one of its subnets below minimumPeersPerSubnet.
func selectIdlePeers(candidates []idleCandidate, coverage [attestationSubnetCount]int, window time.Duration, limit int) []peer.ID {
	candidates = slices.DeleteFunc(slices.Clone(candidates), func(c idleCandidate) bool { return c.idleFor < window })
	slices.SortFunc(candidates, func(a, b idleCandidate) int { return cmp.Compare(b.idleFor, a.idleFor) })
	var selected []peer.ID
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
		selected = append(selected, c.pid)
	}
	return selected
}

func staticPeerIDs(nodes []string) map[peer.ID]struct{} {
	ids := make(map[peer.ID]struct{})
	for _, node := range nodes {
		addrs, err := p2p.ParseStaticPeerAddrs(node)
		if err != nil {
			continue
		}
		infos, err := peer.AddrInfosFromP2pAddrs(addrs...)
		if err != nil {
			continue
		}
		for _, info := range infos {
			ids[info.ID] = struct{}{}
		}
	}
	return ids
}

// evictIdlePeers frees slots held by idle peers when the peer cap is reached, so new peers that
// would be useful are not refused. Static peers are never evicted.
func (s *Sentinel) evictIdlePeers(now time.Time) {
	peers := s.p2p.Host().Network().Peers()
	if len(peers) < int(s.cfg.MaxPeerCount) {
		return
	}
	_, subnetToPeers := s.getSubnetCoverageWithPeers()
	var coverage [attestationSubnetCount]int
	peerToSubnets := make(map[peer.ID][]int)
	for subnet, pids := range &subnetToPeers {
		coverage[subnet] = len(pids)
		for _, pid := range pids {
			peerToSubnets[pid] = append(peerToSubnets[pid], subnet)
		}
	}
	candidates := make([]idleCandidate, 0, len(peers))
	for _, pid := range peers {
		if _, static := s.staticPeers[pid]; static {
			continue
		}
		last, ok := s.p2p.PeerLastUseful(pid)
		if !ok {
			continue
		}
		candidates = append(candidates, idleCandidate{pid: pid, idleFor: now.Sub(last), subnets: peerToSubnets[pid]})
	}
	for _, pid := range selectIdlePeers(candidates, coverage, peerIdleWindow, maxIdleEvictionsPerPass) {
		log.Debug("[Sentinel] Evicting idle peer", "peer", pid)
		s.closePeer(pid)
		s.p2p.Host().Peerstore().RemovePeer(pid)
	}
}
