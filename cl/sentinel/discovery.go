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

package sentinel

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

const (
	goRoutinesOpeningPeerConnections = 4
	attestationSubnetCount           = 64
	// Subscribers wanted per attestation subnet before the proactive search stops: the mesh low watermark, so
	// gossipsub can fill a subnet's mesh.
	minimumPeersPerSubnet = gossip.GossipSubDlo
	subnetSearchTimeout   = 30 * time.Second
	subnetSearchInterval  = 12 * time.Second // Check every slot
	peerPruneInterval     = 60 * time.Second // How often to check for excess peers
)

func filterNonPrivatePeerAddresses(info *peer.AddrInfo) bool {
	info.Addrs = slices.DeleteFunc(info.Addrs, func(addr multiaddr.Multiaddr) bool {
		ip, err := manet.ToIP(addr)
		return err != nil || ip.IsPrivate()
	})
	return len(info.Addrs) > 0
}

// getSubnetCoverage returns a count of peers for each attestation subnet (64 subnets)
func (s *Sentinel) getSubnetCoverage() [attestationSubnetCount]int {
	coverage, _ := s.getSubnetCoverageWithPeers()
	return coverage
}

// subnetSearchState tracks progress for finding peers for multiple subnets
type subnetSearchState struct {
	idx    int // subnet index
	wanted int // how many more peers we need
	found  int // how many we've found so far
}

// findPeersForSubnets proactively searches for peers that advertise any of the given subnets
// Uses a single iterator to efficiently find peers for multiple subnets at once
func (s *Sentinel) findPeersForSubnets(subnets []subnetSearchState) {
	if len(subnets) == 0 {
		return
	}

	// Create a filter that matches any of our target subnets
	iterator := s.nodesOnOurFork(s.listener.RandomNodes())
	filteredIterator := enode.Filter(iterator, func(node *enode.Node) bool {
		var peerSubnets bitfield.Bitvector64
		if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err != nil {
			return false
		}
		if len(peerSubnets) != 8 {
			return false
		}
		// Check if this node covers any subnet we still need
		for i := range subnets {
			if subnets[i].found < subnets[i].wanted {
				subnetIdx := subnets[i].idx
				if peerSubnets[subnetIdx/8]&(1<<(subnetIdx%8)) != 0 {
					return true
				}
			}
		}
		return false
	})
	defer filteredIterator.Close()

	ctx, cancel := context.WithTimeout(s.ctx, subnetSearchTimeout)
	defer cancel()
	// The filter skips non-matching nodes inside Next, so a walk would outlive the timeout unless it is closed.
	defer context.AfterFunc(ctx, filteredIterator.Close)()

	checked := 0
	maxChecks := 2000 // Higher limit since we're searching for multiple subnets

	// Check if all subnets are satisfied
	allSatisfied := func() bool {
		for i := range subnets {
			if subnets[i].found < subnets[i].wanted {
				return false
			}
		}
		return true
	}

	// The walk ends when the iterator is exhausted or when the context ends, which closes the iterator.
	for !allSatisfied() && checked < maxChecks && filteredIterator.Next() {
		checked++
		node := filteredIterator.Node()

		peerInfo, _, err := p2p.ConvertToAddrInfo(node)
		if err != nil {
			continue
		}
		if !s.cfg.P2PConfig.LocalDiscovery && !filterNonPrivatePeerAddresses(peerInfo) {
			continue
		}

		// Skip if already connected
		if s.p2p.Host().Network().Connectedness(peerInfo.ID) == network.Connected {
			continue
		}

		if !s.peers.Dialable(peerInfo.ID) {
			continue
		}

		// Store ENR mapping
		s.pidToEnr.Store(peerInfo.ID, node)
		s.pidToEnodeId.Store(peerInfo.ID, node.ID())

		// Try to connect (serialize via shared semaphore to avoid peerstore race, see #19603)
		if err := s.connectSem.Acquire(ctx, 1); err != nil {
			break
		}
		if err := s.ConnectWithPeer(ctx, *peerInfo, s.connectSem); err != nil {
			log.Trace("[Sentinel] Subnet search: failed to connect", "peer", peerInfo.ID, "err", err)
			continue
		}

		// Check which subnets this peer covers and update counts
		var peerSubnets bitfield.Bitvector64
		if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err != nil {
			continue
		}
		if len(peerSubnets) != 8 {
			continue
		}

		coveredSubnets := []int{}
		for i := range subnets {
			if subnets[i].found < subnets[i].wanted {
				subnetIdx := subnets[i].idx
				if peerSubnets[subnetIdx/8]&(1<<(subnetIdx%8)) != 0 {
					subnets[i].found++
					coveredSubnets = append(coveredSubnets, subnetIdx)
				}
			}
		}

		if len(coveredSubnets) > 0 {
			log.Debug("[Sentinel] Subnet search: connected to peer", "peer", peerInfo.ID, "subnets", coveredSubnets)
		}
	}

	// Log final results
	satisfied, totalFound := 0, 0
	for _, info := range subnets {
		log.Trace("[Sentinel] Subnet peer search result", "subnet", info.idx, "found", info.found, "wanted", info.wanted)
		totalFound += info.found
		if info.found >= info.wanted {
			satisfied++
		}
	}
	// ctxErr tells a search that ran out of time or was cancelled from one whose walk ended on its own.
	log.Debug("[Sentinel] Subnet peer search ended", "nodesChecked", checked, "subnetsReachedMinPeers", satisfied, "subnetsSearched", len(subnets), "peersFound", totalFound, "ctxErr", ctx.Err())
}

// nodesOnOurFork passes on only the discovered nodes that advertise our current fork digest; every dial path takes
// discovered nodes through it.
func (s *Sentinel) nodesOnOurFork(it enode.Iterator) enode.Iterator {
	return enode.Filter(it, s.onOurFork)
}

// onOurFork reports whether a discovered node advertises our current fork digest in its ENR. Discovery is shared by
// every network, and a node without it, such as one of another network, of another fork or an execution layer node,
// fails the status handshake after the dial.
func (s *Sentinel) onOurFork(node *enode.Node) bool {
	var forkID []byte
	if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.Eth2key, &forkID)); err != nil || len(forkID) < 4 {
		return false
	}
	digest, err := s.ethClock.CurrentForkDigest()
	return err == nil && bytes.Equal(forkID[:4], digest[:])
}

// proactiveSubnetPeerSearch is a goroutine that monitors subnet coverage
// and proactively searches for peers when coverage is below threshold
func (s *Sentinel) proactiveSubnetPeerSearch() {
	searchTicker := time.NewTicker(subnetSearchInterval)
	pruneTicker := time.NewTicker(peerPruneInterval)
	defer searchTicker.Stop()
	defer pruneTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-pruneTicker.C:
			// Prune excess peers periodically
			s.pruneExcessPeers()
		case <-searchTicker.C:
			coverage := s.getSubnetCoverage()
			underserved := underservedSubnets(&coverage)
			if len(underserved) == 0 {
				log.Trace("[Sentinel] All subnets have sufficient peers")
				continue
			}

			// Extract just the subnet indices for logging
			underservedIdxs := make([]int, len(underserved))
			for i, info := range underserved {
				underservedIdxs[i] = info.idx
			}

			log.Trace("[Sentinel] Proactive subnet search starting",
				"underservedCount", len(underserved),
				"threshold", minimumPeersPerSubnet,
				"subnets", underservedIdxs)

			// Process up to 16 subnets per cycle (can handle more now with batch approach)
			maxSubnetsPerCycle := 16
			if len(underserved) > maxSubnetsPerCycle {
				underserved = underserved[:maxSubnetsPerCycle]
			}

			// Find peers for all underserved subnets in one pass
			s.findPeersForSubnets(underserved)

			// Log post-search global coverage
			postCoverage := s.getSubnetCoverage()
			stillUnderserved := []int{}
			atMin := 0
			for i, count := range &postCoverage {
				if count >= minimumPeersPerSubnet {
					atMin++
				} else {
					stillUnderserved = append(stillUnderserved, i)
				}
			}
			log.Trace("[Sentinel] Subnet coverage after search",
				"subnetsAtMinPeers", atMin,
				"minPeersPerSubnet", minimumPeersPerSubnet,
				"stillUnderserved", stillUnderserved)
		}
	}
}

// underservedSubnets returns the subnets with fewer than minimumPeersPerSubnet subscribers, the emptiest first.
func underservedSubnets(coverage *[attestationSubnetCount]int) []subnetSearchState {
	var underserved []subnetSearchState
	for i, count := range coverage {
		if count < minimumPeersPerSubnet {
			underserved = append(underserved, subnetSearchState{idx: i, wanted: minimumPeersPerSubnet - count})
		}
	}
	slices.SortFunc(underserved, func(a, b subnetSearchState) int {
		return cmp.Compare(b.wanted, a.wanted)
	})
	return underserved
}

// coversUnderservedSubnet reports whether a peer advertising peerSubnets would add a subscriber to a subnet
// below minimumPeersPerSubnet.
func coversUnderservedSubnet(peerSubnets bitfield.Bitvector64, coverage *[attestationSubnetCount]int) bool {
	for i := range attestationSubnetCount {
		if peerSubnets[i/8]&(1<<(i%8)) != 0 && coverage[i] < minimumPeersPerSubnet {
			return true
		}
	}
	return false
}

// getSubnetCoverageWithPeers returns subnet coverage and which peers cover each subnet
// Uses GossipSub's actual topic subscriptions (more accurate than ENR-based counting)
func (s *Sentinel) getSubnetCoverageWithPeers() (coverage [attestationSubnetCount]int, subnetToPeers [attestationSubnetCount][]peer.ID) {
	// Get current fork digest for topic construction
	forkDigest, err := s.ethClock.CurrentForkDigest()
	if err != nil {
		log.Warn("[Sentinel] Failed to get current fork digest for subnet coverage", "err", err)
		return coverage, subnetToPeers
	}

	// Query GossipSub for actual peers subscribed to each attestation subnet topic
	for i := range attestationSubnetCount {
		topicName := gossip.TopicNameBeaconAttestation(uint64(i))
		fullTopic := fmt.Sprintf("/eth2/%x/%s/%s", forkDigest, topicName, gossip.SSZSnappyCodec)
		peers := s.p2p.Pubsub().ListPeers(fullTopic)
		coverage[i] = len(peers)
		subnetToPeers[i] = peers
	}

	log.Trace("[Sentinel] Subnet coverage check (GossipSub)", "forkDigest", fmt.Sprintf("%x", forkDigest))
	return coverage, subnetToPeers
}

func (s *Sentinel) closePeer(pid peer.ID) {
	if err := s.p2p.Host().Network().ClosePeer(pid); err != nil {
		log.Trace("[Sentinel] failed to close peer", "peer", pid, "err", err)
	}
}

type pruneCandidate struct {
	pid     peer.ID
	inbound bool
	opened  time.Time
	subnets []int
}

// peerCountToPrune gives the inbound trigger the same 10% buffer as the total trigger, so
// it tolerates peers admitted through the underserved-subnet exception in the same way.
func peerCountToPrune(counts peerCounts, maxPeers int) int {
	buffer := maxPeers / 10
	toRemove := 0
	if totalExcess := counts.total() - maxPeers; totalExcess > buffer {
		toRemove = totalExcess
	}
	if inboundExcess := counts.inbound - (maxPeers - outboundReserve(maxPeers)); inboundExcess > buffer {
		toRemove = max(toRemove, inboundExcess)
	}
	return toRemove
}

func selectPeersToPrune(candidates []pruneCandidate, coverage [attestationSubnetCount]int, toRemove, outboundReserve int) []peer.ID {
	slices.SortFunc(candidates, func(a, b pruneCandidate) int {
		if bySubnets := cmp.Compare(len(a.subnets), len(b.subnets)); bySubnets != 0 {
			return bySubnets
		}
		if a.inbound != b.inbound {
			if a.inbound {
				return -1
			}
			return 1
		}
		return b.opened.Compare(a.opened)
	})

	outboundCount := 0
	for _, candidate := range candidates {
		if !candidate.inbound {
			outboundCount++
		}
	}

	selected := make([]peer.ID, 0, min(toRemove, len(candidates)))
	for _, candidate := range candidates {
		if len(selected) == toRemove {
			break
		}
		if !candidate.inbound && outboundCount <= outboundReserve {
			continue
		}
		if slices.ContainsFunc(candidate.subnets, func(subnet int) bool { return coverage[subnet] <= 1 }) {
			continue
		}
		selected = append(selected, candidate.pid)
		if !candidate.inbound {
			outboundCount--
		}
		for _, subnet := range candidate.subnets {
			coverage[subnet]--
		}
	}
	return selected
}

// pruneExcessPeers disconnects excess peers while ensuring no subnet becomes empty
func (s *Sentinel) pruneExcessPeers() {
	peers := s.p2p.Host().Network().Peers()
	candidates := make([]pruneCandidate, 0, len(peers))
	var counts peerCounts
	for _, pid := range peers {
		conns := s.p2p.Host().Network().ConnsToPeer(pid)
		if len(conns) == 0 {
			continue
		}
		candidate := pruneCandidate{pid: pid, inbound: peerIsInbound(conns), opened: conns[0].Stat().Opened}
		if candidate.inbound {
			counts.inbound++
		} else {
			counts.outbound++
		}
		candidates = append(candidates, candidate)
	}

	maxPeers := int(s.cfg.MaxPeerCount)
	toRemove := peerCountToPrune(counts, maxPeers)
	if toRemove == 0 {
		return
	}
	log.Debug("[Sentinel] Pruning excess peers", "current", counts.total(), "inbound", counts.inbound, "outbound", counts.outbound, "toRemove", toRemove)

	coverage, subnetToPeers := s.getSubnetCoverageWithPeers()
	peerToSubnets := make(map[peer.ID][]int)
	for i := range attestationSubnetCount {
		for _, pid := range subnetToPeers[i] {
			peerToSubnets[pid] = append(peerToSubnets[pid], i)
		}
	}
	for i := range candidates {
		candidates[i].subnets = peerToSubnets[candidates[i].pid]
	}

	selected := selectPeersToPrune(candidates, coverage, toRemove, outboundReserve(maxPeers))
	for _, pid := range selected {
		s.closePeer(pid)
		s.p2p.Host().Peerstore().RemovePeer(pid)
		log.Trace("[Sentinel] Pruned excess peer", "peer", pid)
	}

	log.Debug("[Sentinel] Peer pruning complete", "toRemove", toRemove, "removed", len(selected))
}

// ConnectWithPeer is used to attempt to connect and add the peer to our pool
// it errors when if fail to connect with the peer, for instance, if it fails the handshake
// if it does not return an error, the peer is attempted to be added to the pool
func (s *Sentinel) ConnectWithPeer(ctx context.Context, info peer.AddrInfo, sem *semaphore.Weighted) (err error) {
	if sem != nil {
		defer sem.Release(1)
	}
	if info.ID == s.p2p.Host().ID() {
		return nil
	}
	if !s.peers.Dialable(info.ID) {
		return errors.New("peer is not dialable")
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, clparams.MaxDialTimeout)
	defer cancel()
	if s.p2p.Host().Network().Connectedness(info.ID) == network.Connected {
		return nil
	}
	err = s.p2p.Host().Connect(ctxWithTimeout, info)
	if err != nil {
		return err
	}
	log.Trace("[caplin] Connected with peer", "peer", info.ID)
	return nil
}

func (s *Sentinel) connectWithAllPeers(multiAddrs []multiaddr.Multiaddr) error {
	addrInfos, err := peer.AddrInfosFromP2pAddrs(multiAddrs...)
	if err != nil {
		return err
	}
	for _, peerInfo := range addrInfos {
		if err := s.connectSem.Acquire(s.ctx, 1); err != nil {
			return err
		}
		go func(peerInfo peer.AddrInfo) {
			if err := s.ConnectWithPeer(s.ctx, peerInfo, s.connectSem); err != nil {
				log.Debug("[Sentinel] Could not connect with peer", "err", err)
			} else {
				log.Debug("[Sentinel] Connected with peer", "peer", peerInfo.ID)
			}
		}(peerInfo)
	}
	return nil
}

func (s *Sentinel) stickToPeers(peers []multiaddr.Multiaddr) {
	go func() {
		for {
			if err := s.connectWithAllPeers(peers); err != nil {
				log.Debug("[Sentinel] Could not connect with static peers", "err", err)
			}
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(3 * time.Minute):
			}
		}
	}()
}

func (s *Sentinel) listenForPeers() {
	multiAddresses := make([]multiaddr.Multiaddr, 0, len(s.cfg.NetworkConfig.StaticPeers))
	for _, node := range s.cfg.NetworkConfig.StaticPeers {
		addrs, err := p2p.ParseStaticPeerAddrs(node)
		if err != nil {
			log.Warn("Could not connect to static peer", "peer", node, "reason", err)
			continue
		}
		multiAddresses = append(multiAddresses, addrs...)
	}
	log.Info("CL Sentinel static peers", "len", len(multiAddresses))
	if len(multiAddresses) > 0 {
		s.stickToPeers(multiAddresses)
	}
	if s.cfg.NoDiscovery {
		return
	}

	iterator := s.nodesOnOurFork(s.listener.RandomNodes())
	defer iterator.Close()
	// The filter skips rejected nodes inside Next, so the loop below only sees the context end once the iterator closes.
	defer context.AfterFunc(s.ctx, iterator.Close)()
	for {
		if err := s.ctx.Err(); err != nil {
			log.Debug("Stopping Ethereum 2.0 peer discovery", "err", err)
			break
		}

		exists := iterator.Next()
		if !exists {
			continue
		}
		node := iterator.Node()

		if !wantsOutbound(s.peerCountsByDirection(""), int(s.cfg.MaxPeerCount)) {
			log.Trace("[Sentinel] Not looking for peers, at peer limit")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		peerInfo, _, err := p2p.ConvertToAddrInfo(node)
		if err != nil {
			log.Debug("[Sentinel] Could not convert to peer info", "err", err)
			continue
		}
		if !s.cfg.P2PConfig.LocalDiscovery && !filterNonPrivatePeerAddresses(peerInfo) {
			continue
		}
		s.pidToEnr.Store(peerInfo.ID, node)
		s.pidToEnodeId.Store(peerInfo.ID, node.ID())

		if err := s.connectSem.Acquire(s.ctx, 1); err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			log.Error("[caplin] Failed to acquire sem for opening peer connection", "err", err)
			continue
		}

		go func() {
			if err := s.ConnectWithPeer(s.ctx, *peerInfo, s.connectSem); err != nil {
				log.Trace("[Sentinel] Could not connect with peer", "err", err)
			}
		}()

	}
}

func (s *Sentinel) onConnection(_ network.Network, conn network.Conn) {
	peerId := conn.RemotePeer()
	addr := conn.RemoteMultiaddr()
	transport := conn.ConnState().Transport
	if transport == "quic-v1" {
		transport = "quic"
	}
	s.logger.Trace("[Sentinel] Peer connected",
		"peer", peerId,
		"direction", conn.Stat().Direction,
		"addr", addr,
		"transport", transport)
	go s.handleNewConnection(peerId, conn.Stat().Direction, addr, func() (bool, error) {
		return s.handshaker.ValidatePeer(s.ctx, peerId)
	})
}

// handleNewConnection admits or rejects a peer that has just connected, then runs its status
// handshake. Reports whether the peer was kept.
func (s *Sentinel) handleNewConnection(peerId peer.ID, direction network.Direction, addr multiaddr.Multiaddr, validate func() (bool, error)) bool {
	if s.peers.RefuseConnections(peerId) {
		s.logger.Debug("[Sentinel] Closing refused peer connection", "peer", peerId, "direction", direction, "addr", addr)
		s.closePeer(peerId)
		return false
	}

	{
		// Check if this peer helps any underserved subnets (< minimumPeersPerSubnet)
		peerHelpsSubnets := false
		if nodeVal, ok := s.pidToEnr.Load(peerId); ok {
			if node, ok := nodeVal.(*enode.Node); ok {
				var peerSubnets bitfield.Bitvector64
				if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err == nil && len(peerSubnets) == 8 {
					coverage := s.getSubnetCoverage()
					peerHelpsSubnets = coversUnderservedSubnet(peerSubnets, &coverage)
				}
			}
		}

		counts := s.peerCountsByDirection(peerId)
		maxPeers := int(s.cfg.MaxPeerCount)
		inbound := direction != network.DirOutbound
		if conns := s.p2p.Host().Network().ConnsToPeer(peerId); len(conns) > 0 {
			inbound = peerIsInbound(conns)
		}
		allowed := wantsOutbound(counts, maxPeers)
		if inbound {
			allowed = canAcceptInbound(counts, maxPeers)
		}
		if !allowed && !peerHelpsSubnets {
			log.Trace("[Sentinel] Rejecting peer, at peer limit")
			s.p2p.Host().Peerstore().RemovePeer(peerId)
			s.closePeer(peerId)
			return false
		}
	}

	valid, err := validate()
	if !valid && err != nil {
		failureCount, becameUndialable := s.peers.RecordHandshakeFailure(peerId)
		s.logger.Debug("[Sentinel] Handshake failed", "peer", peerId, "count", failureCount, "err", err)
		if becameUndialable {
			s.logger.Debug("[Sentinel] Peer became undialable", "peer", peerId)
		}
		if s.peers.RefuseConnections(peerId) {
			s.logger.Debug("[Sentinel] Refusing peer connections", "peer", peerId, "count", failureCount)
			s.closePeer(peerId)
			return false
		}
		return true
	}

	if !valid {
		// Handshake succeeded but fork digest mismatched — peer is on a different fork.
		// Must disconnect to avoid receiving incompatible blocks.
		log.Debug("[Sentinel] Fork mismatch, disconnecting peer", "peer", peerId)
		s.p2p.Host().Peerstore().RemovePeer(peerId)
		s.closePeer(peerId)
		return false
	}

	s.peers.RecordHandshakeSuccess(peerId)
	log.Trace("[Sentinel] Peer validated and added", "peer", peerId)
	return true
}
