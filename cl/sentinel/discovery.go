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
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/prysmaticlabs/go-bitfield"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

const (
	peerSubnetTarget                 = 4
	goRoutinesOpeningPeerConnections = 4
	attestationSubnetCount           = 64
	minimumPeersPerSubnet            = 4 // Minimum peers needed per subnet before proactive search stops
	subnetSearchTimeout              = 30 * time.Second
	subnetSearchInterval             = 12 * time.Second // Check every slot
	peerPruneInterval                = 60 * time.Second // How often to check for excess peers
)

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
	iterator := s.listener.RandomNodes()
	filteredIterator := enode.Filter(iterator, func(node *enode.Node) bool {
		var peerSubnets bitfield.Bitvector64
		if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err != nil {
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

	for !allSatisfied() && checked < maxChecks {
		select {
		case <-ctx.Done():
			log.Debug("[Sentinel] Subnet peer search timeout", "checked", checked)
			return
		default:
		}

		if !filteredIterator.Next() {
			// Iterator exhausted
			break
		}
		checked++
		node := filteredIterator.Node()

		// Skip private IPs
		if node.IP().IsPrivate() {
			continue
		}

		peerInfo, _, err := convertToAddrInfo(node)
		if err != nil {
			continue
		}

		// Skip if already connected
		if s.p2p.Host().Network().Connectedness(peerInfo.ID) == network.Connected {
			continue
		}

		// Skip banned peers
		if s.peers.BanStatus(peerInfo.ID) {
			continue
		}

		// Store ENR mapping
		s.pidToEnr.Store(peerInfo.ID, node)
		s.pidToEnodeId.Store(peerInfo.ID, node.ID())

		// Try to connect
		if err := s.ConnectWithPeer(ctx, *peerInfo, nil); err != nil {
			log.Trace("[Sentinel] Subnet search: failed to connect", "peer", peerInfo.ID, "err", err)
			continue
		}

		// Check which subnets this peer covers and update counts
		var peerSubnets bitfield.Bitvector64
		if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err != nil {
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
	log.Debug("[Sentinel] Subnet peer search completed", "nodesChecked", checked, "subnetsReachedMinPeers", satisfied, "subnetsSearched", len(subnets), "peersFound", totalFound)
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

			// Collect underserved subnets with their peer counts
			var underserved []subnetSearchState
			for i, count := range &coverage {
				if count < minimumPeersPerSubnet {
					underserved = append(underserved, subnetSearchState{
						idx:    i,
						wanted: minimumPeersPerSubnet - count,
						found:  0,
					})
				}
			}

			if len(underserved) == 0 {
				log.Trace("[Sentinel] All subnets have sufficient peers")
				continue
			}

			// Sort by wanted count descending (subnets needing most peers first, i.e., 0-peer subnets)
			sort.Slice(underserved, func(i, j int) bool {
				return underserved[i].wanted > underserved[j].wanted
			})

			// Extract just the subnet indices for logging
			underservedIdxs := make([]int, len(underserved))
			for i, info := range underserved {
				underservedIdxs[i] = info.idx
			}

			log.Debug("[Sentinel] Proactive subnet search starting",
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
			log.Debug("[Sentinel] Subnet coverage after search",
				"subnetsAtMinPeers", atMin,
				"minPeersPerSubnet", minimumPeersPerSubnet,
				"stillUnderserved", stillUnderserved)
		}
	}
}

// peerSubnetInfo holds information about a peer's subnet coverage for pruning decisions
type peerSubnetInfo struct {
	pid              peer.ID
	subnetsCount     int   // Total subnets this peer covers (from GossipSub)
	criticalSubnets  []int // Subnets where this peer is the only provider
	redundantSubnets []int // Subnets where this peer is one of many
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
	for i := 0; i < attestationSubnetCount; i++ {
		topicName := gossip.TopicNameBeaconAttestation(uint64(i))
		fullTopic := fmt.Sprintf("/eth2/%x/%s/%s", forkDigest, topicName, gossip.SSZSnappyCodec)
		peers := s.p2p.Pubsub().ListPeers(fullTopic)
		coverage[i] = len(peers)
		subnetToPeers[i] = peers
	}

	log.Trace("[Sentinel] Subnet coverage check (GossipSub)", "forkDigest", fmt.Sprintf("%x", forkDigest))
	return coverage, subnetToPeers
}

// pruneExcessPeers disconnects excess peers while ensuring no subnet becomes empty
func (s *Sentinel) pruneExcessPeers() {
	peerCount := len(s.p2p.Host().Network().Peers())
	targetPeerCount := int(s.cfg.MaxPeerCount)

	// Only prune if we're significantly over the limit (10% buffer)
	pruneThreshold := targetPeerCount + targetPeerCount/10
	if peerCount <= pruneThreshold {
		return
	}

	peersToRemove := peerCount - targetPeerCount
	log.Info("[Sentinel] Pruning excess peers", "current", peerCount, "target", targetPeerCount, "toRemove", peersToRemove)

	// Get detailed subnet coverage
	coverage, subnetToPeers := s.getSubnetCoverageWithPeers()

	// Build peer info for all peers based on actual GossipSub subscriptions
	peers := s.p2p.Host().Network().Peers()
	peerInfos := make([]peerSubnetInfo, 0, len(peers))

	// Build a map of peer -> subnets they're subscribed to (from GossipSub)
	peerToSubnets := make(map[peer.ID][]int)
	for i := 0; i < attestationSubnetCount; i++ {
		for _, pid := range subnetToPeers[i] {
			peerToSubnets[pid] = append(peerToSubnets[pid], i)
		}
	}

	for _, pid := range peers {
		subnets := peerToSubnets[pid]
		info := peerSubnetInfo{
			pid:          pid,
			subnetsCount: len(subnets),
		}

		// Check each subnet this peer is actually subscribed to (via GossipSub)
		for _, subnetIdx := range subnets {
			if coverage[subnetIdx] == 1 {
				// This peer is the ONLY one covering this subnet - critical!
				info.criticalSubnets = append(info.criticalSubnets, subnetIdx)
			} else {
				info.redundantSubnets = append(info.redundantSubnets, subnetIdx)
			}
		}

		peerInfos = append(peerInfos, info)
	}

	// Sort peers by "removability" (most removable first)
	// Priority: peers with no critical subnets, then by fewest covered subnets
	sort.Slice(peerInfos, func(i, j int) bool {
		// Critical peers (covering unique subnets) should never be removed
		iCritical := len(peerInfos[i].criticalSubnets) > 0
		jCritical := len(peerInfos[j].criticalSubnets) > 0

		if iCritical != jCritical {
			return !iCritical // Non-critical peers first (more removable)
		}

		// Among non-critical peers, prefer removing those covering fewer subnets
		// (they provide less value)
		return peerInfos[i].subnetsCount < peerInfos[j].subnetsCount
	})

	// Remove peers, but re-check coverage after each removal
	removed := 0
	for _, info := range peerInfos {
		if removed >= peersToRemove {
			break
		}

		// Skip critical peers
		if len(info.criticalSubnets) > 0 {
			log.Trace("[Sentinel] Skipping critical peer", "peer", info.pid, "criticalSubnets", info.criticalSubnets)
			continue
		}

		// Before removing, verify this won't cause any subnet to become empty
		safeToRemove := true
		for _, subnetIdx := range info.redundantSubnets {
			// Check current coverage (may have changed from previous removals)
			currentPeers := subnetToPeers[subnetIdx]
			if len(currentPeers) <= 1 {
				safeToRemove = false
				break
			}
		}

		if !safeToRemove {
			log.Trace("[Sentinel] Skipping peer to prevent empty subnet", "peer", info.pid)
			continue
		}

		// Safe to remove - update coverage tracking
		for _, subnetIdx := range info.redundantSubnets {
			// Remove this peer from the subnet's peer list
			newPeers := make([]peer.ID, 0, len(subnetToPeers[subnetIdx])-1)
			for _, pid := range subnetToPeers[subnetIdx] {
				if pid != info.pid {
					newPeers = append(newPeers, pid)
				}
			}
			subnetToPeers[subnetIdx] = newPeers
			coverage[subnetIdx]--
		}

		// Disconnect the peer
		s.p2p.Host().Network().ClosePeer(info.pid)
		s.p2p.Host().Peerstore().RemovePeer(info.pid)
		s.peers.RemovePeer(info.pid)
		removed++

		log.Trace("[Sentinel] Pruned excess peer", "peer", info.pid, "subnetsCount", info.subnetsCount)
	}

	log.Info("[Sentinel] Peer pruning complete", "removed", removed, "remaining", peerCount-removed)
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
	if s.peers.BanStatus(info.ID) {
		return errors.New("refused to connect to bad peer")
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

// connectWithAllPeers is a helper function used to connect with a list of addrs.
// it only returns an error on fail to parse multiaddrs
// will print connect with peer errors to trace debug level
func (s *Sentinel) connectWithAllPeers(multiAddrs []multiaddr.Multiaddr) error {
	addrInfos, err := peer.AddrInfosFromP2pAddrs(multiAddrs...)
	if err != nil {
		return err
	}
	for _, peerInfo := range addrInfos {
		go func(peerInfo peer.AddrInfo) {
			if err := s.ConnectWithPeer(s.ctx, peerInfo, nil); err != nil {
				log.Debug("[Sentinel] Could not connect with peer", "err", err)
			} else {
				log.Debug("[Sentinel] Connected with peer", "peer", peerInfo.ID)
			}
		}(peerInfo)
	}
	return nil
}

func (s *Sentinel) stickToPeers(peers []multiaddr.Multiaddr) {
	// connect to static peers every one minute
	go func() {
		for {
			if err := s.connectWithAllPeers(peers); err != nil {
				log.Debug("[Sentinel] Could not connect with static peers", "err", err)
			}
			time.Sleep(3 * time.Minute)
		}
	}()
}

func (s *Sentinel) listenForPeers() {
	enodes := []*enode.Node{}
	for _, node := range s.cfg.NetworkConfig.StaticPeers {
		newNode, err := enode.Parse(enode.ValidSchemes, node)
		if err == nil {
			enodes = append(enodes, newNode)
		} else {
			log.Warn("Could not connect to static peer", "peer", node, "reason", err)
		}
	}
	log.Info("CL Sentinel static peers", "len", len(enodes))
	if s.cfg.NoDiscovery {
		return
	}
	multiAddresses := convertToMultiAddr(enodes)
	s.stickToPeers(multiAddresses)

	// limit the number of goroutines opening connection with peers
	sem := semaphore.NewWeighted(int64(goRoutinesOpeningPeerConnections))

	iterator := s.listener.RandomNodes()
	defer iterator.Close()
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

		if s.HasTooManyPeers() {
			log.Trace("[Sentinel] Not looking for peers, at peer limit")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		peerInfo, _, err := convertToAddrInfo(node)
		if err != nil {
			log.Error("[Sentinel] Could not convert to peer info", "err", err)
			continue
		}
		s.pidToEnr.Store(peerInfo.ID, node)
		s.pidToEnodeId.Store(peerInfo.ID, node.ID())
		// Skip Peer if IP was private.
		if node.IP().IsPrivate() {
			continue
		}

		if err := sem.Acquire(s.ctx, 1); err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			log.Error("[caplin] Failed to acquire sem for opening peer connection", "err", err)
			continue
		}

		go func() {
			if err := s.ConnectWithPeer(s.ctx, *peerInfo, sem); err != nil {
				log.Trace("[Sentinel] Could not connect with peer", "err", err)
			}
		}()

	}
}

func (s *Sentinel) onConnection(_ network.Network, conn network.Conn) {
	go func() {
		peerId := conn.RemotePeer()

		// Check if this peer helps any underserved subnets (< minimumPeersPerSubnet)
		peerHelpsSubnets := false
		if nodeVal, ok := s.pidToEnr.Load(peerId); ok {
			if node, ok := nodeVal.(*enode.Node); ok {
				var peerSubnets bitfield.Bitvector64
				if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err == nil {
					coverage := s.getSubnetCoverage()
					for i := 0; i < attestationSubnetCount; i++ {
						if peerSubnets[i/8]&(1<<(i%8)) != 0 && coverage[i] < minimumPeersPerSubnet {
							peerHelpsSubnets = true
							break
						}
					}
				}
			}
		}

		// If peer helps underserved subnets, skip the limit check
		if s.HasTooManyPeers() && !peerHelpsSubnets {
			log.Trace("[Sentinel] Rejecting peer, at peer limit")
			s.p2p.Host().Peerstore().RemovePeer(peerId)
			s.p2p.Host().Network().ClosePeer(peerId)
			s.peers.RemovePeer(peerId)
			return
		}

		valid, err := s.handshaker.ValidatePeer(peerId)
		if err != nil {
			log.Trace("[Sentinel] Failed to validate peer", "peer", peerId, "err", err)
		}

		if !valid {
			log.Trace("[Sentinel] Handshake failed, disconnecting peer", "peer", peerId)
			s.p2p.Host().Peerstore().RemovePeer(peerId)
			s.p2p.Host().Network().ClosePeer(peerId)
			s.peers.RemovePeer(peerId)
			s.peers.RecordHandshakeFailure(peerId)
		} else {
			// we were able to succesfully connect, so add this peer to our pool
			s.peers.AddPeer(peerId)

			log.Trace("[Sentinel] Peer validated and added", "peer", peerId)
		}
	}()
}
