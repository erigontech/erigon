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
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/prysmaticlabs/go-bitfield"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

const (
	peerSubnetTarget                 = 4
	goRoutinesOpeningPeerConnections = 4
	attestationSubnetCount           = 64
)

// getSubnetCoverage returns a count of peers for each attestation subnet (64 subnets)
// Uses on-demand metadata queries with LRU cache (TTL = 1 epoch)
func (s *Sentinel) getSubnetCoverage() [attestationSubnetCount]int {
	var coverage [attestationSubnetCount]int
	peers := s.p2p.Host().Network().Peers()

	for _, pid := range peers {
		// Get attnets from cache or query on-demand
		attnets, ok := s.GetPeerAttnets(pid)
		if !ok {
			// Fallback to ENR data if metadata query fails
			nodeVal, ok := s.pidToEnr.Load(pid)
			if !ok {
				continue
			}
			node, ok := nodeVal.(*enode.Node)
			if !ok {
				continue
			}
			var peerSubnets bitfield.Bitvector64
			if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err != nil {
				continue
			}
			attnets = [8]byte(peerSubnets)
		}

		// Count which subnets this peer covers
		for i := 0; i < attestationSubnetCount; i++ {
			if attnets[i/8]&(1<<(i%8)) != 0 {
				coverage[i]++
			}
		}
	}
	return coverage
}

// getEmptySubnets returns a list of subnet indices that have no peers
func (s *Sentinel) getEmptySubnets() []int {
	coverage := s.getSubnetCoverage()
	var empty []int
	for i, count := range coverage {
		if count == 0 {
			empty = append(empty, i)
		}
	}
	return empty
}

// isPeerUsefulForEmptySubnets checks if a peer covers any subnet that currently has no peers
func (s *Sentinel) isPeerUsefulForEmptySubnets(node *enode.Node, emptySubnets []int) bool {
	return len(s.getFilledSubnets(node, emptySubnets)) > 0
}

// getFilledSubnets returns which empty subnets a peer would fill (based on ENR)
func (s *Sentinel) getFilledSubnets(node *enode.Node, emptySubnets []int) []int {
	if len(emptySubnets) == 0 {
		return nil
	}

	var peerSubnets bitfield.Bitvector64
	if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err != nil {
		return nil
	}

	var filled []int
	for _, subnetIdx := range emptySubnets {
		if peerSubnets[subnetIdx/8]&(1<<(subnetIdx%8)) != 0 {
			filled = append(filled, subnetIdx)
		}
	}
	return filled
}

// isPeerUsefulForAnySubnet checks if a peer's ENR advertises any attestation subnets
// that overlap with our currently subscribed subnets
func (s *Sentinel) isPeerUsefulForAnySubnet(node *enode.Node) bool {
	// Get our subscribed subnets from ENR
	ourSubnets := s.p2p.GetSubscribedAttSubnets()

	// Check if we have any subnets subscribed
	hasAnySubscription := false
	for _, b := range ourSubnets {
		if b != 0 {
			hasAnySubscription = true
			break
		}
	}
	// If we don't have any subnet subscriptions, all peers are equally useful
	if !hasAnySubscription {
		return false
	}

	// Get peer's attnets from their ENR
	var peerSubnets bitfield.Bitvector64
	if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &peerSubnets)); err != nil {
		// Peer doesn't advertise attnets, not specifically useful for subnets
		return false
	}

	// Check for overlap between our subnets and peer's subnets
	for i := 0; i < len(ourSubnets) && i < len(peerSubnets); i++ {
		if ourSubnets[i]&peerSubnets[i] != 0 {
			return true
		}
	}
	return false
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

	// Track empty subnets, refresh every 6 seconds (half slot) to ensure quick coverage
	var emptySubnets []int
	lastEmptySubnetCheck := time.Time{}
	const emptySubnetCheckInterval = 6 * time.Second // Check every half slot

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

		// Refresh empty subnets list every slot (12 seconds)
		if time.Since(lastEmptySubnetCheck) > emptySubnetCheckInterval {
			emptySubnets = s.getEmptySubnets()
			lastEmptySubnetCheck = time.Now()
			if len(emptySubnets) > 0 {
				log.Info("[Sentinel] Subnets without peers", "count", len(emptySubnets), "subnets", emptySubnets)
			}
		}

		// Check if peer is useful for any of our subscribed subnets
		peerUsefulForSubnets := s.isPeerUsefulForAnySubnet(node)
		// Check which empty subnets this peer can fill (based on ENR)
		filledSubnets := s.getFilledSubnets(node, emptySubnets)
		peerFillsEmptySubnet := len(filledSubnets) > 0

		// If we have too many peers, only connect if peer is useful
		if s.HasTooManyPeers() {
			if !peerUsefulForSubnets && !peerFillsEmptySubnet {
				log.Trace("[Sentinel] Not looking for peers, at peer limit")
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// Peer is useful for subnets or fills an empty subnet, allow connection
			if peerFillsEmptySubnet {
				log.Debug("[Sentinel] Connecting to peer that fills empty subnet despite peer limit", "subnets", filledSubnets)
			} else {
				log.Debug("[Sentinel] Connecting to subnet-useful peer despite peer limit")
			}
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

		go func(usefulForSubnets bool, filled []int, enrNode *enode.Node) {
			if err := s.ConnectWithPeer(s.ctx, *peerInfo, sem); err != nil {
				log.Trace("[Sentinel] Could not connect with peer", "err", err)
			} else if len(filled) > 0 {
				// Check if metadata actually covers the subnets ENR claimed
				if metaAttnets, ok := s.GetPeerAttnets(peerInfo.ID); ok {
					actualFilled := []int{}
					for _, subnetIdx := range filled {
						if metaAttnets[subnetIdx/8]&(1<<(subnetIdx%8)) != 0 {
							actualFilled = append(actualFilled, subnetIdx)
						}
					}
					if len(actualFilled) != len(filled) {
						log.Debug("[Sentinel] ENR claimed subnets not in metadata", "peer", peerInfo.ID, "enrClaimed", filled, "metadataActual", actualFilled)
					} else {
						log.Debug("[Sentinel] Connected with peer that fills empty subnet", "peer", peerInfo.ID, "subnets", filled)
					}
				} else {
					log.Debug("[Sentinel] Connected with peer (ENR), metadata query failed", "peer", peerInfo.ID, "enrSubnets", filled)
				}
			} else if usefulForSubnets {
				log.Debug("[Sentinel] Connected with subnet-useful peer", "peer", peerInfo.ID)
			}
		}(peerUsefulForSubnets, filledSubnets, node)

	}
}

func (s *Sentinel) onConnection(_ network.Network, conn network.Conn) {
	go func() {
		peerId := conn.RemotePeer()
		if s.HasTooManyPeers() {
			log.Trace("[Sentinel] Not looking for peers, at peer limit")
			s.p2p.Host().Peerstore().RemovePeer(peerId)
			s.p2p.Host().Network().ClosePeer(peerId)
			s.peers.RemovePeer(peerId)
			return
		}

		valid, err := s.handshaker.ValidatePeer(peerId)
		if err != nil {
			log.Debug("[Sentinel] Failed to validate peer", "peer", peerId, "err", err)
		}

		if !valid {
			log.Debug("[Sentinel] Handshake failed, disconnecting peer", "peer", peerId)
			// on handshake fail, we disconnect with said peer, and remove them from our pool
			s.p2p.Host().Peerstore().RemovePeer(peerId)
			s.p2p.Host().Network().ClosePeer(peerId)
			s.peers.RemovePeer(peerId)
		} else {
			// we were able to succesfully connect, so add this peer to our pool
			s.peers.AddPeer(peerId)

			// Debug: compare ENR attnets vs metadata attnets
			if nodeVal, ok := s.pidToEnr.Load(peerId); ok {
				if node, ok := nodeVal.(*enode.Node); ok {
					var enrSubnets bitfield.Bitvector64
					if err := node.Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &enrSubnets)); err == nil {
						if metaAttnets, ok := s.GetPeerAttnets(peerId); ok {
							// Count how many subnets each advertises
							enrCount, metaCount := 0, 0
							for i := 0; i < 64; i++ {
								if enrSubnets[i/8]&(1<<(i%8)) != 0 {
									enrCount++
								}
								if metaAttnets[i/8]&(1<<(i%8)) != 0 {
									metaCount++
								}
							}
							if enrCount != metaCount {
								log.Debug("[Sentinel] ENR vs Metadata mismatch", "peer", peerId, "enrSubnets", enrCount, "metaSubnets", metaCount)
							}
						}
					}
				}
			}
			log.Debug("[Sentinel] Peer validated and added", "peer", peerId)
		}
	}()
}
