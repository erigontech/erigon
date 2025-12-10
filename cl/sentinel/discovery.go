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
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
)

const (
	peerSubnetTarget                 = 4
	goRoutinesOpeningPeerConnections = 4
)

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

		// needsPeersForSubnets := s.isPeerUsefulForAnySubnet(node)
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
		s.pidToEnr.Store(peerInfo.ID, node.String())
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
		if s.HasTooManyPeers() {
			log.Trace("[Sentinel] Not looking for peers, at peer limit")
			s.p2p.Host().Peerstore().RemovePeer(peerId)
			s.p2p.Host().Network().ClosePeer(peerId)
			s.peers.RemovePeer(peerId)
			return
		}

		valid, err := s.handshaker.ValidatePeer(peerId)
		if err != nil {
			log.Trace("[sentinel] failed to validate peer:", "err", err)
		}

		if !valid {
			log.Trace("Handshake was unsuccessful")
			// on handshake fail, we disconnect with said peer, and remove them from our pool
			s.p2p.Host().Peerstore().RemovePeer(peerId)
			s.p2p.Host().Network().ClosePeer(peerId)
			s.peers.RemovePeer(peerId)
		} else {
			// we were able to succesfully connect, so add this peer to our pool
			s.peers.AddPeer(peerId)
		}
	}()
}
