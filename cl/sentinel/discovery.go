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

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
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
	if info.ID == s.host.ID() {
		return nil
	}
	if s.peers.BanStatus(info.ID) {
		return errors.New("refused to connect to bad peer")
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, clparams.MaxDialTimeout)
	defer cancel()
	err = s.host.Connect(ctxWithTimeout, info)
	if err != nil {
		return err
	}
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
				log.Trace("[Sentinel] Could not connect with peer", "err", err)
			}
		}(peerInfo)
	}
	return nil
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
	log.Info("Static peers", "len", len(enodes))
	if s.cfg.NoDiscovery {
		return
	}
	multiAddresses := convertToMultiAddr(enodes)
	if err := s.connectWithAllPeers(multiAddresses); err != nil {
		log.Warn("Could not connect to static peers", "reason", err)
	}

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

func (s *Sentinel) connectToBootnodes() error {
	for i := range s.discoverConfig.Bootnodes {
		if err := s.discoverConfig.Bootnodes[i].Record().Load(enr.WithEntry("tcp", new(enr.TCP))); err != nil {
			if !enr.IsNotFound(err) {
				log.Error("[Sentinel] Could not retrieve tcp port")
			}
			continue
		}
	}
	multiAddresses := convertToMultiAddr(s.discoverConfig.Bootnodes)
	s.connectWithAllPeers(multiAddresses)
	return nil
}

func (s *Sentinel) setupENR(
	node *enode.LocalNode,
) (*enode.LocalNode, error) {
	forkId, err := s.ethClock.ForkId()
	if err != nil {
		return nil, err
	}
	nfd, err := s.ethClock.NextForkDigest()
	if err != nil {
		return nil, err
	}
	node.Set(enr.WithEntry(s.cfg.NetworkConfig.Eth2key, forkId))
	node.Set(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, bitfield.NewBitvector64().Bytes()))
	node.Set(enr.WithEntry(s.cfg.NetworkConfig.SyncCommsSubnetKey, bitfield.Bitvector4{byte(0x00)}.Bytes()))
	node.Set(enr.WithEntry(s.cfg.NetworkConfig.CgcKey, []byte{}))
	node.Set(enr.WithEntry(s.cfg.NetworkConfig.NfdKey, nfd))
	return node, nil
}

func (s *Sentinel) updateENR(node *enode.LocalNode) {
	for {
		nextForkEpoch := s.ethClock.NextForkEpochIncludeBPO()
		if nextForkEpoch == s.cfg.BeaconConfig.FarFutureEpoch {
			break
		}
		// sleep until next fork epoch
		wakeupTime := s.ethClock.GetSlotTime(nextForkEpoch * s.cfg.BeaconConfig.SlotsPerEpoch).Add(time.Second)
		log.Info("[Sentinel] Sleeping until next fork epoch", "nextForkEpoch", nextForkEpoch, "wakeupTime", wakeupTime)
		time.Sleep(time.Until(wakeupTime)) // add 1 second for safety
		nfd, err := s.ethClock.NextForkDigest()
		if err != nil {
			log.Warn("[Sentinel] Could not get next fork digest", "err", err)
			break
		}
		node.Set(enr.WithEntry(s.cfg.NetworkConfig.NfdKey, nfd))
		forkId, err := s.ethClock.ForkId()
		if err != nil {
			log.Warn("[Sentinel] Could not get fork id", "err", err)
			break
		}
		node.Set(enr.WithEntry(s.cfg.NetworkConfig.Eth2key, forkId))
		log.Info("[Sentinel] Updated fork id and nfd")
	}
}

func (s *Sentinel) onConnection(net network.Network, conn network.Conn) {
	go func() {
		peerId := conn.RemotePeer()
		if s.HasTooManyPeers() {
			log.Trace("[Sentinel] Not looking for peers, at peer limit")
			s.host.Peerstore().RemovePeer(peerId)
			s.host.Network().ClosePeer(peerId)
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
			s.host.Peerstore().RemovePeer(peerId)
			s.host.Network().ClosePeer(peerId)
			s.peers.RemovePeer(peerId)
		} else {
			// we were able to succesfully connect, so add this peer to our pool
			s.peers.AddPeer(peerId)
		}
	}()
}
