/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sentinel

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/prysmaticlabs/go-bitfield"
)

func (s *Sentinel) ConnectWithPeer(ctx context.Context, info peer.AddrInfo, skipHandshake bool) error {
	if info.ID == s.host.ID() {
		return nil
	}
	if s.peers.IsBadPeer(info.ID) {
		return fmt.Errorf("refused to connect to bad peer")
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, clparams.MaxDialTimeout)
	defer cancel()
	if err := s.host.Connect(ctxWithTimeout, info); err != nil {
		s.peers.DisconnectPeer(info.ID)
		return err
	}
	return nil
}

func (s *Sentinel) connectWithAllPeers(multiAddrs []multiaddr.Multiaddr) error {
	addrInfos, err := peer.AddrInfosFromP2pAddrs(multiAddrs...)
	if err != nil {
		return err
	}
	for _, peerInfo := range addrInfos {
		go func(peerInfo peer.AddrInfo) {
			if err := s.ConnectWithPeer(s.ctx, peerInfo, true); err != nil {
				log.Trace("[Sentinel] Could not connect with peer", "err", err)
			}
		}(peerInfo)
	}
	return nil
}

func (s *Sentinel) listenForPeers() {
	s.listenForPeersDoneCh = make(chan struct{}, 3)
	enodes := []*enode.Node{}
	for _, node := range s.cfg.NetworkConfig.StaticPeers {
		newNode, err := enode.Parse(enode.ValidSchemes, node)
		if err == nil {
			enodes = append(enodes, newNode)
		} else {
			log.Warn("Could not connect to static peer", "peer", node, "reason", err)
		}
	}

	multiAddresses := convertToMultiAddr(enodes)
	s.connectWithAllPeers(multiAddresses)

	iterator := s.listener.RandomNodes()
	defer iterator.Close()
	for {
		if err := s.ctx.Err(); err != nil {
			log.Debug("Stopping Ethereum 2.0 peer discovery", "err", err)
			break
		}
		select {
		case <-s.listenForPeersDoneCh:
			return
		default:
		}
		if s.HasTooManyPeers() {
			log.Trace("[Sentinel] Not looking for peers, at peer limit")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		exists := iterator.Next()
		if !exists {
			continue
		}
		node := iterator.Node()
		peerInfo, _, err := convertToAddrInfo(node)
		if err != nil {
			log.Error("[Sentinel] Could not convert to peer info", "err", err)
			continue
		}

		// Skip Peer if IP was private.
		if node.IP().IsPrivate() {
			continue
		}

		go func(peerInfo *peer.AddrInfo) {
			if err := s.ConnectWithPeer(s.ctx, *peerInfo, false); err != nil {
				log.Trace("[Sentinel] Could not connect with peer", "err", err)
			}
		}(peerInfo)
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
	forkId, err := fork.ComputeForkId(s.cfg.BeaconConfig, s.cfg.GenesisConfig)
	if err != nil {
		return nil, err
	}
	node.Set(enr.WithEntry(s.cfg.NetworkConfig.Eth2key, forkId))
	node.Set(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, bitfield.NewBitvector64().Bytes()))
	node.Set(enr.WithEntry(s.cfg.NetworkConfig.SyncCommsSubnetKey, bitfield.Bitvector4{byte(0x00)}.Bytes()))
	return node, nil
}

func (s *Sentinel) onConnection(net network.Network, conn network.Conn) {
	go func() {
		peerId := conn.RemotePeer()
		invalid := !s.handshaker.ValidatePeer(peerId)
		if invalid {
			log.Trace("Handshake was unsuccessful")
			s.peers.DisconnectPeer(peerId)
		}
	}()
}
