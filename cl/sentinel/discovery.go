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

// ConnectWithPeer is used to attempt to connect and add the peer to our pool
// it errors when if fail to connect with the peer, for instance, if it fails the handshake
// if it does not return an error, the peer is attempted to be added to the pool
func (s *Sentinel) ConnectWithPeer(ctx context.Context, info peer.AddrInfo) (err error) {
	if info.ID == s.host.ID() {
		return nil
	}
	if s.peers.BanStatus(info.ID) {
		return fmt.Errorf("refused to connect to bad peer")
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
			if err := s.ConnectWithPeer(s.ctx, peerInfo); err != nil {
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
	log.Info("Static peers", "len", len(enodes))
	if s.cfg.NoDiscovery {
		return
	}
	multiAddresses := convertToMultiAddr(enodes)
	if err := s.connectWithAllPeers(multiAddresses); err != nil {
		log.Warn("Could not connect to static peers", "reason", err)
	}

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
			if err := s.ConnectWithPeer(s.ctx, *peerInfo); err != nil {
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
