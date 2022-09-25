package sentinel

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

func (s *Sentinel) connectWithPeer(ctx context.Context, info peer.AddrInfo) error {
	if info.ID == s.host.ID() {
		return nil
	}
	if s.peers.IsBadPeer(info.ID) {
		return errors.New("refused to connect to bad peer")
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, clparams.MaxDialTimeout)
	defer cancel()
	if err := s.host.Connect(ctxWithTimeout, info); err != nil {
		s.peers.Penalize(info.ID)
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
		// make each dial non-blocking
		go func(peerInfo peer.AddrInfo) {
			if err := s.connectWithPeer(s.ctx, peerInfo); err != nil {
				log.Debug("Could not connect with peer", "err", err)
			}
		}(peerInfo)
	}
	return nil
}

// listen for new nodes watches for new nodes in the network and adds them to the peerstore.
func (s *Sentinel) listenForPeers() {
	iterator := s.listener.RandomNodes()
	defer iterator.Close()
	for {
		// Exit if service's context is canceled
		if s.ctx.Err() != nil {
			break
		}
		if s.HasTooManyPeers() {
			// Pause the main loop for a period to stop looking
			// for new peers.
			log.Trace("Not looking for peers, at peer limit")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		exists := iterator.Next()
		if !exists {
			break
		}
		node := iterator.Node()
		peerInfo, _, err := convertToAddrInfo(node)
		if err != nil {
			log.Error("Could not convert to peer info", "err", err)
			continue
		}

		// Make sure that peer is not dialed too often, for each connection attempt there's a backoff period.
		go func(peerInfo *peer.AddrInfo) {
			if err := s.connectWithPeer(s.ctx, *peerInfo); err != nil {
				log.Debug("Could not connect with peer", "err", err)
			}
		}(peerInfo)
	}
}

func (s *Sentinel) connectToBootnodes() error {
	for i := range s.cfg.DiscoverConfig.Bootnodes {
		// do not dial bootnodes with their tcp ports not set
		if err := s.cfg.DiscoverConfig.Bootnodes[i].Record().Load(enr.WithEntry("tcp", new(enr.TCP))); err != nil {
			if !enr.IsNotFound(err) {
				log.Error("Could not retrieve tcp port")
			}
			continue
		}
	}
	multiAddresses := convertToMultiAddr(s.cfg.DiscoverConfig.Bootnodes)
	s.connectWithAllPeers(multiAddresses)
	return nil
}
