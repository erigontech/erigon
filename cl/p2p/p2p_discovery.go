package p2p

import (
	"context"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/sync/semaphore"
)

func (p *P2Pmanager) connectToBootnodes(ctx context.Context, discoverConfig discover.Config) error {
	for i := range discoverConfig.Bootnodes {
		if err := discoverConfig.Bootnodes[i].Record().Load(enr.WithEntry("tcp", new(enr.TCP))); err != nil {
			if !enr.IsNotFound(err) {
				log.Error("[Sentinel] Could not retrieve tcp port")
			}
			continue
		}
	}
	multiAddresses := convertToMultiAddr(discoverConfig.Bootnodes)
	addrInfos, err := peer.AddrInfosFromP2pAddrs(multiAddresses...)
	if err != nil {
		return err
	}
	for _, peerInfo := range addrInfos {
		go func(peerInfo peer.AddrInfo) {
			if err := p.ConnectWithPeer(ctx, peerInfo, nil); err != nil {
				log.Trace("[Sentinel] Could not connect with peer", "err", err)
			}
		}(peerInfo)
	}
	return nil
}

// connectWithAllPeers is a helper function used to connect with a list of addrs.
// it only returns an error on fail to parse multiaddrs
// will print connect with peer errors to trace debug level
/*func (p *P2Pmanager) connectWithAllPeers(ctx context.Context, multiAddrs []multiaddr.Multiaddr) error {
	addrInfos, err := peer.AddrInfosFromP2pAddrs(multiAddrs...)
	if err != nil {
		return err
	}
	for _, peerInfo := range addrInfos {
		go func(peerInfo peer.AddrInfo) {
			if err := p.ConnectWithPeer(ctx, peerInfo, nil); err != nil {
				log.Trace("[Sentinel] Could not connect with peer", "err", err)
			}
		}(peerInfo)
	}
	return nil
}*/

// ConnectWithPeer is used to attempt to connect and add the peer to our pool
// it errors when if fail to connect with the peer, for instance, if it fails the handshake
// if it does not return an error, the peer is attempted to be added to the pool
func (p *P2Pmanager) ConnectWithPeer(ctx context.Context, info peer.AddrInfo, sem *semaphore.Weighted) (err error) {
	if sem != nil {
		defer sem.Release(1)
	}
	if info.ID == p.Host().ID() {
		return nil
	}
	/*if s.peers.BanStatus(info.ID) {
		return errors.New("refused to connect to bad peer")
	}*/
	ctxWithTimeout, cancel := context.WithTimeout(ctx, clparams.MaxDialTimeout)
	defer cancel()
	err = p.Host().Connect(ctxWithTimeout, info)
	if err != nil {
		return err
	}
	return nil
}
