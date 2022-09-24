package sentinel

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

func convertToInterfacePubkey(pubkey *ecdsa.PublicKey) (crypto.PubKey, error) {
	xVal, yVal := new(btcec.FieldVal), new(btcec.FieldVal)
	if xVal.SetByteSlice(pubkey.X.Bytes()) {
		return nil, errors.Errorf("X value overflows")
	}
	if yVal.SetByteSlice(pubkey.Y.Bytes()) {
		return nil, errors.Errorf("Y value overflows")
	}
	newKey := crypto.PubKey((*crypto.Secp256k1PublicKey)(btcec.NewPublicKey(xVal, yVal)))
	// Zero out temporary values.
	xVal.Zero()
	yVal.Zero()
	return newKey, nil
}

func convertToSingleMultiAddr(node *enode.Node) (multiaddr.Multiaddr, error) {
	pubkey := node.Pubkey()
	assertedKey, err := convertToInterfacePubkey(pubkey)
	if err != nil {
		return nil, errors.Wrap(err, "could not get pubkey")
	}
	id, err := peer.IDFromPublicKey(assertedKey)
	if err != nil {
		return nil, errors.Wrap(err, "could not get peer id")
	}
	return multiAddressBuilderWithID(node.IP().String(), "tcp", uint(node.TCP()), id)
}

func multiAddressBuilderWithID(ipAddr, protocol string, port uint, id peer.ID) (multiaddr.Multiaddr, error) {
	parsedIP := net.ParseIP(ipAddr)
	if parsedIP.To4() == nil && parsedIP.To16() == nil {
		return nil, errors.Errorf("invalid ip address provided: %s", ipAddr)
	}
	if id.String() == "" {
		return nil, errors.New("empty peer id given")
	}
	if parsedIP.To4() != nil {
		return multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/%s/%d/p2p/%s", ipAddr, protocol, port, id.String()))
	}
	return multiaddr.NewMultiaddr(fmt.Sprintf("/ip6/%s/%s/%d/p2p/%s", ipAddr, protocol, port, id.String()))
}

func convertToMultiAddr(nodes []*enode.Node) []multiaddr.Multiaddr {
	var multiAddrs []multiaddr.Multiaddr
	for _, node := range nodes {
		// ignore nodes with no ip address stored
		if node.IP() == nil {
			continue
		}
		multiAddr, err := convertToSingleMultiAddr(node)
		if err != nil {
			log.Debug("Could not convert to multiAddr", "err", err)
			continue
		}
		multiAddrs = append(multiAddrs, multiAddr)
	}
	return multiAddrs
}

func (s *Sentinel) connectWithPeer(ctx context.Context, info peer.AddrInfo) error {
	if info.ID == s.host.ID() {
		return nil
	}
	if s.peers.IsBadPeer(info.ID) {
		return errors.New("refused to connect to bad peer")
	}
	ctx, cancel := context.WithTimeout(ctx, clparams.MaxDialTimeout)
	defer cancel()
	if err := s.host.Connect(ctx, info); err != nil {
		s.peers.IncrementBadResponse(info.ID)
		return err
	}
	return nil
}

func (s *Sentinel) connectWithAllPeers(multiAddrs []multiaddr.Multiaddr) error {
	addrInfos, err := peer.AddrInfosFromP2pAddrs(multiAddrs...)
	if err != nil {
		return err
	}
	for _, info := range addrInfos {
		// make each dial non-blocking
		go func(info peer.AddrInfo) {
			if err := s.connectWithPeer(s.ctx, info); err != nil {
				log.Debug("Could not connect with peer", "err", err)
			}
		}(info)
	}
	return nil
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
