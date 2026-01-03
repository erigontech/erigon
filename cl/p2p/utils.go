package p2p

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func convertToInterfacePubkey(pubkey *ecdsa.PublicKey) (crypto.PubKey, error) {
	xVal, yVal := new(secp256k1.FieldVal), new(secp256k1.FieldVal)
	overflows := xVal.SetByteSlice(pubkey.X.Bytes())
	if overflows {
		return nil, errors.New("x value overflows")
	}
	overflows = yVal.SetByteSlice(pubkey.Y.Bytes())
	if overflows {
		return nil, errors.New("y value overflows")
	}
	newKey := crypto.PubKey((*crypto.Secp256k1PublicKey)(secp256k1.NewPublicKey(xVal, yVal)))
	// Zero out temporary values.
	xVal.Zero()
	yVal.Zero()
	return newKey, nil
}

func convertToAddrInfo(node *enode.Node) (*peer.AddrInfo, multiaddr.Multiaddr, error) {
	multiAddr, err := convertToSingleMultiAddr(node)
	if err != nil {
		return nil, nil, err
	}
	info, err := peer.AddrInfoFromP2pAddr(multiAddr)
	if err != nil {
		return nil, nil, err
	}
	return info, multiAddr, nil
}

func convertToSingleMultiAddr(node *enode.Node) (multiaddr.Multiaddr, error) {
	pubkey := node.Pubkey()
	assertedKey, err := convertToInterfacePubkey(pubkey)
	if err != nil {
		return nil, fmt.Errorf("could not get pubkey: %w", err)
	}
	id, err := peer.IDFromPublicKey(assertedKey)
	if err != nil {
		return nil, fmt.Errorf("could not get peer id: %w", err)
	}
	return multiAddressBuilderWithID(node.IP().String(), "tcp", uint(node.TCP()), id)
}

func multiAddressBuilderWithID(ipAddr, protocol string, port uint, id peer.ID) (multiaddr.Multiaddr, error) {
	parsedIP := net.ParseIP(ipAddr)
	if parsedIP.To4() == nil && parsedIP.To16() == nil {
		return nil, fmt.Errorf("invalid ip address provided: %s", ipAddr)
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
	multiAddrs := []multiaddr.Multiaddr{}
	for _, node := range nodes {
		// ignore nodes with no ip address stored
		if node.IP() == nil {
			continue
		}
		multiAddr, err := convertToSingleMultiAddr(node)
		if err != nil {
			log.Debug("[Sentinel] Could not convert to multiAddr", "err", err)
			continue
		}
		multiAddrs = append(multiAddrs, multiAddr)
	}
	return multiAddrs
}
