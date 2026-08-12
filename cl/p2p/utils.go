package p2p

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
)

func ConvertToInterfacePubkey(pubkey *ecdsa.PublicKey) (crypto.PubKey, error) {
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

func ConvertToAddrInfo(node *enode.Node) (*peer.AddrInfo, multiaddr.Multiaddr, error) {
	multiAddr, err := ConvertToSingleMultiAddr(node)
	if err != nil {
		return nil, nil, err
	}
	info, err := peer.AddrInfoFromP2pAddr(multiAddr)
	if err != nil {
		return nil, nil, err
	}
	return info, multiAddr, nil
}

func ParseStaticPeer(value string) (multiaddr.Multiaddr, error) {
	node, nodeErr := enode.Parse(enode.ValidSchemes, value)
	if nodeErr == nil {
		return ConvertToSingleMultiAddr(node)
	}

	addr, addrErr := multiaddr.NewMultiaddr(value)
	if addrErr != nil {
		return nil, fmt.Errorf("static peer is neither a valid node record nor a multiaddr: %w", errors.Join(nodeErr, addrErr))
	}
	info, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid libp2p static peer: %w", err)
	}
	if len(info.Addrs) == 0 {
		return nil, errors.New("libp2p static peer does not provide a dial address")
	}
	protocols := addr.Protocols()
	if len(protocols) != 3 || protocols[1].Code != multiaddr.P_TCP || protocols[2].Code != multiaddr.P_P2P {
		return nil, errors.New("libp2p static peer must use a direct TCP address")
	}
	switch protocols[0].Code {
	case multiaddr.P_IP4, multiaddr.P_IP6, multiaddr.P_DNS, multiaddr.P_DNS4, multiaddr.P_DNS6:
	default:
		return nil, errors.New("libp2p static peer must use an IP or DNS address")
	}
	portValue, err := addr.ValueForProtocol(multiaddr.P_TCP)
	if err != nil {
		return nil, errors.New("libp2p static peer does not provide a TCP address")
	}
	port, err := strconv.ParseUint(portValue, 10, 16)
	if err != nil || port == 0 {
		return nil, errors.New("libp2p static peer does not provide a valid TCP port")
	}
	for _, protocol := range []int{multiaddr.P_IP4, multiaddr.P_IP6} {
		ipValue, err := addr.ValueForProtocol(protocol)
		if err == nil && net.ParseIP(ipValue).IsUnspecified() {
			return nil, errors.New("libp2p static peer uses an unspecified IP address")
		}
	}
	return addr, nil
}

func ConvertToSingleMultiAddr(node *enode.Node) (multiaddr.Multiaddr, error) {
	if node.TCP() == 0 {
		return nil, fmt.Errorf("node %s does not provide a tcp port", node.ID())
	}
	pubkey := node.Pubkey()
	assertedKey, err := ConvertToInterfacePubkey(pubkey)
	if err != nil {
		return nil, fmt.Errorf("could not get pubkey: %w", err)
	}
	id, err := peer.IDFromPublicKey(assertedKey)
	if err != nil {
		return nil, fmt.Errorf("could not get peer id: %w", err)
	}
	return MultiAddressBuilderWithID(node.IP().String(), "tcp", uint(node.TCP()), id)
}

func MultiAddressBuilderWithID(ipAddr, protocol string, port uint, id peer.ID) (multiaddr.Multiaddr, error) {
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

func ConvertToMultiAddr(nodes []*enode.Node) []multiaddr.Multiaddr {
	multiAddrs := []multiaddr.Multiaddr{}
	for _, node := range nodes {
		// ignore nodes with no ip address stored
		if node.IP() == nil {
			continue
		}
		multiAddr, err := ConvertToSingleMultiAddr(node)
		if err != nil {
			log.Debug("[Sentinel] Could not convert to multiAddr", "err", err)
			continue
		}
		multiAddrs = append(multiAddrs, multiAddr)
	}
	return multiAddrs
}
