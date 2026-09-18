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
	multiAddrs, err := convertToMultiAddrs(node)
	if err != nil {
		return nil, nil, err
	}
	infos, err := peer.AddrInfosFromP2pAddrs(multiAddrs...)
	if err != nil {
		return nil, nil, err
	}
	if len(infos) != 1 {
		return nil, nil, errors.New("node addresses do not resolve to one peer")
	}
	return &infos[0], multiAddrs[0], nil
}

func ParseStaticPeer(value string) (multiaddr.Multiaddr, error) {
	addrs, err := ParseStaticPeerAddrs(value)
	if err != nil {
		return nil, err
	}
	return addrs[0], nil
}

func ParseStaticPeerAddrs(value string) ([]multiaddr.Multiaddr, error) {
	node, nodeErr := enode.Parse(enode.ValidSchemes, value)
	if nodeErr == nil {
		return convertToMultiAddrs(node)
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
	portProtocol := multiaddr.P_TCP
	switch {
	case len(protocols) == 3 && protocols[1].Code == multiaddr.P_TCP && protocols[2].Code == multiaddr.P_P2P:
	case len(protocols) == 4 && protocols[1].Code == multiaddr.P_UDP && protocols[2].Code == multiaddr.P_QUIC_V1 && protocols[3].Code == multiaddr.P_P2P:
		portProtocol = multiaddr.P_UDP
	default:
		return nil, errors.New("libp2p static peer must use a direct TCP or QUIC address")
	}
	switch protocols[0].Code {
	case multiaddr.P_IP4, multiaddr.P_IP6, multiaddr.P_DNS, multiaddr.P_DNS4, multiaddr.P_DNS6:
	default:
		return nil, errors.New("libp2p static peer must use an IP or DNS address")
	}
	portValue, err := addr.ValueForProtocol(portProtocol)
	if err != nil {
		return nil, errors.New("libp2p static peer does not provide a transport address")
	}
	port, err := strconv.ParseUint(portValue, 10, 16)
	if err != nil || port == 0 {
		return nil, errors.New("libp2p static peer does not provide a valid transport port")
	}
	for _, protocol := range []int{multiaddr.P_IP4, multiaddr.P_IP6} {
		ipValue, err := addr.ValueForProtocol(protocol)
		if err == nil && net.ParseIP(ipValue).IsUnspecified() {
			return nil, errors.New("libp2p static peer uses an unspecified IP address")
		}
	}
	return []multiaddr.Multiaddr{addr}, nil
}

func ParseBootstrapNodes(values []string) (discoveryNodes, directPeers, unsupportedPeers []string, err error) {
	for _, value := range values {
		if _, nodeErr := enode.Parse(enode.ValidSchemes, value); nodeErr == nil {
			discoveryNodes = append(discoveryNodes, value)
			continue
		}
		addr, addrErr := multiaddr.NewMultiaddr(value)
		if addrErr != nil {
			return nil, nil, nil, fmt.Errorf("invalid bootstrap node %q: %w", value, addrErr)
		}
		info, infoErr := peer.AddrInfoFromP2pAddr(addr)
		if infoErr != nil {
			return nil, nil, nil, fmt.Errorf("invalid libp2p bootstrap node %q: %w", value, infoErr)
		}
		if len(info.Addrs) == 0 {
			return nil, nil, nil, fmt.Errorf("libp2p bootstrap node %q does not provide a dial address", value)
		}
		if _, directErr := ParseStaticPeer(value); directErr != nil {
			unsupportedPeers = append(unsupportedPeers, value)
			continue
		}
		directPeers = append(directPeers, value)
	}
	if len(values) > 0 && len(discoveryNodes) == 0 && len(directPeers) == 0 {
		return nil, nil, nil, errors.New("bootstrap nodes do not contain a supported ENR or direct TCP or QUIC libp2p address")
	}
	return discoveryNodes, directPeers, unsupportedPeers, nil
}

func ConvertToSingleMultiAddr(node *enode.Node) (multiaddr.Multiaddr, error) {
	multiAddrs, err := convertToMultiAddrs(node)
	if err != nil {
		return nil, err
	}
	return multiAddrs[0], nil
}

func convertToMultiAddrs(node *enode.Node) ([]multiaddr.Multiaddr, error) {
	pubkey := node.Pubkey()
	assertedKey, err := ConvertToInterfacePubkey(pubkey)
	if err != nil {
		return nil, fmt.Errorf("could not get pubkey: %w", err)
	}
	id, err := peer.IDFromPublicKey(assertedKey)
	if err != nil {
		return nil, fmt.Errorf("could not get peer id: %w", err)
	}

	multiAddrs := make([]multiaddr.Multiaddr, 0, 2)
	if endpoint, ok := node.QUICEndpoint(); ok {
		addr, err := quicMultiAddressBuilderWithID(endpoint.Addr().String(), uint(endpoint.Port()), id)
		if err != nil {
			return nil, err
		}
		multiAddrs = append(multiAddrs, addr)
	}
	if endpoint, ok := node.TCPEndpoint(); ok {
		addr, err := MultiAddressBuilderWithID(endpoint.Addr().String(), "tcp", uint(endpoint.Port()), id)
		if err != nil {
			return nil, err
		}
		multiAddrs = append(multiAddrs, addr)
	}
	if len(multiAddrs) == 0 {
		return nil, fmt.Errorf("node %s does not provide a QUIC or TCP port", node.ID())
	}
	return multiAddrs, nil
}

func MultiAddressBuilderWithID(ipAddr, protocol string, port uint, id peer.ID) (multiaddr.Multiaddr, error) {
	return multiAddressBuilderWithTransport(ipAddr, fmt.Sprintf("%s/%d", protocol, port), id)
}

func quicMultiAddressBuilderWithID(ipAddr string, port uint, id peer.ID) (multiaddr.Multiaddr, error) {
	return multiAddressBuilderWithTransport(ipAddr, fmt.Sprintf("udp/%d/quic-v1", port), id)
}

func multiAddressBuilderWithTransport(ipAddr, transport string, id peer.ID) (multiaddr.Multiaddr, error) {
	parsedIP := net.ParseIP(ipAddr)
	if parsedIP.To4() == nil && parsedIP.To16() == nil {
		return nil, fmt.Errorf("invalid ip address provided: %s", ipAddr)
	}
	if id.String() == "" {
		return nil, errors.New("empty peer id given")
	}
	if parsedIP.To4() != nil {
		return multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/%s/p2p/%s", ipAddr, transport, id.String()))
	}
	return multiaddr.NewMultiaddr(fmt.Sprintf("/ip6/%s/%s/p2p/%s", ipAddr, transport, id.String()))
}

func ConvertToMultiAddr(nodes []*enode.Node) []multiaddr.Multiaddr {
	multiAddrs := []multiaddr.Multiaddr{}
	for _, node := range nodes {
		// ignore nodes with no ip address stored
		if node.IP() == nil {
			continue
		}
		nodeAddrs, err := convertToMultiAddrs(node)
		if err != nil {
			log.Debug("[Sentinel] Could not convert to multiAddr", "err", err)
			continue
		}
		multiAddrs = append(multiAddrs, nodeAddrs...)
	}
	return multiAddrs
}
