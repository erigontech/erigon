package observer

import (
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"net"
	"net/url"
)

func nodeID(node *enode.Node) (database.NodeID, error) {
	if node.Incomplete() {
		return "", errors.New("nodeID not implemented for incomplete nodes")
	}
	nodeURL, err := url.Parse(node.URLv4())
	if err != nil {
		return "", fmt.Errorf("failed to parse node URL: %w", err)
	}
	id := nodeURL.User.Username()
	return database.NodeID(id), nil
}

func makeNodeAddr(node *enode.Node) database.NodeAddr {
	var addr database.NodeAddr

	var ipEntry enr.IPv4
	if node.Load(&ipEntry) == nil {
		addr.IP = net.IP(ipEntry)
	}

	var ipV6Entry enr.IPv6
	if node.Load(&ipV6Entry) == nil {
		addr.IPv6.IP = net.IP(ipEntry)
	}

	var portDiscEntry enr.UDP
	if (addr.IP != nil) && (node.Load(&portDiscEntry) == nil) {
		addr.PortDisc = uint16(portDiscEntry)
	}

	var ipV6PortDiscEntry enr.UDP6
	if (addr.IPv6.IP != nil) && (node.Load(&ipV6PortDiscEntry) == nil) {
		addr.IPv6.PortDisc = uint16(ipV6PortDiscEntry)
	}

	var portRLPxEntry enr.TCP
	if (addr.IP != nil) && (node.Load(&portRLPxEntry) == nil) {
		addr.PortRLPx = uint16(portRLPxEntry)
	}

	var ipV6PortRLPxEntry enr.TCP
	if (addr.IPv6.IP != nil) && (node.Load(&ipV6PortRLPxEntry) == nil) {
		addr.IPv6.PortRLPx = uint16(ipV6PortRLPxEntry)
	}

	return addr
}

func makeNodeFromAddr(id database.NodeID, addr database.NodeAddr) (*enode.Node, error) {
	rec := new(enr.Record)

	pubkey, err := parseHexPublicKey(string(id))
	if err != nil {
		return nil, err
	}
	rec.Set((*enode.Secp256k1)(pubkey))

	if addr.IP != nil {
		rec.Set(enr.IP(addr.IP))
	}
	if addr.IPv6.IP != nil {
		rec.Set(enr.IPv6(addr.IPv6.IP))
	}
	if addr.PortDisc != 0 {
		rec.Set(enr.UDP(addr.PortDisc))
	}
	if addr.PortRLPx != 0 {
		rec.Set(enr.TCP(addr.PortRLPx))
	}
	if addr.IPv6.PortDisc != 0 {
		rec.Set(enr.UDP6(addr.IPv6.PortDisc))
	}
	if addr.IPv6.PortRLPx != 0 {
		rec.Set(enr.TCP6(addr.IPv6.PortRLPx))
	}

	rec.Set(enr.ID("unsigned"))
	node, err := enode.New(enr.SchemeMap{"unsigned": noSignatureIDScheme{}}, rec)
	if err != nil {
		return nil, fmt.Errorf("failed to make a node: %w", err)
	}
	return node, nil
}

type noSignatureIDScheme struct {
	enode.V4ID
}

func (noSignatureIDScheme) Verify(_ *enr.Record, _ []byte) error {
	return nil
}

func parseHexPublicKey(keyStr string) (*ecdsa.PublicKey, error) {
	nodeWithPubkey, err := enode.ParseV4("enode://" + keyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode a public key: %w", err)
	}
	return nodeWithPubkey.Pubkey(), nil
}

func parseHexPublicKeys(hexKeys []string) ([]*ecdsa.PublicKey, error) {
	if hexKeys == nil {
		return nil, nil
	}
	keys := make([]*ecdsa.PublicKey, 0, len(hexKeys))
	for _, keyStr := range hexKeys {
		key, err := parseHexPublicKey(keyStr)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func hexEncodePublicKey(key *ecdsa.PublicKey) string {
	return hex.EncodeToString(crypto.MarshalPubkey(key))
}

func hexEncodePublicKeys(keys []*ecdsa.PublicKey) []string {
	if keys == nil {
		return nil
	}
	hexKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		keyStr := hexEncodePublicKey(key)
		hexKeys = append(hexKeys, keyStr)
	}
	return hexKeys
}
