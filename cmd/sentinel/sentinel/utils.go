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
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
	"strings"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/peers"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func convertToInterfacePubkey(pubkey *ecdsa.PublicKey) (crypto.PubKey, error) {
	xVal, yVal := new(btcec.FieldVal), new(btcec.FieldVal)
	overflows := xVal.SetByteSlice(pubkey.X.Bytes())
	if overflows {
		return nil, fmt.Errorf("x value overflows")
	}
	overflows = yVal.SetByteSlice(pubkey.Y.Bytes())
	if overflows {
		return nil, fmt.Errorf("y value overflows")
	}
	newKey := crypto.PubKey((*crypto.Secp256k1PublicKey)(btcec.NewPublicKey(xVal, yVal)))
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
		return nil, fmt.Errorf("empty peer id given")
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

// will iterate onto randoms nodes until our sentinel connects to one
func connectToRandomPeer(s *Sentinel, topic string) (peerInfo peer.ID, err error) {
	var sub *GossipSubscription
	for t, currSub := range s.subManager.subscriptions {
		if strings.Contains(t, topic) {
			sub = currSub
		}
	}

	if sub == nil {
		return peer.ID(""), fmt.Errorf("no peers")
	}

	validPeerList := sub.topic.ListPeers()
	if len(validPeerList) == 0 {
		return peer.ID(""), fmt.Errorf("no peers")
	}

	connectedPeer := false
	maxTries := peers.DefaultMaxPeers
	tries := 0
	for !connectedPeer {
		if tries >= maxTries {
			break
		}
		tries++
		index := int64(0)
		if len(validPeerList) > 1 {
			n, err := rand.Int(rand.Reader, big.NewInt(int64(len(validPeerList)-1)))
			if err != nil {
				panic(err)
			}
			index = n.Int64()
		}

		node := validPeerList[index]
		if !isPeerWhitelisted(node, validPeerList) {
			continue
		}

		if !s.peers.IsPeerAvaiable(node) {
			continue
		}

		return node, nil
	}

	return peer.ID(""), fmt.Errorf("failed to connect to peer")

}
func isPeerWhitelisted(peer peer.ID, whitelist []peer.ID) bool {
	for _, currPeer := range whitelist {
		if peer == currPeer {
			return true
		}
	}
	return false
}
