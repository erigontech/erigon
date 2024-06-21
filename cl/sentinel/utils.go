// Copyright 2022 Erigon-Lightclient contributors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// package sentinel
package sentinel

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/pion/randutil"
	"github.com/prysmaticlabs/go-bitfield"
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

var shuffleSource = randutil.NewMathRandomGenerator()

func (s *Sentinel) oneSlotDuration() time.Duration {
	return time.Duration(s.cfg.BeaconConfig.SecondsPerSlot) * time.Second
}

func (s *Sentinel) oneEpochDuration() time.Duration {
	return s.oneSlotDuration() * time.Duration(s.cfg.BeaconConfig.SlotsPerEpoch)
}

// the cap for `inMesh` time scoring.
func (s *Sentinel) inMeshCap() float64 {
	return float64((3600 * time.Second) / s.oneSlotDuration())
}

// updateENRAttSubnets calls the ENR to notify other peers their attnets preferences.
func (s *Sentinel) updateENRAttSubnets(subnetIndex int, on bool) {
	subnetField := bitfield.NewBitvector64()
	if err := s.listener.LocalNode().Node().Load(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &subnetField)); err != nil {
		log.Error("[Sentinel] Could not load attSubnetKey", "err", err)
		return
	}
	subnetField = common.Copy(subnetField)
	if len(subnetField) <= subnetIndex/8 {
		log.Error("[Sentinel] Subnet index out of range", "subnetIndex", subnetIndex, "len", len(subnetField))
		return
	}
	if on {
		subnetField[subnetIndex/8] |= 1 << (subnetIndex % 8)
	} else {
		subnetField[subnetIndex/8] &^= 1 << (subnetIndex % 8)
	}
	s.listener.LocalNode().Set(enr.WithEntry(s.cfg.NetworkConfig.AttSubnetKey, &subnetField))
}

// updateENRSyncNets calls the ENR to notify other peers their attnets preferences.
func (s *Sentinel) updateENRSyncNets(subnetIndex int, on bool) {
	subnetField := bitfield.NewBitvector4()
	if err := s.listener.LocalNode().Node().Load(enr.WithEntry(s.cfg.NetworkConfig.SyncCommsSubnetKey, &subnetField)); err != nil {
		log.Error("[Sentinel] Could not load syncCommsSubnetKey", "err", err)
		return
	}
	subnetField = common.Copy(subnetField)
	if len(subnetField) <= subnetIndex/8 {
		log.Error("[Sentinel] Subnet index out of range", "subnetIndex", subnetIndex, "len", len(subnetField))
		return
	}
	if on {
		subnetField[subnetIndex/8] |= 1 << (subnetIndex % 8)
	} else {
		subnetField[subnetIndex/8] &^= 1 << (subnetIndex % 8)
	}
	s.listener.LocalNode().Set(enr.WithEntry(s.cfg.NetworkConfig.SyncCommsSubnetKey, &subnetField))
}

// updateENROnSubscription updates the ENR based on the subscription status to subnets/syncnets.
func (s *Sentinel) updateENROnSubscription(topicName string, subscribe bool) {
	s.metadataLock.Lock()
	defer s.metadataLock.Unlock()
	//. topic: /eth2/d31f6191/beacon_attestation_45/ssz_snappy
	// extract third part of the topic name
	parts := strings.Split(topicName, "/")
	if len(parts) < 4 {
		return
	}
	part := parts[3]
	for i := 0; i < int(s.cfg.NetworkConfig.AttestationSubnetCount); i++ {
		if part == gossip.TopicNameBeaconAttestation(uint64(i)) {
			log.Info("[Sentinel] Update ENR on subscription", "subnet", i, "subscribe", subscribe, "type", "attestation")
			s.updateENRAttSubnets(i, subscribe)
			return
		}
	}
	for i := 0; i < int(s.cfg.BeaconConfig.SyncCommitteeSubnetCount); i++ {
		if part == gossip.TopicNameSyncCommittee(i) {
			log.Info("[Sentinel] Update ENR on subscription", "subnet", i, "subscribe", subscribe, "type", "syncnets")
			s.updateENRSyncNets(i, subscribe)
			return
		}
	}
}
