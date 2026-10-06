// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"fmt"
	"testing"

	"github.com/golang/snappy"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common/crypto"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestMsgID(t *testing.T) {
	n := clparams.NetworkConfigs[chainspec.MainnetChainID]
	s := &p2pManager{
		cfg: &P2PConfig{
			BeaconConfig:  &clparams.MainnetBeaconConfig,
			NetworkConfig: &n,
		},
	}
	d := [4]byte{108, 122, 33, 65}
	tpc := fmt.Sprintf("/eth2/%x/beacon_block", d)
	topicLen := uint64(len(tpc))
	topicLenBytes := utils.Uint64ToLE(topicLen)
	invalidSnappy := [32]byte{'J', 'U', 'N', 'K'}
	pMsg := &pubsubpb.Message{Data: invalidSnappy[:], Topic: &tpc}
	combinedObj := make([]byte, 0, len(n.MessageDomainInvalidSnappy)+len(topicLenBytes)+len(tpc)+len(pMsg.Data))
	combinedObj = append(combinedObj, n.MessageDomainInvalidSnappy[:]...)
	combinedObj = append(combinedObj, topicLenBytes...)
	combinedObj = append(combinedObj, tpc...)
	combinedObj = append(combinedObj, pMsg.Data...)
	hashedData := crypto.Sha256(combinedObj)
	msgID := string(hashedData[:20])
	require.Equal(t, msgID, s.msgId(pMsg), "Got incorrect msg id")

	validObj := [32]byte{'v', 'a', 'l', 'i', 'd'}
	enc := snappy.Encode(nil, validObj[:])
	nMsg := &pubsubpb.Message{Data: enc, Topic: &tpc}
	combinedObj = make([]byte, 0, len(n.MessageDomainValidSnappy)+len(topicLenBytes)+len(tpc)+len(validObj))
	combinedObj = append(combinedObj, n.MessageDomainValidSnappy[:]...)
	combinedObj = append(combinedObj, topicLenBytes...)
	combinedObj = append(combinedObj, tpc...)
	combinedObj = append(combinedObj, validObj[:]...)
	hashedData = crypto.Sha256(combinedObj)
	msgID = string(hashedData[:20])
	require.Equal(t, msgID, s.msgId(nMsg), "Got incorrect msg id")
}

func TestMsgIDUsesInvalidDomainAbovePayloadBounds(t *testing.T) {
	n := clparams.NetworkConfigs[chainspec.MainnetChainID]
	s := &p2pManager{cfg: &P2PConfig{BeaconConfig: &clparams.MainnetBeaconConfig, NetworkConfig: &n}}
	expectedID := func(domain [4]byte, topic string, payload []byte) string {
		combined := append([]byte{}, domain[:]...)
		combined = append(combined, utils.Uint64ToLE(uint64(len(topic)))...)
		combined = append(combined, topic...)
		combined = append(combined, payload...)
		h := crypto.Sha256(combined)
		return string(h[:20])
	}

	blockTopic := "/eth2/6c7a2141/beacon_block/ssz_snappy"
	aboveNetworkLimit := make([]byte, n.GossipMaxSize+1)
	msg := &pubsubpb.Message{Data: snappy.Encode(nil, aboveNetworkLimit), Topic: &blockTopic}
	require.Equal(t, expectedID(n.MessageDomainInvalidSnappy, blockTopic, msg.Data), s.msgId(msg))

	aggregateTopic := "/eth2/6c7a2141/beacon_aggregate_and_proof/ssz_snappy"
	bound := gossip.MaxUncompressedSize(gossip.TopicNameBeaconAggregateAndProof, &clparams.MainnetBeaconConfig, &n)
	require.Less(t, bound, n.GossipMaxSize)
	fitting := make([]byte, bound)
	msg = &pubsubpb.Message{Data: snappy.Encode(nil, fitting), Topic: &aggregateTopic}
	require.Equal(t, expectedID(n.MessageDomainValidSnappy, aggregateTopic, fitting), s.msgId(msg))
	msg = &pubsubpb.Message{Data: snappy.Encode(nil, make([]byte, bound+1)), Topic: &aggregateTopic}
	require.Equal(t, expectedID(n.MessageDomainInvalidSnappy, aggregateTopic, msg.Data), s.msgId(msg))
}
