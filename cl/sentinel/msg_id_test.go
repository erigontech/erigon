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

package sentinel

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/snappy"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestMsgID(t *testing.T) {
	n := clparams.NetworkConfigs[chainspec.MainnetChainID]
	s := &Sentinel{
		ctx: context.TODO(),
		cfg: &SentinelConfig{
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
	// Create object to hash
	combinedObj := append(n.MessageDomainInvalidSnappy[:], topicLenBytes...)
	combinedObj = append(combinedObj, tpc...)
	combinedObj = append(combinedObj, pMsg.Data...)
	hashedData := utils.Sha256(combinedObj)
	msgID := string(hashedData[:20])
	require.Equal(t, msgID, s.msgId(pMsg), "Got incorrect msg id")

	validObj := [32]byte{'v', 'a', 'l', 'i', 'd'}
	enc := snappy.Encode(nil, validObj[:])
	nMsg := &pubsubpb.Message{Data: enc, Topic: &tpc}
	// Create object to hash
	combinedObj = append(n.MessageDomainValidSnappy[:], topicLenBytes...)
	combinedObj = append(combinedObj, tpc...)
	combinedObj = append(combinedObj, validObj[:]...)
	hashedData = utils.Sha256(combinedObj)
	msgID = string(hashedData[:20])
	require.Equal(t, msgID, s.msgId(nMsg), "Got incorrect msg id")
}
