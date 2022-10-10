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

package ssz_snappy

import (
	"context"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataPacketStream(t *testing.T) {
	ctx := context.Background()

	h1, err := basichost.NewHost(swarmt.GenSwarm(t), nil)
	require.NoError(t, err)
	defer h1.Close()

	h2, err := basichost.NewHost(swarmt.GenSwarm(t), nil)
	require.NoError(t, err)
	defer h2.Close()
	mock32 := common.HexToHash("9e85f8605954286b4f1958cbd7017041025f6a6000858b09caf0b9b20699662d")
	mock64 := append(mock32[:], mock32[:]...)
	mockHeader := &cltypes.BeaconBlockHeader{
		Slot:          19,
		ProposerIndex: 24,
	}
	packet := &cltypes.LightClientFinalityUpdate{
		AttestedHeader:  mockHeader,
		FinalizedHeader: mockHeader,
		FinalityBranch:  [][]byte{mock32[:], mock32[:], mock32[:], mock32[:], mock32[:], mock32[:]},
		SyncAggregate: &cltypes.SyncAggregate{
			SyncCommiteeBits: mock64,
		},
		SignatureSlot: 66,
	}

	doneCh := make(chan struct{})
	h2.SetStreamHandler(protocol.TestingID, func(stream network.Stream) {
		p := &cltypes.LightClientFinalityUpdate{}
		codecA := NewStreamCodec(stream)
		_, err := codecA.Decode(p)
		require.NoError(t, err)
		require.Equal(t, p.SignatureSlot, uint64(66))
		doneCh <- struct{}{}
	})

	h2pi := h2.Peerstore().PeerInfo(h2.ID())
	require.NoError(t, h1.Connect(ctx, h2pi))

	s, err := h1.NewStream(ctx, h2pi.ID, protocol.TestingID)
	require.NoError(t, err)

	codec := NewStreamCodec(s)
	err = codec.WritePacket(packet)
	require.NoError(t, err)
	require.NoError(t, codec.CloseWriter())
	timeout := time.NewTimer(2 * time.Second)
	select {
	case <-doneCh:
	case <-timeout.C:
		t.Fail()
	}
}

// See https://github.com/libp2p/go-libp2p-pubsub/issues/426
func TestGossipCodecTest(t *testing.T) {
	codec := NewGossipCodec(nil, nil).(*GossipCodec)
	val := &cltypes.Ping{
		Id: 89,
	}
	ans, err := utils.EncodeSSZSnappy(val)
	require.NoError(t, err)

	decoded := &cltypes.Ping{}
	require.NoError(t, codec.decodeData(decoded, ans))
	assert.Equal(t, decoded, val)
}
