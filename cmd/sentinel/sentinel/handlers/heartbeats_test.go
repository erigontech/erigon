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

package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/peers"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testLocalMetadataV2 = &cltypes.MetadataV2{
		SeqNumber: 42,
		Attnets:   43,
		Syncnets:  44,
	}
	testLocalMetadataV1 = &cltypes.MetadataV1{
		SeqNumber: testLocalMetadataV2.SeqNumber,
		Attnets:   testLocalMetadataV2.Attnets,
	}
)

func initializeNetwork(t *testing.T, ctx context.Context) (*ConsensusHandlers, host.Host, host.Host) {
	h1, err := basichost.NewHost(swarmt.GenSwarm(t), nil)
	require.NoError(t, err)

	h2, err := basichost.NewHost(swarmt.GenSwarm(t), nil)
	require.NoError(t, err)
	h2pi := h2.Peerstore().PeerInfo(h2.ID())
	require.NoError(t, h1.Connect(ctx, h2pi))

	return NewConsensusHandlers(h2, &peers.Peers{}, testLocalMetadataV2), h1, h2
}

func TestHeartbeatHandlers(t *testing.T) {
	tests := map[string]struct {
		protocol       string
		writePacket    communication.Packet
		gotPacket      communication.Packet
		expectedPacket communication.Packet
	}{
		"ping": {
			protocol:       PingProtocolV1,
			writePacket:    &cltypes.Ping{Id: 32},
			gotPacket:      &cltypes.Ping{},
			expectedPacket: &cltypes.Ping{Id: testLocalMetadataV2.SeqNumber},
		},
		"goodbye": {
			protocol:       GoodbyeProtocolV1,
			writePacket:    &cltypes.Ping{Id: 1},
			gotPacket:      &cltypes.Ping{},
			expectedPacket: &cltypes.Ping{Id: 1},
		},
		"status": {
			protocol:       StatusProtocolV1,
			writePacket:    &cltypes.Status{HeadSlot: 666999},
			gotPacket:      &cltypes.Status{},
			expectedPacket: &cltypes.Status{HeadSlot: 666999},
		},
		"metadatav1": {
			protocol:       MetadataProtocolV1,
			writePacket:    nil,
			gotPacket:      &cltypes.MetadataV1{},
			expectedPacket: testLocalMetadataV1,
		},
		"metadatav2": {
			protocol:       MetadataProtocolV2,
			writePacket:    nil,
			gotPacket:      &cltypes.MetadataV2{},
			expectedPacket: testLocalMetadataV2,
		},
	}
	ctx := context.TODO()
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			handlers, h1, h2 := initializeNetwork(t, ctx)
			defer h1.Close()
			defer h2.Close()
			handlers.Start()

			stream, err := h1.NewStream(ctx, h2.ID(), protocol.ID(tc.protocol))
			require.NoError(t, err)
			codec := ssz_snappy.NewStreamCodec(stream)

			// Write packet.
			err = codec.WritePacket(tc.writePacket)
			require.NoError(t, err)
			require.NoError(t, codec.CloseWriter())
			time.Sleep(100 * time.Millisecond)

			// Read packet.
			code := make([]byte, 1)
			stream.Read(code)
			assert.Equal(t, code, []byte{SuccessfulResponsePrefix})
			_, err = codec.Decode(tc.gotPacket)
			require.NoError(t, err)

			// Assert on expected packet.
			assert.Equal(t, tc.gotPacket, tc.expectedPacket)
		})
	}
}
