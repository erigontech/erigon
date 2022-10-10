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

	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/peers"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/protocol"
	basichost "github.com/libp2p/go-libp2p/p2p/host/basic"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func initializeNetwork(t *testing.T, ctx context.Context) (*ConsensusHandlers, host.Host, host.Host) {
	h1, err := basichost.NewHost(swarmt.GenSwarm(t), nil)
	require.NoError(t, err)

	h2, err := basichost.NewHost(swarmt.GenSwarm(t), nil)
	require.NoError(t, err)
	h2pi := h2.Peerstore().PeerInfo(h2.ID())
	require.NoError(t, h1.Connect(ctx, h2pi))

	return NewConsensusHandlers(h2, &peers.Peers{}, &cltypes.MetadataV2{}), h1, h2
}

func TestPingHandler(t *testing.T) {
	ctx := context.TODO()

	handlers, h1, h2 := initializeNetwork(t, ctx)
	defer h1.Close()
	defer h2.Close()
	handlers.Start()

	stream, err := h1.NewStream(ctx, h2.ID(), protocol.ID(PingProtocolV1))
	require.NoError(t, err)
	packet := &cltypes.Ping{
		Id: 32,
	}
	codec := ssz_snappy.NewStreamCodec(stream)
	err = codec.WritePacket(packet)
	require.NoError(t, err)
	require.NoError(t, codec.CloseWriter())
	time.Sleep(100 * time.Millisecond)
	r := &cltypes.Ping{}

	code := make([]byte, 1)
	stream.Read(code)
	assert.Equal(t, code, []byte{SuccessfullResponsePrefix})

	_, err = codec.Decode(r)
	require.NoError(t, err)

	assert.Equal(t, r, packet)
}

func TestStatusHandler(t *testing.T) {
	ctx := context.TODO()

	handlers, h1, h2 := initializeNetwork(t, ctx)
	defer h1.Close()
	defer h2.Close()
	handlers.Start()

	stream, err := h1.NewStream(ctx, h2.ID(), protocol.ID(StatusProtocolV1))
	require.NoError(t, err)
	packet := &cltypes.Status{
		HeadSlot: 666999,
	}
	codec := ssz_snappy.NewStreamCodec(stream)
	require.NoError(t, codec.WritePacket(packet))
	require.NoError(t, codec.CloseWriter())
	time.Sleep(100 * time.Millisecond)
	r := &cltypes.Status{}

	code := make([]byte, 1)
	stream.Read(code)
	assert.Equal(t, code, []byte{SuccessfullResponsePrefix})

	_, err = codec.Decode(r)
	require.NoError(t, err)

	assert.Equal(t, r, packet)
}
