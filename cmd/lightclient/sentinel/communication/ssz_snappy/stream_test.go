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
	"bytes"
	"context"
	"encoding/hex"
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

var (
	// Hex strings to read into []byte
	finalityUpdateResponse   = "4a26c58bc804ff060000734e61507059014c020083eec0dbdf664c00000000004de90600000000001781be17c85df24278e5a08c2cebbab21dcd60593574c231f47075262425fb7e613fae1212348abcbb15ed2e6e99b1ce5d93c8013eb66009d824721f0a068db1be1d557934252345e99093d8e6b227b4ee19423a35d984a56489bfcb57d2647b80664c00000000009b480200000000003c68b551600735c179abfbd5c74341a8d524365217e70c031d13acc22b003e4938e9d045749e349689d211972dca6c9792a364f309b5c202f25d87b45b3f55b07feaab3b47760bb2436849a855b52e08a61be89156b90d5080f5719b9989274b34630200000000000000000000000000000000000000000000000000000000004165581604f4b02f5dc241254fdde469a55c8ec1ba3a431486f187227a7d7339ac7a41b9c769bd31b1f1b1d22163f4d61a5f54bdaee0c136ec3fddb84e6d7ffddd55a8a16926d19569039f7af4feb2ff5b4d78bb7d8e3378008d32ae06db97ef0055e5bc3b4060c2a667378780d589571cb7fe0af24cb59a2245bfbcc73728645f17cfe000a126a583ceffd3fa2362fbad8368cbfdf9b197e2d7ab8af059a924ffffbfffffffffffff5f7fffffffffffffffffffffdffffffefffffffffffffffffffffffffffffbfdfefffffbfffff7fff9dfbfffffffffffffffffffffffff817d6d9bd4199e2c7296c91a574e5082af3a28ad831793265a28181ac5795f61e93575902ef7668c12ebc3f151949ae017052f8eb5ef7a89901fa07a0b24816fa6ffd207e973106bc5a207124ca49401d8f0f3d325c9feedcb5edb7e0e386d2ee0664c0000000000"
	optimisticUpdateResponse = "4a26c58b9802ff060000734e61507059011c010082d6f5f3ff794c0000000000a49a0200000000009a768d1014238eee34b812ac38d4d5fac898baa2b0051fa1ab70f3e8845f3489070bfacdbd9c1578d50d7a9fd74e1f5fc3cf73a0f46cf08b63fe1c8cbbfbfdc4c2ab947c8bc9de93febc896aeca3a83267ad5554a80bd981eb3bbaf6be29c401ffffffffffffffffffdf7fffdffffffffffefeffffffffffffffffffffffffe7fffffffffffffbfffffffffffffffffffffdffbf7fffbfffffffffbfffffffffb41efa9795abfddb166a4d660fae5ca7af3702653112bf180f477693e99869ead8a86a252e9351cf649b0eff14f584a80a144a988a04641cde878990e48d68a5fc5c61d3faea46493a3920744adad1948429cb282650618b290a0294894a4820007a4c0000000000"
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

func TestDecodeAndReadSuccess(t *testing.T) {
	tests := map[string]struct {
		raw    string // hex encoded.
		output cltypes.ObjectSSZ
	}{
		"finalityUpdate": {
			raw:    finalityUpdateResponse,
			output: &cltypes.LightClientFinalityUpdate{},
		},
		"optimisticUpdate": {
			raw:    optimisticUpdateResponse,
			output: &cltypes.LightClientOptimisticUpdate{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			data, err := hex.DecodeString(tc.raw)
			if err != nil {
				t.Errorf("unable to decode string into byte slice: %v", err)
			}
			r := bytes.NewReader(data)
			require.NoError(t, DecodeAndRead(r, tc.output))
		})
	}
}
