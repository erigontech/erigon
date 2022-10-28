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
	finalityUpdateResponse = []byte{
		74, 38, 197, 139, 200, 4, 255, 6, 0, 0, 115, 78, 97, 80, 112, 89, 1, 76, 2,
		0, 131, 238, 192, 219, 223, 102, 76, 0, 0, 0, 0, 0, 77, 233, 6, 0, 0, 0, 0,
		0, 23, 129, 190, 23, 200, 93, 242, 66, 120, 229, 160, 140, 44, 235, 186, 178,
		29, 205, 96, 89, 53, 116, 194, 49, 244, 112, 117, 38, 36, 37, 251, 126, 97,
		63, 174, 18, 18, 52, 138, 188, 187, 21, 237, 46, 110, 153, 177, 206, 93, 147,
		200, 1, 62, 182, 96, 9, 216, 36, 114, 31, 10, 6, 141, 177, 190, 29, 85, 121,
		52, 37, 35, 69, 233, 144, 147, 216, 230, 178, 39, 180, 238, 25, 66, 58, 53,
		217, 132, 165, 100, 137, 191, 203, 87, 210, 100, 123, 128, 102, 76, 0, 0, 0,
		0, 0, 155, 72, 2, 0, 0, 0, 0, 0, 60, 104, 181, 81, 96, 7, 53, 193, 121, 171,
		251, 213, 199, 67, 65, 168, 213, 36, 54, 82, 23, 231, 12, 3, 29, 19, 172,
		194, 43, 0, 62, 73, 56, 233, 208, 69, 116, 158, 52, 150, 137, 210, 17, 151,
		45, 202, 108, 151, 146, 163, 100, 243, 9, 181, 194, 2, 242, 93, 135, 180, 91,
		63, 85, 176, 127, 234, 171, 59, 71, 118, 11, 178, 67, 104, 73, 168, 85, 181,
		46, 8, 166, 27, 232, 145, 86, 185, 13, 80, 128, 245, 113, 155, 153, 137, 39,
		75, 52, 99, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 65, 101, 88, 22, 4, 244, 176, 47, 93, 194, 65, 37,
		79, 221, 228, 105, 165, 92, 142, 193, 186, 58, 67, 20, 134, 241, 135, 34,
		122, 125, 115, 57, 172, 122, 65, 185, 199, 105, 189, 49, 177, 241, 177, 210,
		33, 99, 244, 214, 26, 95, 84, 189, 174, 224, 193, 54, 236, 63, 221, 184, 78,
		109, 127, 253, 221, 85, 168, 161, 105, 38, 209, 149, 105, 3, 159, 122, 244,
		254, 178, 255, 91, 77, 120, 187, 125, 142, 51, 120, 0, 141, 50, 174, 6, 219,
		151, 239, 0, 85, 229, 188, 59, 64, 96, 194, 166, 103, 55, 135, 128, 213, 137,
		87, 28, 183, 254, 10, 242, 76, 181, 154, 34, 69, 191, 188, 199, 55, 40, 100,
		95, 23, 207, 224, 0, 161, 38, 165, 131, 206, 255, 211, 250, 35, 98, 251, 173,
		131, 104, 203, 253, 249, 177, 151, 226, 215, 171, 138, 240, 89, 169, 36, 255,
		255, 191, 255, 255, 255, 255, 255, 255, 95, 127, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 223, 255, 255, 254, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 251, 253, 254, 255, 255, 251, 255,
		255, 247, 255, 249, 223, 191, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 129, 125, 109, 155, 212, 25, 158, 44, 114, 150, 201, 26, 87,
		78, 80, 130, 175, 58, 40, 173, 131, 23, 147, 38, 90, 40, 24, 26, 197, 121,
		95, 97, 233, 53, 117, 144, 46, 247, 102, 140, 18, 235, 195, 241, 81, 148,
		154, 224, 23, 5, 47, 142, 181, 239, 122, 137, 144, 31, 160, 122, 11, 36, 129,
		111, 166, 255, 210, 7, 233, 115, 16, 107, 197, 162, 7, 18, 76, 164, 148, 1,
		216, 240, 243, 211, 37, 201, 254, 237, 203, 94, 219, 126, 14, 56, 109, 46,
		224, 102, 76, 0, 0, 0, 0, 0,
	}

	optimisticUpdateResponse = []byte{
		74, 38, 197, 139, 152, 2, 255, 6, 0, 0, 115, 78, 97, 80, 112, 89, 1, 28,
		1, 0, 130, 214, 245, 243, 255, 121, 76, 0, 0, 0, 0, 0, 164, 154, 2, 0,
		0, 0, 0, 0, 154, 118, 141, 16, 20, 35, 142, 238, 52, 184, 18, 172, 56,
		212, 213, 250, 200, 152, 186, 162, 176, 5, 31, 161, 171, 112, 243, 232,
		132, 95, 52, 137, 7, 11, 250, 205, 189, 156, 21, 120, 213, 13, 122, 159,
		215, 78, 31, 95, 195, 207, 115, 160, 244, 108, 240, 139, 99, 254, 28, 140,
		187, 251, 253, 196, 194, 171, 148, 124, 139, 201, 222, 147, 254, 188, 137,
		106, 236, 163, 168, 50, 103, 173, 85, 84, 168, 11, 217, 129, 235, 59, 186,
		246, 190, 41, 196, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 223,
		127, 255, 223, 255, 255, 255, 255, 254, 254, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 231, 255, 255, 255, 255, 255, 255, 251, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 253, 255, 191, 127, 255, 191,
		255, 255, 255, 255, 191, 255, 255, 255, 255, 180, 30, 250, 151, 149, 171,
		253, 219, 22, 106, 77, 102, 15, 174, 92, 167, 175, 55, 2, 101, 49, 18,
		191, 24, 15, 71, 118, 147, 233, 152, 105, 234, 216, 168, 106, 37, 46, 147,
		81, 207, 100, 155, 14, 255, 20, 245, 132, 168, 10, 20, 74, 152, 138, 4,
		100, 28, 222, 135, 137, 144, 228, 141, 104, 165, 252, 92, 97, 211, 250,
		234, 70, 73, 58, 57, 32, 116, 74, 218, 209, 148, 132, 41, 203, 40, 38, 80,
		97, 139, 41, 10, 2, 148, 137, 74, 72, 32, 0, 122, 76, 0, 0, 0, 0, 0,
	}
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
		data   []byte
		output cltypes.ObjectSSZ
	}{
		"finalityUpdate": {
			data:   finalityUpdateResponse,
			output: &cltypes.LightClientFinalityUpdate{},
		},
		"optimisticUpdate": {
			data:   optimisticUpdateResponse,
			output: &cltypes.LightClientOptimisticUpdate{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := bytes.NewReader(tc.data)
			err := DecodeAndRead(r, tc.output)
			require.NoError(t, err)
		})
	}
}
