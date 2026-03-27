// Copyright 2026 The Erigon Authors
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

package handlers

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/peers"
)

// TestPingRateLimit verifies end-to-end that the per-peer rate limiter rejects
// excess ping requests over real libp2p streams. Ping has burst=2, so the 3rd
// request is rate-limited. The 4th verifies the 30s punishment period is active.
func TestPingRateLimit(t *testing.T) {
	ctx := context.Background()

	server, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { server.Close() })

	client, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	err = client.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()})
	require.NoError(t, err)

	peersPool := peers.NewPool(server)
	beaconDB, indiciesDB := setupStore(t)
	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)

	c := NewConsensusHandlers(
		ctx, beaconDB, indiciesDB, server, peersPool,
		&clparams.NetworkConfig{}, testLocalNode(t), beaconCfg, ethClock,
		nil, &mock_services.ForkChoiceStorageMock{}, nil, nil, nil, true,
	)
	c.Start()

	sendPing := func() byte {
		stream, err := client.NewStream(ctx, server.ID(), protocol.ID(communication.PingProtocolV1))
		require.NoError(t, err)
		defer func() {
			_, _ = io.ReadAll(stream)
			stream.Close()
		}()
		stream.SetDeadline(time.Now().Add(5 * time.Second))

		_, err = stream.Write(nil)
		require.NoError(t, err)

		firstByte := make([]byte, 1)
		_, err = stream.Read(firstByte)
		require.NoError(t, err)
		return firstByte[0]
	}

	// Ping burst = 2. First 2 should succeed.
	require.Equal(t, byte(SuccessfulResponsePrefix), sendPing(), "ping 1 should succeed")
	require.Equal(t, byte(SuccessfulResponsePrefix), sendPing(), "ping 2 should succeed")
	// 3rd ping exhausts the bucket and triggers punishment.
	require.Equal(t, byte(InvalidRequestPrefix), sendPing(), "ping 3 should be rate-limited")
	// 4th ping: still rejected (30s punishment in effect).
	require.Equal(t, byte(InvalidRequestPrefix), sendPing(), "ping 4 should be rejected (punishment)")
}

// TestBlocksByRangeRateLimit verifies end-to-end that the per-item rate limiter
// rejects blocks-by-range requests that exceed the token bucket over real libp2p
// streams. The block protocol has burst=128, counted per response item.
// First request for 96 blocks costs 96 tokens (32 left); second request for 96
// blocks passes the 1-token admission but fails the 95-token batch check.
func TestBlocksByRangeRateLimit(t *testing.T) {
	ctx := context.Background()

	server, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { server.Close() })

	client, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	err = client.Connect(ctx, peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()})
	require.NoError(t, err)

	peersPool := peers.NewPool(server)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	populateDatabaseWithBlocks(t, store, tx, 100, 96)
	require.NoError(t, tx.Commit())

	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)

	c := NewConsensusHandlers(
		ctx, store, indiciesDB, server, peersPool,
		&clparams.NetworkConfig{}, nil, beaconCfg, ethClock,
		nil, &mock_services.ForkChoiceStorageMock{}, nil, nil, nil, true,
	)
	c.Start()

	sendBlocksByRange := func(start, count uint64) byte {
		req := &cltypes.BeaconBlocksByRangeRequest{
			StartSlot: start,
			Count:     count,
			Step:      1,
		}
		var reqBuf bytes.Buffer
		err := ssz_snappy.EncodeAndWrite(&reqBuf, req)
		require.NoError(t, err)

		stream, err := client.NewStream(ctx, server.ID(),
			protocol.ID(communication.BeaconBlocksByRangeProtocolV2))
		require.NoError(t, err)
		defer func() {
			_, _ = io.ReadAll(stream)
			stream.Close()
		}()
		stream.SetDeadline(time.Now().Add(5 * time.Second))

		_, err = stream.Write(reqBuf.Bytes())
		require.NoError(t, err)

		firstByte := make([]byte, 1)
		_, err = stream.Read(firstByte)
		require.NoError(t, err)
		return firstByte[0]
	}

	// First request: 96 blocks → 96 tokens consumed (1 admission + 95 batch), 32 left.
	resp := sendBlocksByRange(100, 96)
	require.Equal(t, byte(SuccessfulResponsePrefix), resp,
		"first blocks-by-range (96 blocks) should succeed")

	// Second request: 96 blocks → admission passes (1 token, 31 left),
	// batch cost fails (needs 95, only 31 available) → rate-limited.
	resp = sendBlocksByRange(100, 96)
	require.Equal(t, byte(InvalidRequestPrefix), resp,
		"second blocks-by-range should be rate-limited (per-item cost)")
}
