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
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type unavailableBlocksReader struct {
	freezeblocks.BeaconSnapshotReader
	blocksBySlot map[uint64]*cltypes.SignedBeaconBlock
}

func (r *unavailableBlocksReader) ReadBlockBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	if slot == 42 {
		return nil, snapshot_format.ErrExecutionBodyNotFound
	}
	return r.blocksBySlot[slot], nil
}

func (r *unavailableBlocksReader) ReadBlockByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
	return nil, snapshot_format.ErrExecutionBodyNotFound
}

type memoryStream struct {
	network.Stream
	buffer bytes.Buffer
}

type memoryConn struct {
	network.Conn
}

func (s *memoryStream) Conn() network.Conn {
	return &memoryConn{}
}

func (s *memoryStream) Protocol() protocol.ID {
	return protocol.ID(communication.BeaconBlocksByRangeProtocolV2)
}

func (c *memoryConn) RemotePeer() peer.ID {
	return peer.ID("peer")
}

func (s *memoryStream) Read(p []byte) (int, error) {
	return s.buffer.Read(p)
}

func (s *memoryStream) Write(p []byte) (int, error) {
	return s.buffer.Write(p)
}

func TestBlockHandlersOmitUnavailableExecutionBody(t *testing.T) {
	_, db := setupStore(t)
	blocks, _ := makeBlocksByHeadChain(t, 43, 1)
	handlers := &ConsensusHandlers{
		ctx:          t.Context(),
		beaconDB:     &unavailableBlocksReader{blocksBySlot: map[uint64]*cltypes.SignedBeaconBlock{43: blocks[0]}},
		indiciesDB:   db,
		beaconConfig: &clparams.MainnetBeaconConfig,
		ethClock:     getEthClock(t),
		rateLimiter:  newPeerRateLimiter(),
	}

	t.Run("range", func(t *testing.T) {
		stream := &memoryStream{}
		require.NoError(t, ssz_snappy.EncodeAndWrite(&stream.buffer, &cltypes.BeaconBlocksByRangeRequest{StartSlot: 42, Count: 2, Step: 1}))

		require.NoError(t, handlers.beaconBlocksByRangeHandler(stream))
		require.Zero(t, stream.buffer.Len())
	})

	t.Run("root", func(t *testing.T) {
		stream := &memoryStream{}
		request := solid.NewHashList(1)
		request.Append(common.Hash{0x42})
		require.NoError(t, ssz_snappy.EncodeAndWrite(&stream.buffer, request))

		require.NoError(t, handlers.beaconBlocksByRootHandler(stream))
		require.Zero(t, stream.buffer.Len())
	})

	t.Run("head", func(t *testing.T) {
		stream := &memoryStream{}
		require.NoError(t, ssz_snappy.EncodeAndWrite(&stream.buffer, &cltypes.BeaconBlocksByHeadRequest{BeaconRoot: common.Hash{0x42}, Count: 1}))

		require.NoError(t, handlers.beaconBlocksByHeadHandler(stream))
		require.Zero(t, stream.buffer.Len())
	})
}
