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

package jsonrpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func TestSyncingPayloadStartingBlockPinnedForSession(t *testing.T) {
	var b syncingPayloadBuilder

	first := b.build(&remoteproto.SyncingReply{Syncing: true, CurrentBlock: 100, LastNewBlockSeen: 200})
	result, ok := first.(syncingResult)
	require.True(t, ok)
	require.EqualValues(t, 100, result.StartingBlock)

	second := b.build(&remoteproto.SyncingReply{Syncing: true, CurrentBlock: 150, LastNewBlockSeen: 210})
	result, ok = second.(syncingResult)
	require.True(t, ok)
	require.EqualValues(t, 100, result.StartingBlock, "startingBlock must not move while the sync session lasts")
	require.EqualValues(t, 150, result.CurrentBlock)
	require.EqualValues(t, 210, result.HighestBlock)
}

func TestSyncingPayloadIsFalseOnceSynced(t *testing.T) {
	var b syncingPayloadBuilder
	b.build(&remoteproto.SyncingReply{Syncing: true, CurrentBlock: 100, LastNewBlockSeen: 200})

	payload := b.build(&remoteproto.SyncingReply{Syncing: false, CurrentBlock: 200, LastNewBlockSeen: 200})
	require.Equal(t, false, payload)
}

func TestSyncingPayloadNewSessionRecapturesStartingBlock(t *testing.T) {
	var b syncingPayloadBuilder
	b.build(&remoteproto.SyncingReply{Syncing: true, CurrentBlock: 100, LastNewBlockSeen: 200})
	b.build(&remoteproto.SyncingReply{Syncing: false, CurrentBlock: 200, LastNewBlockSeen: 200})

	payload := b.build(&remoteproto.SyncingReply{Syncing: true, CurrentBlock: 300, LastNewBlockSeen: 500})
	result, ok := payload.(syncingResult)
	require.True(t, ok)
	require.EqualValues(t, 300, result.StartingBlock, "a new sync session must capture a fresh startingBlock")
}

func TestSyncingPayloadFirstObservationAlreadySynced(t *testing.T) {
	var b syncingPayloadBuilder
	payload := b.build(&remoteproto.SyncingReply{Syncing: false, CurrentBlock: 200, LastNewBlockSeen: 200})
	require.Equal(t, false, payload)
}
