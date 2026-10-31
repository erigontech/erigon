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
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// The node pins the starting block for the whole session, so a client that
// subscribes mid-sync gets where the session began, not where it joined.
func TestSyncingPayloadStartingBlockComesFromTheNode(t *testing.T) {
	payload := syncingPayload(&remoteproto.SyncingReply{Syncing: true, StartingBlock: proto.Uint64(100), CurrentBlock: 150, LastNewBlockSeen: 210})
	result, ok := payload.(syncingResult)
	require.True(t, ok)
	require.EqualValues(t, 100, result.StartingBlock)
	require.EqualValues(t, 150, result.CurrentBlock)
	require.EqualValues(t, 210, result.HighestBlock)
}

func TestSyncingPayloadIsFalseOnceSynced(t *testing.T) {
	payload := syncingPayload(&remoteproto.SyncingReply{Syncing: false, CurrentBlock: 200, LastNewBlockSeen: 200})
	require.Equal(t, false, payload)
}

func TestSyncingPayloadWithoutAPinReportsTheCurrentBlock(t *testing.T) {
	payload := syncingPayload(&remoteproto.SyncingReply{Syncing: true, CurrentBlock: 150, LastNewBlockSeen: 210})
	result, ok := payload.(syncingResult)
	require.True(t, ok)
	require.EqualValues(t, 150, result.StartingBlock)
}
