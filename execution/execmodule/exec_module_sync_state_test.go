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

package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// A forkchoice update whose target is beyond the reorg range must notify
// syncing=true before the catch-up executes, and synced (false) once done.
func TestUpdateForkChoiceNotifiesSyncStateWhenBehind(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: chain.AllProtocolChanges}))

	const chainLen = 12 // beyond the reorg range (8) used by the syncing heuristic
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainLen, func(i int, b *blockgen.BlockGen) {})
	require.NoError(t, err)

	insRes, err := insertBlocks(ctx, m.ExecModule, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, insRes)

	tip := chainPack.Blocks[chainLen-1].Header()
	vr, err := validateChain(ctx, m.ExecModule, tip)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus)

	ch, unsubscribe := m.Notifications.Events.AddSyncStateSubscription()
	defer unsubscribe()

	ur, err := updateForkChoice(ctx, m.ExecModule, tip)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status)
	m.ExecModule.WaitIdle(ctx)

	var got []*remoteproto.SyncingReply
drain:
	for {
		select {
		case reply := <-ch:
			got = append(got, reply)
		default:
			break drain
		}
	}

	require.NotEmpty(t, got, "the forkchoice update must notify the sync state")
	require.True(t, got[0].Syncing, "a node behind the FCU target must notify syncing=true first")
	require.Equal(t, uint64(chainLen), got[0].LastNewBlockSeen)
	last := got[len(got)-1]
	require.False(t, last.Syncing, "the catch-up must end with a synced notification")
	require.Equal(t, uint64(chainLen), last.CurrentBlock)
}
