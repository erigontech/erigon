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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// TestHeadWitnessesRemovalClosure builds each block's witness on a node
// without commitment history, from a pin taken at its parent: the witness
// verifies statelessly, holds every node of the legacy witness, and adds the
// removal closure's.
func TestHeadWitnessesRemovalClosure(t *testing.T) {
	previousAssert := dbg.AssertEnabled
	dbg.AssertEnabled = true
	t.Cleanup(func() { dbg.AssertEnabled = previousAssert })

	m, chain := rpcdaemontest.CreateTestExecModuleNoInsert(t)
	ctx := context.Background()
	closure := NewHeadWitnesses(m.DB, m.BlockReader, m.Engine)
	legacy := &HeadWitnesses{api: NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})}

	added := 0
	for n := range chain.Length() {
		pin, err := closure.Pin(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(n), pin.Number())
		require.NoError(t, m.InsertChain(chain.Slice(n, n+1)))
		hash := chain.Blocks[n].Hash()

		got, err := closure.Build(ctx, pin, hash)
		require.NoError(t, err, "block %d", n+1)
		want, err := legacy.Build(ctx, pin, hash)
		require.NoError(t, err, "block %d", n+1)
		pin.Close()

		nodes := map[string]struct{}{}
		for _, node := range got.State {
			nodes[string(node)] = struct{}{}
		}
		for _, node := range want.State {
			require.Contains(t, nodes, string(node), "block %d: legacy node %x", n+1, []byte(node))
		}
		require.Equal(t, want.Codes, got.Codes)
		require.Equal(t, want.Keys, got.Keys)
		require.Equal(t, want.Headers, got.Headers)
		if len(got.State) > len(want.State) {
			added++
		}
	}
	require.Positive(t, added, "no block's removal closure adds a node")

	pin, err := closure.Pin(ctx)
	require.NoError(t, err)
	defer pin.Close()
	_, err = closure.Build(ctx, pin, chain.Blocks[0].Hash())
	require.ErrorContains(t, err, "not a child of the pinned head")
}
