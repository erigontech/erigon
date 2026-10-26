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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
)

// InsertBlocks sets the module's cached context; only a later forkchoice
// clears it. Between the two it is simply set, and both paths need the
// module semaphore — so SetHead cannot wait for it while holding the
// semaphore (the forkchoice that would clear it could never run), and
// releasing to wait hands the gap straight back to the next block.
//
// On a chain producing every 12s that race is unwinnable. A soak setHead sat
// through 23 forkchoices in five minutes, never caught the window between one
// clearing the context and the next setting it, and the call was refused on a
// node that was healthy and following the chain throughout:
//
//	debug_setHead: execution did not become quiescent within 5m0s
//	  (currentContext is still set)
//
// Here no forkchoice is coming at all, which is the same position with the
// clock removed.
func TestSetHeadAfterInsertWithoutForkchoice(t *testing.T) {
	m, chainPack := bulkInsertChain(t, 12)
	ctxBg := context.Background()

	// Advance the head the normal way: insert, then forkchoice. The
	// forkchoice is what clears the module's cached context.
	first := chainPack.Blocks[:8]
	_, err := m.InsertBlocks(ctxBg, first)
	require.NoError(t, err)
	res, err := m.UpdateForkChoice(ctxBg, first[len(first)-1].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, res.Status)

	// Now insert without a forkchoice behind it: the cached context is set
	// again and nothing is coming to clear it.
	_, err = m.InsertBlocks(ctxBg, chainPack.Blocks[8:])
	require.NoError(t, err)

	// The caller's deadline bounds the wait, so this fails fast rather than
	// spending the full quiescence budget.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := first[len(first)-1].NumberU64() - 2
	require.NoError(t, m.ExecModule.SetHead(ctx, target),
		"SetHead must not block on a forkchoice it is itself preventing from running")
}
