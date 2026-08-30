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

package receipts

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/execution/chain"
)

// unreachableFrozenBlocks fails the test if the frozen block count is read. On a remote
// rpcdaemon that read is a backend call, and every receipt request reaches the predicate.
type unreachableFrozenBlocks struct {
	dbservices.FullBlockReader
	t *testing.T
}

func (r unreachableFrozenBlocks) FrozenBlocks() uint64 {
	r.t.Fatal("FrozenBlocks read for a block whose fork answers on its own")
	return 0
}

func TestPostStateCalculated(t *testing.T) {
	t.Parallel()

	byzantium := uint64(10)
	cfg := &chain.Config{ByzantiumBlock: &byzantium}

	t.Run("above the fork the frozen block count is not read", func(t *testing.T) {
		t.Parallel()
		require.False(t, PostStateCalculated(cfg, byzantium, false, unreachableFrozenBlocks{t: t}))
	})

	t.Run("below the fork commitment history decides without it", func(t *testing.T) {
		t.Parallel()
		require.True(t, PostStateCalculated(cfg, byzantium-1, true, unreachableFrozenBlocks{t: t}))
	})
}
