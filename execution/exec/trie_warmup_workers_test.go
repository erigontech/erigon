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

package exec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrieWarmupWorkersLeaveCoresToTxs(t *testing.T) {
	for _, c := range []struct{ max, txs, want int }{
		{10, 0, 10},
		{10, 3, 7},
		{10, 9, 1},
		{10, 1438, 1},
		{1, 3, 1},
		{0, 3, 0},
	} {
		require.Equal(t, c.want, trieWarmupWorkers(c.max, c.txs), "max=%d txs=%d", c.max, c.txs)
	}
}
