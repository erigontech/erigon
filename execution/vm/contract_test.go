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

package vm

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestJumpdestAnalysisCachedByCodeHash(t *testing.T) {
	code := []byte{byte(PUSH1), byte(JUMPDEST), byte(JUMPDEST)}
	hash := common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9}
	c := &Contract{Code: code, CodeHash: accounts.InternCodeHash(hash)}
	bits := c.jumpdestAnalysis()
	require.False(t, bits.isJumpdest(1), "push data")
	require.True(t, bits.isJumpdest(2))
	require.Same(t, &bits[0], &c.jumpdestAnalysis()[0], "the second analysis was recomputed")

	// A code hash that shares the slot replaces the entry, and the old one is recomputed.
	other := hash
	other[31] = 1
	o := &Contract{Code: []byte{byte(JUMPDEST)}, CodeHash: accounts.InternCodeHash(other)}
	require.True(t, o.jumpdestAnalysis().isJumpdest(0))
	require.NotSame(t, &bits[0], &c.jumpdestAnalysis()[0])
}
