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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func forEachFlushingMode(t *testing.T, f func(t *testing.T, parallel bool)) {
	for _, mode := range []struct {
		name     string
		parallel bool
	}{{"synchronous", false}, {"parallel", true}} {
		t.Run(mode.name, func(t *testing.T) { f(t, mode.parallel) })
	}
}

func headBlockHash(t *testing.T, m *execmoduletester.ExecModuleTester) common.Hash {
	t.Helper()
	var h common.Hash
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		h = rawdb.ReadHeadBlockHash(tx)
		return nil
	}))
	return h
}

func canonicalHashAt(t *testing.T, m *execmoduletester.ExecModuleTester, num uint64) common.Hash {
	t.Helper()
	var h common.Hash
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		var err error
		h, err = rawdb.ReadCanonicalHash(tx, num)
		return err
	}))
	return h
}

func updateForkChoice(t *testing.T, m *execmoduletester.ExecModuleTester, head, safe, finalized common.Hash) execmodule.ForkChoiceResult {
	t.Helper()
	result, err := m.ExecModule.UpdateForkChoice(t.Context(), head, safe, finalized)
	require.NoError(t, err)
	m.ExecModule.WaitIdle(t.Context())
	return result
}

func requireValid(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block) {
	t.Helper()
	validation, err := m.ValidateChain(t.Context(), block.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
}

func TestUpdateForkChoiceSideForkUnwind(t *testing.T) {
	for _, tc := range []struct {
		name         string
		invalidFirst bool
	}{{"after_invalid_safe_hash", true}, {"direct", false}} {
		t.Run(tc.name, func(t *testing.T) {
			forEachFlushingMode(t, func(t *testing.T, parallel bool) {
				m := execmoduletester.New(t, execmoduletester.WithParallelStateFlushing(parallel))
				a, err := m.GenerateChainFrom(m.Genesis, 2, func(_ int, b *blockgen.BlockGen) {
					b.SetCoinbase(common.Address{0x81})
				})
				require.NoError(t, err)
				b, err := m.GenerateChainFrom(m.Genesis, 3, func(_ int, blk *blockgen.BlockGen) {
					blk.SetCoinbase(common.Address{0x82})
				})
				require.NoError(t, err)

				_, err = m.InsertBlocks(t.Context(), a.Blocks)
				require.NoError(t, err)
				requireValid(t, m, a.Blocks[1])
				require.Equal(t, execmodule.ExecutionStatusSuccess, updateForkChoice(t, m, a.Blocks[1].Hash(), common.Hash{}, common.Hash{}).Status)

				_, err = m.InsertBlocks(t.Context(), b.Blocks)
				require.NoError(t, err)
				requireValid(t, m, b.Blocks[2])

				if tc.invalidFirst {
					bad := updateForkChoice(t, m, b.Blocks[2].Hash(), a.Blocks[0].Hash(), common.Hash{})
					require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, bad.Status)
					require.Equal(t, a.Blocks[1].Hash(), headBlockHash(t, m))
				}

				requireValid(t, m, b.Blocks[2])
				toB := updateForkChoice(t, m, b.Blocks[2].Hash(), common.Hash{}, m.Genesis.Hash())
				require.Equal(t, execmodule.ExecutionStatusSuccess, toB.Status)
				require.Equal(t, b.Blocks[2].Hash(), toB.LatestValidHash)
				require.Equal(t, b.Blocks[2].Hash(), headBlockHash(t, m))
				require.Equal(t, b.Blocks[0].Hash(), canonicalHashAt(t, m, 1))

				requireValid(t, m, a.Blocks[1])
				toA := updateForkChoice(t, m, a.Blocks[1].Hash(), common.Hash{}, m.Genesis.Hash())
				require.Equal(t, execmodule.ExecutionStatusSuccess, toA.Status)
				require.Equal(t, a.Blocks[1].Hash(), headBlockHash(t, m))
				require.Equal(t, a.Blocks[0].Hash(), canonicalHashAt(t, m, 1))
			})
		})
	}
}
