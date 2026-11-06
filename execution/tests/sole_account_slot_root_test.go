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

package executiontests

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// A zero withdrawal deletes an empty genesis account, so the whole state is the other
// account, which has one storage slot. The block's state root must cover that slot.
func TestSoleAccountWithOneSlotRootAfterDeleteCollapse(t *testing.T) {
	survivor := common.HexToAddress("0xaa01")
	deleted := common.HexToAddress("0xaa02")
	survivorAccount := types.GenesisAccount{
		Balance: big.NewInt(1),
		Storage: map[common.Hash]common.Hash{common.HexToHash("0x01"): common.HexToHash("0x05")},
	}
	gspec := &types.Genesis{Config: chain.TestChainOsakaConfig, Alloc: types.GenesisAlloc{
		survivor: survivorAccount,
		deleted:  {Balance: new(big.Int)},
	}}
	final, _, err := genesiswrite.GenesisToBlock(&types.Genesis{Config: gspec.Config, Alloc: types.GenesisAlloc{
		survivor: survivorAccount,
	}}, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)

	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprintf("parallelCommitment=%v", parallel), func(t *testing.T) {
			prev := statecfg.ExperimentalParallelCommitment
			t.Cleanup(func() { statecfg.ExperimentalParallelCommitment = prev })
			statecfg.ExperimentalParallelCommitment = parallel

			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec))
			cp, err := m.GenerateChain(1, func(_ int, bg *blockgen.BlockGen) {
				bg.AddWithdrawal(&types.Withdrawal{Address: deleted})
			})
			require.NoError(t, err)
			require.Equal(t, final.Root(), cp.Blocks[0].Root())
			require.NoError(t, m.InsertChain(cp))
		})
	}
}
