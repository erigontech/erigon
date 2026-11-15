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

// The first block deletes one of the two genesis accounts with a zero withdrawal, so the
// whole state is one account. The next block adds an account with a withdrawal, and its
// state root must cover both accounts.
func TestSoleAccountRootAfterDeleteCollapse(t *testing.T) {
	survivor := common.HexToAddress("0xaa01")
	deleted := common.HexToAddress("0xaa02")
	newcomer := common.HexToAddress("0xaa03")
	gspec := &types.Genesis{Config: chain.TestChainOsakaConfig, Alloc: types.GenesisAlloc{
		survivor: {Balance: big.NewInt(1)},
		deleted:  {Balance: new(big.Int)},
	}}
	final, _, err := genesiswrite.GenesisToBlock(&types.Genesis{Config: gspec.Config, Alloc: types.GenesisAlloc{
		survivor: {Balance: big.NewInt(1)},
		newcomer: {Balance: big.NewInt(1e9)},
	}}, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)

	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprintf("parallelCommitment=%v", parallel), func(t *testing.T) {
			prev := statecfg.ExperimentalParallelCommitment
			t.Cleanup(func() { statecfg.ExperimentalParallelCommitment = prev })
			statecfg.ExperimentalParallelCommitment = parallel

			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec))
			cp, err := m.GenerateChain(2, func(i int, bg *blockgen.BlockGen) {
				if i == 0 {
					bg.AddWithdrawal(&types.Withdrawal{Index: 0, Address: deleted})
				} else {
					bg.AddWithdrawal(&types.Withdrawal{Index: 1, Address: newcomer, Amount: 1})
				}
			})
			require.NoError(t, err)
			require.Equal(t, final.Root(), cp.Blocks[1].Root())
		})
	}
}
