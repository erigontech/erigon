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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// The chain sends the base fee to a burnt contract B, and the only tx of the
// block destroys B. Serial execution credits the base fee to the destroyed B,
// which is then removed with it, so B is absent after the block.
func TestBurntContractDestroyedByItsTx(t *testing.T) {
	selfdestruct := program.New().Selfdestruct(0xdead).Bytes()
	factory := common.HexToAddress("0xfac7")
	created := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(selfdestruct)))
	deployed := common.HexToAddress("0xb0b0")
	for _, tc := range []struct {
		name   string
		cancun bool
		burnt  common.Address
	}{
		{"pre-cancun deployed contract", false, deployed},
		{"cancun contract created in the tx", true, created},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := chain.TestChainOsakaConfig.Copy()
			if !tc.cancun {
				config.CancunTime, config.PragueTime, config.OsakaTime = nil, nil, nil
			}
			config.BurntContract = map[string]common.Address{"0": tc.burnt}
			alloc := types.GenesisAlloc{
				factory:  {Code: program.New().Create2(selfdestruct, 0).Op(vm.STOP).Bytes()},
				deployed: {Code: selfdestruct},
			}
			keys := revivalKeys(t, alloc, 1)
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
			to := factory
			if tc.burnt == deployed {
				to = deployed
			}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{{to: to}})

			prevParallel := dbg.Exec3Parallel
			t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })
			dbg.Exec3Parallel = false
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
			require.NoError(t, m.InsertChain(chainPack))

			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				exist, err := st.Exist(accounts.InternAddress(tc.burnt))
				require.NoError(t, err)
				require.False(t, exist)
			})
		})
	}
}
