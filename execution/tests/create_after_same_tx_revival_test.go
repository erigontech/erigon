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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// One block: a CREATE2 with 1 wei deploys X, whose constructor destroys X
// when called with 1 wei. A later tx sends 2 wei to X with a CALL and then
// runs the same CREATE2 with no value, which deploys X with code. The new
// contract keeps the 2 wei it received in the same tx.
func TestCreateAfterSameTxRevival(t *testing.T) {
	runtime := []byte{byte(vm.STOP)}
	initCode := func(skip uint64) *program.Program {
		p := program.New().Push(1).Op(vm.CALLVALUE, vm.EQ, vm.ISZERO, vm.PUSH1).Append([]byte{byte(skip)}).Op(vm.JUMPI)
		return p.Selfdestruct(common.HexToAddress("0xbeef"))
	}
	_, deploy := initCode(0).Jumpdest()
	init, _ := initCode(deploy).Jumpdest()
	init.ReturnViaCodeCopy(runtime)

	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(init.Bytes())))
	fundThenCreate := common.HexToAddress("0xf0c7")
	factoryCode := program.New().Mstore(init.Bytes(), 0).Push(0).Push(len(init.Bytes())).Push(0).Op(vm.CALLVALUE, vm.CREATE2, vm.STOP).Bytes()
	fundThenCreateCode := program.New().Call(nil, x, 2, 0, 0, 0, 0).Op(vm.POP).Call(nil, factory, 0, 0, 0, 0, 0).Op(vm.STOP).Bytes()

	shanghai := chain.TestChainOsakaConfig.Copy()
	shanghai.CancunTime, shanghai.PragueTime, shanghai.OsakaTime = nil, nil, nil
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{{"shanghai", shanghai}, {"osaka", chain.TestChainOsakaConfig}} {
		t.Run(fork.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				factory:        {Code: factoryCode},
				fundThenCreate: {Code: fundThenCreateCode, Balance: big.NewInt(2)},
			}
			keys := revivalKeys(t, alloc, 2)
			gspec := &types.Genesis{Config: fork.config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
				{to: factory, value: 1},
				{to: fundThenCreate},
			})
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				balance, err := st.GetBalance(accounts.InternAddress(x))
				require.NoError(t, err)
				require.Equal(t, *uint256.NewInt(2), balance)
				code, err := st.GetCode(accounts.InternAddress(x))
				require.NoError(t, err)
				require.Equal(t, runtime, code)
			})
		})
	}
}
