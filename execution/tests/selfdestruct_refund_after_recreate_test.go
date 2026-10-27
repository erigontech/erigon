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

// Before London, one block: a CREATE2 deploys X, whose constructor writes
// slot 1. The next tx makes X destroy itself, another contract's SELFDESTRUCT
// sends 3 wei to X, and the same CREATE2, now with 1 wei, deploys X again.
// This time the constructor reads slot 1 and destroys X, which earns the
// SELFDESTRUCT refund: X has not destroyed itself before in that tx.
func TestSelfdestructRefundAfterRecreate(t *testing.T) {
	runtime := program.New().Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
	beneficiary := common.HexToAddress("0xbeef")
	// The constructor stores SLOAD(1)+1 in slot 2 and 7 in slot 1, then
	// destroys X when called with 1 wei and deploys runtime otherwise.
	initCode := func(skip uint64) *program.Program {
		p := program.New().Push(1).Op(vm.SLOAD).Push(1).Op(vm.ADD).Push(2).Op(vm.SSTORE).Sstore(1, 7)
		p.Push(1).Op(vm.CALLVALUE, vm.EQ, vm.ISZERO, vm.PUSH1).Append([]byte{byte(skip)}).Op(vm.JUMPI)
		return p.Selfdestruct(beneficiary)
	}
	_, deploy := initCode(0).Jumpdest()
	init, _ := initCode(deploy).Jumpdest()
	init.ReturnViaCodeCopy(runtime)

	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(init.Bytes())))
	donor := common.HexToAddress("0xd040")
	factoryCode := program.New().Mstore(init.Bytes(), 0).Push(0).Push(len(init.Bytes())).Push(0).Op(vm.CALLVALUE, vm.CREATE2, vm.STOP).Bytes()

	petersburg := chain.TestChainBerlinConfig.Copy()
	petersburg.IstanbulBlock, petersburg.MuirGlacierBlock, petersburg.BerlinBlock = nil, nil, nil
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{{"petersburg", petersburg}, {"berlin", chain.TestChainBerlinConfig}} {
		t.Run(fork.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				factory: {Code: factoryCode},
				donor:   {Code: program.New().Selfdestruct(x).Bytes(), Balance: big.NewInt(3)},
			}
			keys := revivalKeys(t, alloc, 4)
			gspec := &types.Genesis{Config: fork.config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
				{to: factory},
				{to: x},
				{to: donor},
				{to: factory, value: 1},
			})
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				exists, err := st.Exist(accounts.InternAddress(x))
				require.NoError(t, err)
				require.False(t, exists)
			})
		})
	}
}
