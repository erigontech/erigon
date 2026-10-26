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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// One block: the first tx hashes for a while and then deploys X with CREATE2.
// The second tx runs the same CREATE2 twice and stores each result in slot 1,
// which stays 0 because X already exists. A run of the second tx that does not
// see X yet stores X's address and then 0, a net-zero write, and the run after
// it writes nothing.
func TestDroppedNetZeroWrite(t *testing.T) {
	init := program.New().ReturnViaCodeCopy([]byte{byte(vm.STOP)})
	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(init.Bytes())))
	slowFactory := common.HexToAddress("0x5107")
	twice := common.HexToAddress("0x7a1c")

	// The factory stores the CREATE2 result in the slot given by the calldata.
	factoryCode := program.New().Mstore(init.Bytes(), 0).Push(0).Push(len(init.Bytes())).Push(0).Push(0).Op(vm.CREATE2)
	factoryCode.Push(0).Op(vm.CALLDATALOAD, vm.SSTORE, vm.STOP)
	slow := program.New().Push(1000)
	_, loop := slow.Jumpdest()
	slow.Push(1024).Push(0).Op(vm.KECCAK256, vm.POP).Push(1).Op(vm.SWAP1, vm.SUB, vm.DUP1).Push(loop).Op(vm.JUMPI, vm.POP)
	slow.Mstore(common.LeftPadBytes([]byte{0x10}, 32), 0).Call(nil, factory, 0, 0, 32, 0, 0).Op(vm.STOP)
	twiceCode := program.New().Mstore(common.LeftPadBytes([]byte{1}, 32), 0)
	twiceCode.Call(nil, factory, 0, 0, 32, 0, 0).Op(vm.POP).Call(nil, factory, 0, 0, 32, 0, 0).Op(vm.STOP)

	amsterdam := chain.TestChainOsakaConfig.Copy()
	amsterdam.AmsterdamTime = common.NewUint64(0)
	alloc := types.GenesisAlloc{
		factory:     {Code: factoryCode.Bytes()},
		slowFactory: {Code: slow.Bytes()},
		twice:       {Code: twiceCode.Bytes()},
	}
	keys := revivalKeys(t, alloc, 2)
	gspec := &types.Genesis{Config: amsterdam, GasLimit: 30_000_000, Alloc: alloc}
	chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
		{to: slowFactory},
		{to: twice},
	})
	importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
		slot, err := st.GetState(accounts.InternAddress(factory), accounts.InternKey(common.BytesToHash([]byte{1})))
		require.NoError(t, err)
		require.True(t, slot.IsZero())
		code, err := st.GetCode(accounts.InternAddress(x))
		require.NoError(t, err)
		require.Equal(t, []byte{byte(vm.STOP)}, code)
	})
}
