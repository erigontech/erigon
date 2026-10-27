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

// X's first incarnation writes slot 5 and is destroyed; the same CREATE2
// deploys X again in the same block. Then one tx stores 7 in slot 5 and a
// later tx increments it, so slot 5 must be 8. The storing tx hashes for a
// while first, so the incrementing tx usually runs before it.
func TestStorageReadAfterRecreate(t *testing.T) {
	// X runtime: no calldata increments slot 5; calldata v stores v in slot 5,
	// or self-destructs when v is 0.
	runtime := func(store, write byte) (*program.Program, byte, byte) {
		p := program.New().Op(vm.CALLDATASIZE, vm.PUSH1).Append([]byte{store}).Op(vm.JUMPI)
		p.Push(1).Push(5).Op(vm.SLOAD, vm.ADD).Push(5).Op(vm.SSTORE, vm.STOP)
		_, s := p.Jumpdest()
		p.Push(0).Op(vm.CALLDATALOAD, vm.DUP1, vm.PUSH1).Append([]byte{write}).Op(vm.JUMPI, vm.CALLER, vm.SELFDESTRUCT)
		_, w := p.Jumpdest()
		p.Push(5).Op(vm.SSTORE, vm.STOP)
		return p, byte(s), byte(w)
	}
	_, store, write := runtime(0, 0)
	rt, _, _ := runtime(store, write)
	code := rt.Bytes()

	// With no value, the constructor writes slot 5 and destroys X in the same tx.
	initCode := func(skip uint64) *program.Program {
		p := program.New().Op(vm.CALLVALUE, vm.PUSH1).Append([]byte{byte(skip)}).Op(vm.JUMPI)
		return p.Sstore(5, 5).Selfdestruct(common.HexToAddress("0xbeef"))
	}
	_, deploy := initCode(0).Jumpdest()
	init, _ := initCode(deploy).Jumpdest()
	init.ReturnViaCodeCopy(code)

	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(init.Bytes())))
	slowStore := common.HexToAddress("0x5107")
	factoryCode := program.New().Mstore(init.Bytes(), 0).Push(0).Push(len(init.Bytes())).Push(0).Op(vm.CALLVALUE, vm.CREATE2, vm.STOP).Bytes()
	slow := program.New().Push(3000)
	_, loop := slow.Jumpdest()
	slow.Push(1024).Push(0).Op(vm.KECCAK256, vm.POP).Push(1).Op(vm.SWAP1, vm.SUB, vm.DUP1).Push(loop).Op(vm.JUMPI, vm.POP)
	slow.Mstore(common.LeftPadBytes([]byte{7}, 32), 0).Call(nil, x, 0, 0, 32, 0, 0).Op(vm.STOP)

	word := func(v byte) []byte { return common.LeftPadBytes([]byte{v}, 32) }
	shanghai := chain.TestChainOsakaConfig.Copy()
	shanghai.CancunTime, shanghai.PragueTime, shanghai.OsakaTime = nil, nil, nil
	for _, tc := range []struct {
		name   string
		config *chain.Config
		calls  []revivalCall
	}{
		// The first incarnation is created and destroyed in one tx.
		{"osaka", chain.TestChainOsakaConfig, []revivalCall{
			{to: factory},
			{to: factory, value: 1},
			{to: slowStore},
			{to: x},
		}},
		// The first incarnation writes the slot in one tx and is destroyed in another.
		{"shanghai", shanghai, []revivalCall{
			{to: factory, value: 1},
			{to: x, data: word(5)},
			{to: x, data: word(0)},
			{to: factory, value: 1},
			{to: slowStore},
			{to: x},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				factory:   {Code: factoryCode},
				slowStore: {Code: slow.Bytes()},
			}
			keys := revivalKeys(t, alloc, len(tc.calls))
			gspec := &types.Genesis{Config: tc.config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, tc.calls)
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				slot, err := st.GetState(accounts.InternAddress(x), accounts.InternKey(common.BytesToHash([]byte{5})))
				require.NoError(t, err)
				require.Equal(t, *uint256.NewInt(8), slot)
			})
		})
	}
}
