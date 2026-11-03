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

// One block: a CREATE2 deploys X, whose constructor destroys X while it holds
// less than 2 wei. Another contract's SELFDESTRUCT sends 3 wei to X, and the
// same CREATE2 deploys X again, now with code. The last tx reads that code
// with EXTCODECOPY and runs it with DELEGATECALL, so it must store the code
// and 0x42. The re-deploying tx hashes for a while first, so the reader
// usually runs before it.
func TestCodeReadAfterRecreateOverDestructCredit(t *testing.T) {
	runtime := program.New().Sstore(2, 0x42).Op(vm.STOP).Bytes()
	beneficiary := common.HexToAddress("0xbeef")
	initCode := func(skip uint64) *program.Program {
		p := program.New().Push(2).Op(vm.SELFBALANCE, vm.LT, vm.ISZERO, vm.PUSH1).Append([]byte{byte(skip)}).Op(vm.JUMPI)
		return p.Selfdestruct(beneficiary)
	}
	_, deploy := initCode(0).Jumpdest()
	init, _ := initCode(deploy).Jumpdest()
	init.ReturnViaCodeCopy(runtime)

	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(init.Bytes())))
	donor := common.HexToAddress("0xd040")
	slowFactory := common.HexToAddress("0x5107")
	reader := common.HexToAddress("0x9e0b")

	factoryCode := program.New().Mstore(init.Bytes(), 0).Push(0).Push(len(init.Bytes())).Push(0).Op(vm.CALLVALUE, vm.CREATE2, vm.STOP).Bytes()
	slow := program.New().Push(3000)
	_, loop := slow.Jumpdest()
	slow.Push(1024).Push(0).Op(vm.KECCAK256, vm.POP).Push(1).Op(vm.SWAP1, vm.SUB, vm.DUP1).Push(loop).Op(vm.JUMPI, vm.POP)
	slow.Call(nil, factory, 0, 0, 0, 0, 0).Op(vm.STOP)
	readerCode := program.New().Push(beneficiary).Op(vm.BALANCE).Push(0).Op(vm.SSTORE)
	readerCode.ExtcodeCopy(x, 0, 0, 32).Push(0).Op(vm.MLOAD).Push(1).Op(vm.SSTORE)
	readerCode.DelegateCall(nil, x, 0, 0, 0, 0).Op(vm.STOP)

	shanghai := chain.TestChainOsakaConfig.Copy()
	shanghai.CancunTime, shanghai.PragueTime, shanghai.OsakaTime = nil, nil, nil
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{{"shanghai", shanghai}, {"osaka", chain.TestChainOsakaConfig}} {
		t.Run(fork.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				factory:     {Code: factoryCode},
				donor:       {Code: program.New().Selfdestruct(x).Bytes(), Balance: big.NewInt(3)},
				slowFactory: {Code: slow.Bytes()},
				reader:      {Code: readerCode.Bytes()},
			}
			keys := revivalKeys(t, alloc, 4)
			gspec := &types.Genesis{Config: fork.config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
				{to: factory, value: 1},
				{to: donor},
				{to: slowFactory},
				{to: reader},
			})
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				code, err := st.GetCode(accounts.InternAddress(x))
				require.NoError(t, err)
				require.Equal(t, runtime, code)
				var word [32]byte
				copy(word[:], runtime)
				copied, err := st.GetState(accounts.InternAddress(reader), accounts.InternKey(common.BytesToHash([]byte{1})))
				require.NoError(t, err)
				require.Equal(t, *new(uint256.Int).SetBytes(word[:]), copied)
				stored, err := st.GetState(accounts.InternAddress(reader), accounts.InternKey(common.BytesToHash([]byte{2})))
				require.NoError(t, err)
				require.Equal(t, *uint256.NewInt(0x42), stored)
			})
		})
	}
}

// Before Cancun, one block: a CREATE2 deploys X, the next tx makes X destroy
// itself, a CALL sends 3 wei to X, and the same CREATE2 deploys X again, now
// with other code. The last tx reads X's code size and code and runs it.
func TestCodeReadAfterRecreateOverCallRevival(t *testing.T) {
	oldRuntime := program.New().Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
	runtime := program.New().Sstore(2, 0x42).Op(vm.STOP).Bytes()
	initCode := func(skip uint64) *program.Program {
		p := program.New().Op(vm.SELFBALANCE, vm.PUSH1).Append([]byte{byte(skip)}).Op(vm.JUMPI)
		return p.ReturnViaCodeCopy(oldRuntime)
	}
	_, deploy := initCode(0).Jumpdest()
	init, _ := initCode(deploy).Jumpdest()
	init.ReturnViaCodeCopy(runtime)

	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(init.Bytes())))
	payer := common.HexToAddress("0x9a7e")
	slowFactory := common.HexToAddress("0x5107")
	reader := common.HexToAddress("0x9e0b")

	factoryCode := program.New().Mstore(init.Bytes(), 0).Push(0).Push(len(init.Bytes())).Push(0).Op(vm.CALLVALUE, vm.CREATE2, vm.STOP).Bytes()
	slow := program.New().Push(3000)
	_, loop := slow.Jumpdest()
	slow.Push(1024).Push(0).Op(vm.KECCAK256, vm.POP).Push(1).Op(vm.SWAP1, vm.SUB, vm.DUP1).Push(loop).Op(vm.JUMPI, vm.POP)
	slow.Call(nil, factory, 0, 0, 0, 0, 0).Op(vm.STOP)
	readerCode := program.New().Push(payer).Op(vm.BALANCE).Push(0).Op(vm.SSTORE)
	readerCode.Push(x).Op(vm.EXTCODESIZE).Push(3).Op(vm.SSTORE)
	readerCode.ExtcodeCopy(x, 0, 0, 32).Push(0).Op(vm.MLOAD).Push(1).Op(vm.SSTORE)
	readerCode.DelegateCall(nil, x, 0, 0, 0, 0).Op(vm.STOP)

	shanghai := chain.TestChainOsakaConfig.Copy()
	shanghai.CancunTime, shanghai.PragueTime, shanghai.OsakaTime = nil, nil, nil
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{{"berlin", chain.TestChainBerlinConfig}, {"shanghai", shanghai}} {
		t.Run(fork.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				factory:     {Code: factoryCode},
				payer:       {Code: program.New().Call(nil, x, 3, 0, 0, 0, 0).Op(vm.STOP).Bytes(), Balance: big.NewInt(3)},
				slowFactory: {Code: slow.Bytes()},
				reader:      {Code: readerCode.Bytes()},
			}
			keys := revivalKeys(t, alloc, 5)
			gspec := &types.Genesis{Config: fork.config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
				{to: factory},
				{to: x},
				{to: payer},
				{to: slowFactory},
				{to: reader},
			})
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				code, err := st.GetCode(accounts.InternAddress(x))
				require.NoError(t, err)
				require.Equal(t, runtime, code)
				var word [32]byte
				copy(word[:], runtime)
				for slot, want := range map[byte]uint256.Int{
					1: *new(uint256.Int).SetBytes(word[:]),
					2: *uint256.NewInt(0x42),
					3: *uint256.NewInt(uint64(len(runtime))),
				} {
					got, err := st.GetState(accounts.InternAddress(reader), accounts.InternKey(common.BytesToHash([]byte{slot})))
					require.NoError(t, err)
					require.Equal(t, want, got, "reader slot %d", slot)
				}
			})
		})
	}
}

// Recreate-over-destruct shape read with EXTCODEHASH: a reader that observes X
// after its destruct but before the CREATE2 recreate must still store the
// recreated code's hash, not the empty hash it saw first. Complements the
// EXTCODECOPY/EXTCODESIZE recreate tests above, which do not exercise EXTCODEHASH.
func TestCodeHashReadAfterRecreate(t *testing.T) {
	oldRuntime := program.New().Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
	runtime := program.New().Sstore(2, 0x42).Op(vm.STOP).Bytes()
	wantHash := crypto.Keccak256Hash(runtime)
	initCode := func(skip uint64) *program.Program {
		p := program.New().Op(vm.SELFBALANCE, vm.PUSH1).Append([]byte{byte(skip)}).Op(vm.JUMPI)
		return p.ReturnViaCodeCopy(oldRuntime)
	}
	_, deploy := initCode(0).Jumpdest()
	init, _ := initCode(deploy).Jumpdest()
	init.ReturnViaCodeCopy(runtime)

	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(init.Bytes())))
	payer := common.HexToAddress("0x9a7e")
	slowFactory := common.HexToAddress("0x5107")
	reader := common.HexToAddress("0x9e0b")

	factoryCode := program.New().Mstore(init.Bytes(), 0).Push(0).Push(len(init.Bytes())).Push(0).Op(vm.CALLVALUE, vm.CREATE2, vm.STOP).Bytes()
	slow := program.New().Push(3000)
	_, loop := slow.Jumpdest()
	slow.Push(1024).Push(0).Op(vm.KECCAK256, vm.POP).Push(1).Op(vm.SWAP1, vm.SUB, vm.DUP1).Push(loop).Op(vm.JUMPI, vm.POP)
	slow.Call(nil, factory, 0, 0, 0, 0, 0).Op(vm.STOP)
	readerCode := program.New().Push(x).Op(vm.EXTCODEHASH).Push(1).Op(vm.SSTORE, vm.STOP)

	shanghai := chain.TestChainOsakaConfig.Copy()
	shanghai.CancunTime, shanghai.PragueTime, shanghai.OsakaTime = nil, nil, nil
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{{"berlin", chain.TestChainBerlinConfig}, {"shanghai", shanghai}} {
		t.Run(fork.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				factory:     {Code: factoryCode},
				payer:       {Code: program.New().Call(nil, x, 3, 0, 0, 0, 0).Op(vm.STOP).Bytes(), Balance: big.NewInt(3)},
				slowFactory: {Code: slow.Bytes()},
				reader:      {Code: readerCode.Bytes()},
			}
			keys := revivalKeys(t, alloc, 5)
			gspec := &types.Genesis{Config: fork.config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
				{to: factory},
				{to: x},
				{to: payer},
				{to: slowFactory},
				{to: reader},
			})
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				code, err := st.GetCode(accounts.InternAddress(x))
				require.NoError(t, err)
				require.Equal(t, runtime, code)
				got, err := st.GetState(accounts.InternAddress(reader), accounts.InternKey(common.BytesToHash([]byte{1})))
				require.NoError(t, err)
				require.Equal(t, *new(uint256.Int).SetBytes(wantHash[:]), got, "reader stored code hash")
			})
		})
	}
}
