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

// Before Cancun, one block: a CREATE2 deploys X and sends 1 wei to a marker
// account, a later tx makes X destroy itself, the next one sends 3 wei to X
// (with a CALL, or as another contract's SELFDESTRUCT beneficiary), and the
// last tx reads the marker's balance and X's code size and code, and runs X
// with DELEGATECALL. X has no code then, so the reader must store only the
// marker's balance. The destroying tx hashes for a while first, so the
// reader usually runs before the destruct lands.
func TestCodeReadAcrossDestructAndRevival(t *testing.T) {
	// X runtime: calldata -> SELFDESTRUCT(CALLER), else SSTORE(0x12, 0x99).
	runtime := func(destruct byte) *program.Program {
		return program.New().Op(vm.CALLDATASIZE, vm.PUSH1).Append([]byte{destruct}).Op(vm.JUMPI).Sstore(0x12, 0x99).Op(vm.STOP)
	}
	_, destruct := runtime(0).Jumpdest()
	rt, _ := runtime(byte(destruct)).Jumpdest()
	rt.Op(vm.CALLER, vm.SELFDESTRUCT)
	init := program.New().ReturnViaCodeCopy(rt.Bytes())

	factory := common.HexToAddress("0xfac7")
	marker := common.HexToAddress("0x3a7c")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(init.Bytes())))
	slowDestroyer := common.HexToAddress("0x5107")
	payer := common.HexToAddress("0x9a7e")
	donor := common.HexToAddress("0xd040")
	reader := common.HexToAddress("0x9e0b")

	factoryCode := program.New().Mstore(init.Bytes(), 0).Push(0).Push(len(init.Bytes())).Push(0).Push(0).Op(vm.CREATE2, vm.POP)
	factoryCode.Call(nil, marker, 1, 0, 0, 0, 0).Op(vm.STOP)
	slow := program.New().Push(3000)
	_, loop := slow.Jumpdest()
	slow.Push(1024).Push(0).Op(vm.KECCAK256, vm.POP).Push(1).Op(vm.SWAP1, vm.SUB, vm.DUP1).Push(loop).Op(vm.JUMPI, vm.POP)
	slow.Call(nil, x, 0, 0, 1, 0, 0).Op(vm.STOP)
	readerCode := program.New().Push(marker).Op(vm.BALANCE).Push(0x10).Op(vm.SSTORE)
	readerCode.DelegateCall(nil, x, 0, 0, 0, 0).Op(vm.POP)
	readerCode.Push(x).Op(vm.EXTCODESIZE).Push(0x13).Op(vm.SSTORE)
	readerCode.ExtcodeCopy(x, 0, 0, 32).Push(0).Op(vm.MLOAD).Push(0x11).Op(vm.SSTORE, vm.STOP)

	shanghai := chain.TestChainOsakaConfig.Copy()
	shanghai.CancunTime, shanghai.PragueTime, shanghai.OsakaTime = nil, nil, nil
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{{"berlin", chain.TestChainBerlinConfig}, {"shanghai", shanghai}} {
		for _, reviver := range []struct {
			name string
			addr common.Address
		}{{"call", payer}, {"destruct", donor}} {
			t.Run(fork.name+"/"+reviver.name, func(t *testing.T) {
				alloc := types.GenesisAlloc{
					factory:       {Code: factoryCode.Bytes(), Balance: big.NewInt(1)},
					slowDestroyer: {Code: slow.Bytes()},
					payer:         {Code: program.New().Call(nil, x, 3, 0, 0, 0, 0).Op(vm.STOP).Bytes(), Balance: big.NewInt(3)},
					donor:         {Code: program.New().Selfdestruct(x).Bytes(), Balance: big.NewInt(3)},
					reader:        {Code: readerCode.Bytes()},
				}
				keys := revivalKeys(t, alloc, 4)
				gspec := &types.Genesis{Config: fork.config, GasLimit: 30_000_000, Alloc: alloc}
				chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
					{to: factory},
					{to: slowDestroyer},
					{to: reviver.addr},
					{to: reader},
				})
				importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
					code, err := st.GetCode(accounts.InternAddress(x))
					require.NoError(t, err)
					require.Empty(t, code)
					marked, err := st.GetState(accounts.InternAddress(reader), accounts.InternKey(common.BytesToHash([]byte{0x10})))
					require.NoError(t, err)
					require.Equal(t, *uint256.NewInt(1), marked)
					for _, slot := range []byte{0x11, 0x12, 0x13} {
						v, err := st.GetState(accounts.InternAddress(reader), accounts.InternKey(common.BytesToHash([]byte{slot})))
						require.NoError(t, err)
						require.Equal(t, uint256.Int{}, v, "reader slot %#x", slot)
					}
				})
			})
		}
	}
}
