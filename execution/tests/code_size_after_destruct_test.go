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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// Before Cancun, one block: the first tx hashes for a long time, the second
// makes an existing contract X destroy itself, and the third stores
// EXTCODESIZE(X), which must be 0, and then hashes for a while. The third tx
// reads X before the destruct lands and finishes after it.
func TestCodeSizeAfterDestructOfExistingContract(t *testing.T) {
	hash := func(p *program.Program, n int) *program.Program {
		p.Push(n)
		_, loop := p.Jumpdest()
		return p.Push(1024).Push(0).Op(vm.KECCAK256, vm.POP).Push(1).Op(vm.SWAP1, vm.SUB, vm.DUP1).Push(loop).Op(vm.JUMPI, vm.POP)
	}
	x := common.HexToAddress("0x7777")
	slow := common.HexToAddress("0x5101")
	destroyer := common.HexToAddress("0x5102")
	reader := common.HexToAddress("0x5103")

	shanghai := chain.TestChainOsakaConfig.Copy()
	shanghai.CancunTime, shanghai.PragueTime, shanghai.OsakaTime = nil, nil, nil
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{{"berlin", chain.TestChainBerlinConfig}, {"shanghai", shanghai}} {
		t.Run(fork.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				x:         {Code: program.New().Op(vm.CALLER, vm.SELFDESTRUCT).Bytes(), Balance: big.NewInt(5), Nonce: 1},
				slow:      {Code: hash(program.New(), 3000).Op(vm.STOP).Bytes()},
				destroyer: {Code: hash(program.New(), 200).Call(nil, x, 0, 0, 0, 0, 0).Op(vm.STOP).Bytes()},
				reader:    {Code: hash(program.New().Push(x).Op(vm.EXTCODESIZE).Push(1).Op(vm.SSTORE), 1500).Op(vm.STOP).Bytes()},
			}
			keys := revivalKeys(t, alloc, 3)
			gspec := &types.Genesis{Config: fork.config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
				{to: slow},
				{to: destroyer},
				{to: reader},
			})
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				size, err := st.GetState(accounts.InternAddress(reader), accounts.InternKey(common.BytesToHash([]byte{1})))
				require.NoError(t, err)
				require.True(t, size.IsZero(), "EXTCODESIZE of the destroyed contract: %d", size.Uint64())
			})
		})
	}
}
