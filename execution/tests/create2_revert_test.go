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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// A SELFDESTRUCT credit funds an address without CreateAccount (a value
// transfer would run it and hide the bug), and a later tx sends to it and then
// reverts a CREATE2 at it. A tx after that must still see both credits. Both
// importers must accept the block built by the versioned generator.
func TestRevertedCreate2KeepsCredits(t *testing.T) {
	for _, mode := range []struct {
		name     string
		parallel bool
	}{
		{"serial", false},
		{"parallel", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			const salt = 1
			initCode := program.New().Push(0).Op(vm.DUP1, vm.REVERT).Bytes()
			factory := common.HexToAddress("0xaaaa")
			contract := types.CreateAddress2(factory, [32]byte{31: salt}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
			funder := common.HexToAddress("0xbbbb")
			observer := common.HexToAddress("0xcccc")
			config := chain.TestChainOsakaConfig.Copy()
			config.AmsterdamTime = common.NewUint64(0)
			signer := types.LatestSignerForChainID(config.ChainID)
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
				sender:   {Balance: big.NewInt(1_000_000_000_000_000_000)},
				funder:   {Balance: big.NewInt(1000), Code: program.New().Selfdestruct(contract).Bytes()},
				factory:  {Balance: big.NewInt(5), Code: program.New().Call(nil, contract, 5, 0, 0, 0, 0).Op(vm.POP).Create2(initCode, salt).Bytes()},
				observer: {Code: program.New().Push(contract).Op(vm.BALANCE).Push(0).Op(vm.SSTORE).Bytes()},
			}}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
			chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
				for _, to := range []common.Address{funder, factory, observer} {
					txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), to, uint256.NewInt(0), 1_000_000, uint256.NewInt(10_000_000_000), nil), *signer, key)
					require.NoError(t, err)
					b.AddTx(txn)
				}
			})
			require.NoError(t, err)

			previousParallel := dbg.Exec3Parallel
			dbg.Exec3Parallel = mode.parallel
			t.Cleanup(func() { dbg.Exec3Parallel = previousParallel })
			require.NoError(t, m.InsertChain(chainPack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				seen, err := st.GetState(accounts.InternAddress(observer), accounts.InternKey(common.Hash{}))
				require.NoError(t, err)
				require.Equal(t, uint64(1005), seen.Uint64())
				return nil
			}))
		})
	}
}

// A factory CREATE2s a contract with an endowment onto a prefunded address, and
// a reverting frame then makes the contract SELFDESTRUCT. The contract keeps the
// prefund plus the endowment, and a later tx of the block must see that balance.
func TestRevertedSelfdestructKeepsCreateEndowment(t *testing.T) {
	for _, amsterdam := range []bool{false, true} {
		t.Run(fmt.Sprintf("amsterdam=%v", amsterdam), func(t *testing.T) {
			const salt, prefund, endowment = 1, 7, 1000
			beneficiary := common.HexToAddress("0xbeef")
			runtime := program.New().Selfdestruct(beneficiary).Bytes()
			initCode := program.New().ReturnData(runtime).Bytes()
			factory := common.HexToAddress("0xaaaa")
			contract := types.CreateAddress2(factory, [32]byte{31: salt}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
			reverter := common.HexToAddress("0xbbbb")
			observer := common.HexToAddress("0xcccc")
			config := chain.TestChainOsakaConfig.Copy()
			config.AmsterdamTime = nil
			if amsterdam {
				config.AmsterdamTime = common.NewUint64(0)
			}
			signer := types.LatestSignerForChainID(config.ChainID)
			create2 := program.New().Mstore(initCode, 0).
				Push(salt).Push(len(initCode)).Push(0).Push(endowment).Op(vm.CREATE2).Op(vm.POP)
			alloc := types.GenesisAlloc{
				contract: {Balance: big.NewInt(prefund)},
				factory:  {Balance: big.NewInt(endowment), Code: create2.Call(nil, reverter, 0, 0, 0, 0, 0).Op(vm.POP).Bytes()},
				reverter: {Code: program.New().Call(nil, contract, 0, 0, 0, 0, 0).Op(vm.POP).
					Push(0).Push(0).Op(vm.REVERT).Bytes()},
				observer: {Code: program.New().
					Push(contract).Op(vm.BALANCE).Push(0).Op(vm.SSTORE).
					Push(contract).Op(vm.EXTCODESIZE).Push(1).Op(vm.SSTORE).
					Push(beneficiary).Op(vm.BALANCE).Push(2).Op(vm.SSTORE).
					Bytes()},
			}
			keys := revivalKeys(t, alloc, 1)
			sender := crypto.PubkeyToAddress(keys[0].PublicKey)
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
			gen := func(_ int, b *blockgen.BlockGen) {
				for _, to := range []common.Address{factory, observer} {
					txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), to, uint256.NewInt(0), 1_000_000, uint256.NewInt(10_000_000_000), nil), *signer, keys[0])
					require.NoError(t, err)
					b.AddTx(txn)
				}
			}
			serial, parallels := importSerialThenParallel(t, gspec, keys[0], 1, []int{1, 2, 4}, gen)
			want := storageValues(t, serial, observer, 0, 1, 2)
			require.Equal(t, []uint64{prefund + endowment, uint64(len(runtime)), 0}, want)
			for i, p := range parallels {
				require.Equalf(t, want, storageValues(t, p, observer, 0, 1, 2), "parallel #%d", i)
			}
		})
	}
}
