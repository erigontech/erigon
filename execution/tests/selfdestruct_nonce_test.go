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

// A contract created in the tx CREATEs a child and self-destructs, and is then
// called again: a CREATE in a reverted frame must not reset its nonce. Parallel
// execution must accept the block built by GenerateChain.
func TestSelfDestructedContractCreatesAfterRevert(t *testing.T) {
	for _, eip8246 := range []bool{false, true} {
		t.Run(fmt.Sprintf("eip8246=%v", eip8246), func(t *testing.T) {
			prev := dbg.Exec3Parallel
			t.Cleanup(func() { dbg.Exec3Parallel = prev })
			dbg.Exec3Parallel = false

			key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			// CREATE an empty child, then SELFDESTRUCT for calldata 0x00, else REVERT.
			runtime := []byte{
				byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.CREATE), byte(vm.POP),
				byte(vm.PUSH1), 0, byte(vm.CALLDATALOAD), byte(vm.PUSH1), 248, byte(vm.SHR),
				byte(vm.PUSH1), 19, byte(vm.JUMPI),
				byte(vm.ADDRESS), byte(vm.SELFDESTRUCT),
				byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.REVERT),
			}
			initCode := program.New().ReturnViaCodeCopy(runtime).Bytes()
			factory := common.HexToAddress("0xaaaa")
			contract := types.CreateAddress2(factory, [32]byte{31: 1}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
			factoryCode := program.New().Create2(initCode, 1).Op(vm.POP)
			for _, flag := range []int{0, 1, 0} {
				factoryCode.Push(flag).Push(0).Op(vm.MSTORE8).Call(nil, contract, 0, 0, 1, 0, 0).Op(vm.POP)
			}

			config := chain.TestChainOsakaConfig.Copy()
			if eip8246 {
				config.AmsterdamTime = common.NewUint64(0)
			}
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
				sender:  {Balance: big.NewInt(1_000_000_000_000_000_000)},
				factory: {Code: factoryCode.Bytes()},
			}}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
			signer := types.LatestSignerForChainID(config.ChainID)
			chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
				txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), factory, uint256.NewInt(0), 3_000_000, uint256.NewInt(10_000_000_000), nil), *signer, key)
				require.NoError(t, err)
				b.AddTx(txn)
			})
			require.NoError(t, err)

			dbg.Exec3Parallel = true
			require.NoError(t, m.InsertChain(chainPack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				for nonce, want := range []bool{false, true, true} {
					exist, err := st.Exist(accounts.InternAddress(types.CreateAddress(contract, uint64(nonce))))
					require.NoError(t, err)
					require.Equal(t, want, exist, "child at nonce %d", nonce)
				}
				return nil
			}))
		})
	}
}
