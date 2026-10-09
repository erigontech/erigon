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

// Before Cancun, a contract stores a slot and self-destructs in one tx, and a
// later tx of the same block re-creates it with the same slot value. A block
// built in one execution mode must be accepted in the other.
func TestRecreateWritesBackSlotOfDestroyingTx(t *testing.T) {
	for _, mode := range []struct {
		name        string
		parallelGen bool
	}{
		{"serial-gen/parallel-import", false},
		{"parallel-gen/serial-import", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			prev := dbg.Exec3Parallel
			t.Cleanup(func() { dbg.Exec3Parallel = prev })
			dbg.Exec3Parallel = mode.parallelGen

			key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			// The constructor and the runtime store the block number in slot 0. Slot 1
			// is written only by the runtime: it is empty only if the destruct wiped it.
			runtime := program.New().Op(vm.NUMBER).Push(0).Op(vm.SSTORE).Sstore(1, 1).Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
			initCode := program.New().Op(vm.NUMBER).Push(0).Op(vm.SSTORE).ReturnViaCodeCopy(runtime).Bytes()
			factory := common.HexToAddress("0xaaaa")
			contract := types.CreateAddress2(factory, [32]byte{31: 1}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))

			gspec := &types.Genesis{Config: chain.TestChainBerlinConfig, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
				sender:   {Balance: big.NewInt(1_000_000_000_000_000_000)},
				factory:  {Code: program.New().Create2(initCode, 1).Op(vm.POP).Bytes()},
				contract: {Code: runtime},
			}}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
			signer := types.LatestSignerForChainID(gspec.Config.ChainID)
			chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
				// Destroy the contract, then re-create it.
				for _, to := range []common.Address{contract, factory} {
					txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), to, uint256.NewInt(0), 500_000, uint256.NewInt(10_000_000_000), nil), *signer, key)
					require.NoError(t, err)
					b.AddTx(txn)
				}
			})
			require.NoError(t, err)

			dbg.Exec3Parallel = !mode.parallelGen
			require.NoError(t, m.InsertChain(chainPack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				addr := accounts.InternAddress(contract)
				slot0, err := st.GetState(addr, accounts.InternKey([32]byte{}))
				require.NoError(t, err)
				require.Equal(t, uint64(1), slot0.Uint64())
				slot1, err := st.GetState(addr, accounts.InternKey([32]byte{31: 1}))
				require.NoError(t, err)
				require.True(t, slot1.IsZero())
				return nil
			}))
		})
	}
}
