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
	"crypto/ecdsa"
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
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestExtCodeHashAfterSelfDestruct(t *testing.T) {
	for _, mode := range []struct {
		name     string
		parallel bool
		workers  int
	}{
		{"serial", false, 1},
		{"parallel/workers=1", true, 1},
		{"parallel/workers=2", true, 2},
		{"parallel/workers=4", true, 4},
	} {
		t.Run(mode.name, func(t *testing.T) {
			previousWorkers := ethconfig.Defaults.Sync.ExecWorkerCount
			ethconfig.Defaults.Sync.ExecWorkerCount = mode.workers
			t.Cleanup(func() { ethconfig.Defaults.Sync.ExecWorkerCount = previousWorkers })
			previousParallel := dbg.Exec3Parallel
			dbg.Exec3Parallel = mode.parallel
			t.Cleanup(func() { dbg.Exec3Parallel = previousParallel })

			key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			observerKeys := make([]*ecdsa.PrivateKey, 0, 3)
			for _, hex := range []string{
				"8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a",
				"49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee",
				"ba5734d8f7091719471e7f7ed6b9df170dc70cc661ca05e688601ad984f068b0",
			} {
				k, err := crypto.HexToECDSA(hex)
				require.NoError(t, err)
				observerKeys = append(observerKeys, k)
			}
			contract := types.CreateAddress(sender, 1)
			observer := common.HexToAddress("0xe1000000000000000000000000000000c0de0000")
			// Recursive CREATE consumes gas before storing EXTCODEHASH.
			code := []byte{
				byte(vm.CODESIZE), byte(vm.CODESIZE), byte(vm.PUSH1), 0, byte(vm.CALLVALUE), byte(vm.CODECOPY),
				byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.CREATE), byte(vm.PUSH20),
			}
			code = append(code, contract[:]...)
			code = append(code, byte(vm.EXTCODEHASH), byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP))
			genesis := &types.Genesis{
				Config: &chain.Config{
					ChainID:               uint256.NewInt(1),
					HomesteadBlock:        new(uint64),
					ByzantiumBlock:        new(uint64),
					ConstantinopleBlock:   new(uint64),
					PetersburgBlock:       new(uint64),
					TangerineWhistleBlock: new(uint64),
					SpuriousDragonBlock:   new(uint64),
				},
				GasLimit: 30_000_000,
				Alloc: types.GenesisAlloc{
					sender:   {Balance: big.NewInt(1_000_000_000_000_000_000)},
					observer: {Balance: big.NewInt(0), Code: code},
				},
			}
			signer := types.LatestSignerForChainID(nil)
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
			sign := func(txn types.Transaction, k *ecdsa.PrivateKey) types.Transaction {
				signed, err := types.SignTx(txn, *signer, k)
				require.NoError(t, err)
				return signed
			}
			chainPack, err := m.GenerateChain(1, func(_ int, block *blockgen.BlockGen) {
				block.AddTx(sign(types.NewTransaction(block.TxNonce(sender), contract, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil), key))
				block.AddTx(sign(types.NewContractCreation(block.TxNonce(sender), uint256.NewInt(0), 100_000, uint256.NewInt(1), []byte{byte(vm.CALLER), byte(vm.SELFDESTRUCT)}), key))
				for _, observerKey := range observerKeys {
					block.AddTx(sign(types.NewTransaction(0, observer, uint256.NewInt(0), 9_000_000, uint256.NewInt(0), nil), observerKey))
				}
			})
			require.NoError(t, err)
			for _, receipt := range chainPack.Receipts[0] {
				require.Equal(t, uint64(types.ReceiptStatusSuccessful), receipt.Status)
			}
			require.NoError(t, m.InsertChain(chainPack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				slot, err := st.GetState(accounts.InternAddress(observer), accounts.InternKey(common.Hash{}))
				require.NoError(t, err)
				require.True(t, slot.IsZero(), "EXTCODEHASH of the destroyed account must be zero")
				exists, err := st.Exist(accounts.InternAddress(contract))
				require.NoError(t, err)
				require.False(t, exists)
				return nil
			}))
		})
	}
}
