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
	"github.com/jinzhu/copier"
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

// A pre-EIP-8246 account destroyed by a same-tx create+SELFDESTRUCT, then funded
// by a later tx, then zero-value touched by a still-later tx must retain the
// funded balance: the touch does not delete a now non-empty account. Exercises
// the parallel destroyed->funded->touched read/validation path against serial.
func TestSelfdestructedThenFundedThenTouched(t *testing.T) {
	for _, mode := range []struct {
		name     string
		parallel bool
		workers  int
	}{
		{"serial", false, 1},
		{"parallel/workers=1", true, 1},
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
			config := new(chain.Config)
			require.NoError(t, copier.CopyWithOption(config, chain.TestChainBerlinConfig, copier.Option{DeepCopy: true}))
			genesis := &types.Genesis{
				Config:   config,
				GasLimit: 30_000_000,
				Alloc:    types.GenesisAlloc{sender: {Balance: big.NewInt(1_000_000_000_000_000_000)}},
			}
			signer := types.LatestSignerForChainID(config.ChainID)
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
			contract := types.CreateAddress(sender, 0)
			gasPrice := uint256.NewInt(10_000_000_000)
			sign := func(txn types.Transaction) types.Transaction {
				signed, err := types.SignTx(txn, *signer, key)
				require.NoError(t, err)
				return signed
			}
			chainPack, err := m.GenerateChain(1, func(_ int, block *blockgen.BlockGen) {
				block.SetCoinbase(common.Address{1})
				// tx0: deploy + SELFDESTRUCT in one tx -> contract is net-absent.
				block.AddTx(sign(types.NewContractCreation(block.TxNonce(sender), uint256.NewInt(0), 2_000_000, gasPrice, []byte{byte(vm.ADDRESS), byte(vm.SELFDESTRUCT)})))
				// tx1: fund the destroyed address.
				block.AddTx(sign(types.NewTransaction(block.TxNonce(sender), contract, uint256.NewInt(7), 300_000, gasPrice, nil)))
				// tx2: zero-value touch of the funded address.
				block.AddTx(sign(types.NewTransaction(block.TxNonce(sender), contract, uint256.NewInt(0), 300_000, gasPrice, nil)))
			})
			require.NoError(t, err)
			for _, receipt := range chainPack.Receipts[0] {
				require.Equal(t, uint64(types.ReceiptStatusSuccessful), receipt.Status)
			}
			require.NoError(t, m.InsertChain(chainPack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				balance, err := st.GetBalance(accounts.InternAddress(contract))
				require.NoError(t, err)
				require.Equal(t, *uint256.NewInt(7), balance)
				return nil
			}))
		})
	}
}
