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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestCommittedContractSelfdestructThenCredited pins, per fork, the account a
// pre-block (committed) contract leaves behind when it SELFDESTRUCTs to itself in
// one tx and a later tx credits the address. The pre-block contract is never
// same-tx created, so EIP-6780 decides whether the SELFDESTRUCT deletes it:
//
//   - pre-EIP-6780: the SELFDESTRUCT deletes the contract (code + nonce gone), so
//     the later credit recreates it as a plain EOA (nonce 0, no code).
//   - EIP-6780 onward: a not-same-tx-created contract is not deleted, so its code
//     and nonce survive and the credit lands on the preserved contract.
//
// It is the reachability proof for the version-map reader unification: the
// committed-base "balance-preserving SELFDESTRUCT then credited" cell shape (on
// which the live read path and the one finalized interpreter disagree on
// nonce/code) can only arise if the write side wipes a committed contract's
// nonce/code cross-tx. EIP-6780 forbids that, so the shape is unreachable — which
// this test holds true under real execution across the fork ladder.
func TestCommittedContractSelfdestructThenCredited(t *testing.T) {
	// forks spanning the three SELFDESTRUCT eras the account outcome depends on.
	forkNames := []string{
		"SpuriousDragon", "Byzantium", "Istanbul", "Berlin", "London",
		"Shanghai", "Cancun", "Prague", "Osaka", "Amsterdam",
	}
	contractCode := []byte{byte(vm.ADDRESS), byte(vm.SELFDESTRUCT)}
	emptyCodeHash := accounts.EmptyCodeHash

	for _, forkName := range forkNames {
		cfg, ok := testforks.Forks[forkName]
		require.Truef(t, ok, "unknown fork %s", forkName)
		eip6780 := cfg.IsCancun(0)

		for _, mode := range []struct {
			name    string
			workers int
		}{
			{"workers=1", 1},
			{"workers=4", 4},
		} {
			t.Run(forkName+"/"+mode.name, func(t *testing.T) {
				previousWorkers := ethconfig.Defaults.Sync.ExecWorkerCount
				ethconfig.Defaults.Sync.ExecWorkerCount = mode.workers
				t.Cleanup(func() { ethconfig.Defaults.Sync.ExecWorkerCount = previousWorkers })

				config := cfg.Copy()
				key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
				require.NoError(t, err)
				sender := crypto.PubkeyToAddress(key.PublicKey)
				contract := common.HexToAddress("0x00000000000000000000000000000000c0ffee01")
				genesis := &types.Genesis{
					Config:   config,
					GasLimit: 30_000_000,
					Alloc: types.GenesisAlloc{
						sender:   {Balance: big.NewInt(1_000_000_000_000_000_000)},
						contract: {Balance: big.NewInt(100), Nonce: 1, Code: contractCode},
					},
				}
				signer := types.LatestSignerForChainID(config.ChainID)
				m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
				gasPrice := uint256.NewInt(10_000_000_000)
				sign := func(txn types.Transaction) types.Transaction {
					signed, err := types.SignTx(txn, *signer, key)
					require.NoError(t, err)
					return signed
				}
				chainPack, err := m.GenerateChain(1, func(_ int, block *blockgen.BlockGen) {
					block.SetCoinbase(common.Address{1})
					// tx0: call the committed contract; its code SELFDESTRUCTs to itself.
					block.AddTx(sign(types.NewTransaction(block.TxNonce(sender), contract, uint256.NewInt(0), 300_000, gasPrice, nil)))
					// tx1: credit the self-destructed address.
					block.AddTx(sign(types.NewTransaction(block.TxNonce(sender), contract, uint256.NewInt(7), 300_000, gasPrice, nil)))
				})
				require.NoError(t, err)
				for _, receipt := range chainPack.Receipts[0] {
					require.Equal(t, uint64(types.ReceiptStatusSuccessful), receipt.Status)
				}
				require.NoError(t, m.InsertChain(chainPack))
				require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
					st := state.New(m.NewStateReader(tx))
					defer st.Close()
					addr := accounts.InternAddress(contract)
					exists, err := st.Exist(addr)
					require.NoError(t, err)
					require.True(t, exists, "credited address must exist")
					nonce, err := st.GetNonce(addr)
					require.NoError(t, err)
					codeHash, err := st.GetCodeHash(addr)
					require.NoError(t, err)
					if eip6780 {
						require.Equal(t, uint64(1), nonce, "EIP-6780: committed contract nonce preserved")
						require.NotEqual(t, emptyCodeHash, codeHash, "EIP-6780: committed contract code preserved")
					} else {
						require.Equal(t, uint64(0), nonce, "pre-6780: destroyed contract recreated as EOA, nonce 0")
						require.Equal(t, emptyCodeHash, codeHash, "pre-6780: destroyed contract recreated as EOA, no code")
					}
					return nil
				}))
			})
		}
	}
}
