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
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// A CREATE before Spurious Dragon that returns empty code leaves an empty
// account with incarnation 1. A touch removes it (EIP-161) and a later credit in
// the same block creates a new account, whose record must have incarnation 0 as
// on serial. The slow first transaction finishes last, so the parallel executor
// usually validates the touch and the credit in one pass.
func TestTouchedEmptyAccountCreditedInSameBlock(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() {
		dbg.Exec3Parallel = prevParallel
		ethconfig.Defaults.Sync.ExecWorkerCount = prevWorkers
	})
	ethconfig.Defaults.Sync.ExecWorkerCount = 4

	var keys [3]*ecdsa.PrivateKey
	var senders [3]common.Address
	alloc := types.GenesisAlloc{}
	for i, hex := range []string{
		"b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291",
		"8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a",
		"49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee",
	} {
		key, err := crypto.HexToECDSA(hex)
		require.NoError(t, err)
		keys[i] = key
		senders[i] = crypto.PubkeyToAddress(key.PublicKey)
		alloc[senders[i]] = types.GenesisAccount{Balance: big.NewInt(1_000_000_000_000_000_000)}
	}
	burner := common.HexToAddress("0xbbbb")
	alloc[burner] = types.GenesisAccount{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}}
	emptyAcc := types.CreateAddress(senders[1], 0)
	gspec := &types.Genesis{
		Config: &chain.Config{
			ChainID:               uint256.NewInt(1),
			HomesteadBlock:        common.NewUint64(0),
			TangerineWhistleBlock: common.NewUint64(0),
			SpuriousDragonBlock:   common.NewUint64(2),
		},
		GasLimit: 30_000_000,
		Alloc:    alloc,
	}
	signer := types.MakeFrontierSigner()

	type sendFn func(from int, to common.Address, value, gas, gasPrice uint64)
	for _, tc := range []struct {
		name string
		fill func(b *blockgen.BlockGen, send sendFn)
	}{
		{"value-transfer", func(b *blockgen.BlockGen, send sendFn) {
			send(0, burner, 0, 29_000_000, 1)
			send(1, emptyAcc, 0, 21_000, 1)
			send(2, emptyAcc, 1, 21_000, 1)
		}},
		{"coinbase-tip", func(b *blockgen.BlockGen, send sendFn) {
			// The zero-priced tx tips the empty coinbase with 0, which removes it.
			b.SetCoinbase(emptyAcc)
			send(0, burner, 0, 29_000_000, 0)
			send(2, senders[2], 0, 21_000, 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbg.Exec3Parallel = false
			serial := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
			chainPack, err := serial.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
				sign := func(txn types.Transaction, from int) {
					signed, err := types.SignTx(txn, *signer, keys[from])
					require.NoError(t, err)
					b.AddTx(signed)
				}
				if i == 0 {
					sign(types.NewContractCreation(b.TxNonce(senders[1]), uint256.NewInt(0), 100_000, uint256.NewInt(1), []byte{byte(vm.STOP)}), 1)
					return
				}
				tc.fill(b, func(from int, to common.Address, value, gas, gasPrice uint64) {
					sign(types.NewTransaction(b.TxNonce(senders[from]), to, uint256.NewInt(value), gas, uint256.NewInt(gasPrice), nil), from)
				})
			})
			require.NoError(t, err)

			record := func(m *execmoduletester.ExecModuleTester) *accounts.Account {
				var acc *accounts.Account
				require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
					var err error
					acc, err = m.NewStateReader(tx).ReadAccountData(accounts.InternAddress(emptyAcc))
					return err
				}))
				require.NotNil(t, acc)
				return acc
			}

			require.NoError(t, serial.InsertChain(chainPack.Slice(0, 1)))
			before := record(serial)
			require.True(t, before.Empty())
			require.Equal(t, uint64(1), before.Incarnation)

			require.NoError(t, serial.InsertChain(chainPack.Slice(1, 2)))
			want := record(serial)
			require.False(t, want.Balance.IsZero())
			require.Zero(t, want.Incarnation)

			dbg.Exec3Parallel = true
			parallel := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
			require.NoError(t, parallel.InsertChain(chainPack))
			require.Equal(t, *want, *record(parallel))
		})
	}
}
