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
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	libchain "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// A destination carrying storage while having zero nonce, zero balance and empty
// code is the only shape EIP-7610 rejects beyond EIP-684. No post-EIP-161
// execution path produces it, so a genesis alloc is the only way in — which also
// means a chain can put one there deliberately.
func TestEIP7610Create2OntoStorageOnlyAccount(t *testing.T) {
	t.Parallel()

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	bb := common.HexToAddress("0x000000000000000000000000000000000000bbbb")

	// Returns two zero bytes as the deployed code.
	initCode := []byte{
		byte(vm.PUSH1), 0x2, // size
		byte(vm.PUSH1), 0x0, // offset
		byte(vm.RETURN),
	}
	bbCode := []byte{byte(vm.PUSH1) + byte(len(initCode)-1)}
	bbCode = append(bbCode, initCode...)
	bbCode = append(bbCode, []byte{
		byte(vm.PUSH1), 0x0,
		byte(vm.MSTORE),
		byte(vm.PUSH1), 0x00, // salt
		byte(vm.PUSH1), byte(len(initCode)), // size
		byte(vm.PUSH1), byte(32 - len(initCode)), // offset
		byte(vm.PUSH1), 0x00, // endowment
		byte(vm.CREATE2),
	}...)

	initHash := accounts.InternCodeHash(crypto.Keccak256Hash(initCode))
	aa := accounts.InternAddress(types.CreateAddress2(bb, [32]byte{}, initHash))

	oneSlot := map[common.Hash]common.Hash{{}: common.BigToHash(big.NewInt(1))}

	for _, tc := range []struct {
		name     string
		allocAA  *types.GenesisAccount
		wantCode int
		wantSlot bool
	}{
		{
			// One slot and nothing else, so the storage clause is the only
			// thing that can reject the deployment.
			name:     "storage_only_destination_collides",
			allocAA:  &types.GenesisAccount{Balance: big.NewInt(0), Storage: oneSlot},
			wantCode: 0,
			wantSlot: true,
		},
		{
			// An omitted balance is the same account; a genesis file cannot
			// leave it out, but a Go caller building the alloc can.
			name:     "storage_only_nil_balance_collides",
			allocAA:  &types.GenesisAccount{Storage: oneSlot},
			wantCode: 0,
			wantSlot: true,
		},
		{
			// Same CREATE2 with the destination absent: without this the arms
			// above pass just as well on a deployment that never worked.
			name:     "absent_destination_deploys",
			allocAA:  nil,
			wantCode: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			alloc := types.GenesisAlloc{
				address: {Balance: big.NewInt(1000000000)},
				bb:      {Code: bbCode, Balance: big.NewInt(1)},
			}
			if tc.allocAA != nil {
				alloc[aa.Value()] = *tc.allocAA
			}
			gspec := &types.Genesis{Config: libchain.TestChainBerlinConfig, Alloc: alloc}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

			chain, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
				b.SetCoinbase(common.Address{1})
				txn, _ := types.SignTx(types.NewTransaction(0, bb,
					&u256.Num0, 100000, &u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
				b.AddTx(txn)
			})
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(chain))

			require.NoError(t, m.DB.ViewTemporal(m.Ctx, func(tx kv.TemporalTx) error {
				sdb := state.New(m.NewStateReader(tx))
				defer sdb.Close()

				callerNonce, err := sdb.GetNonce(accounts.InternAddress(bb))
				require.NoError(t, err)
				require.Equal(t, uint64(1), callerNonce, "the CREATE2 must have been reached")

				code, err := sdb.GetCode(aa)
				require.NoError(t, err)
				require.Len(t, code, tc.wantCode)

				if tc.wantSlot {
					slot, err := sdb.GetState(aa, accounts.StorageKey{})
					require.NoError(t, err)
					require.Equal(t, uint64(1), slot.Uint64(), "genesis storage must survive")
				}
				return nil
			}))
		})
	}
}
