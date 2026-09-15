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

package jsonrpc

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestPBinExecutionWitnessCreateOverStorage(t *testing.T) {
	for _, tc := range []struct {
		name string
		slot common.Hash
	}{
		{"header slot", pbinStatelessSlot(3)},
		{"storage zone slot", pbinStatelessSlot(1 << 20)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withCommitmentHistory(t)
			withBinCommitmentDatadir(t)

			bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			bankAddress := crypto.PubkeyToAddress(bankKey.PublicKey)
			victim := types.CreateAddress(bankAddress, 0)

			alloc := types.GenesisAlloc{
				bankAddress: {Balance: big.NewInt(1e18)},
				victim: {
					Balance: big.NewInt(1),
					Storage: map[common.Hash]common.Hash{tc.slot: common.BigToHash(big.NewInt(9))},
				},
			}
			for i := range 256 {
				alloc[common.BytesToAddress([]byte{0x02, byte(i)})] = types.GenesisAccount{
					Balance: big.NewInt(1),
					Storage: map[common.Hash]common.Hash{pbinStatelessSlot(1 << 20): common.BigToHash(big.NewInt(1))},
				}
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: chain.TestChainBerlinConfig.Copy(),
				Alloc:  alloc,
			}), execmoduletester.WithKey(bankKey))

			signer := types.LatestSignerForChainID(nil)
			pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
				if i > 0 {
					return
				}
				txn, err := types.SignTx(&types.LegacyTx{
					CommonTx: types.CommonTx{Nonce: b.TxNonce(bankAddress), GasLimit: 200_000, Data: pbinDeployCode(pbinStoreRuntime)},
					GasPrice: *uint256.NewInt(1_000_000_000),
				}, *signer, bankKey)
				require.NoError(t, err)
				b.AddTx(txn)
			})
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(pack))
			require.EqualValues(t, types.ReceiptStatusSuccessful, pack.Receipts[0][0].Status)
			requirePBinCreateWipedStorage(t, m, victim, tc.slot)

			c := &pbinWitnessChain{m: m, pack: pack}
			result := pbinWitnessOf(t, pbinWitnessAPI(t, m), 1)
			requirePBinWitnessVerifies(t, c, result, 1)
		})
	}
}

func requirePBinCreateWipedStorage(t *testing.T, m *execmoduletester.ExecModuleTester, victim common.Address, slot common.Hash) {
	t.Helper()

	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	reader, err := rpchelper.CreateHistoryStateReader(t.Context(), tx, 2, 0, rawdbv3.TxNums)
	require.NoError(t, err)
	st := state.New(reader)
	defer st.Close()

	code, err := st.GetCode(accounts.InternAddress(victim))
	require.NoError(t, err)
	require.Equal(t, pbinStoreRuntime, code)

	value, err := st.GetState(accounts.InternAddress(victim), accounts.InternKey(slot))
	require.NoError(t, err)
	require.True(t, value.IsZero())
}
