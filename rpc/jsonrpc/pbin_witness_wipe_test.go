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
	"crypto/ecdsa"
	"maps"
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

var pbinWipeSlots = []struct {
	name    string
	slot    common.Hash
	storage map[common.Hash]common.Hash
}{
	{"header slot", pbinStatelessSlot(3), map[common.Hash]common.Hash{pbinStatelessSlot(3): common.BigToHash(big.NewInt(9))}},
	{"storage zone slot", pbinStatelessSlot(1 << 20), map[common.Hash]common.Hash{pbinStatelessSlot(1 << 20): common.BigToHash(big.NewInt(9))}},
	{"no storage", pbinStatelessSlot(3), nil},
}

func TestPBinExecutionWitnessCreateOverStorage(t *testing.T) {
	bankKey := pbinWipeBankKey(t)
	victim := types.CreateAddress(crypto.PubkeyToAddress(bankKey.PublicKey), 0)

	for _, slot := range pbinWipeSlots {
		for _, tc := range []struct {
			name   string
			writes []uint64
			want   uint64
		}{
			{"create", nil, 0},
			{"create then write", []uint64{7}, 7},
			{"create then write and clear", []uint64{7, 0}, 0},
		} {
			t.Run(slot.name+"/"+tc.name, func(t *testing.T) {
				m := pbinWipeTester(t, bankKey, types.GenesisAlloc{
					victim: {Balance: big.NewInt(1), Storage: slot.storage},
				})
				txs := []*types.LegacyTx{{CommonTx: types.CommonTx{GasLimit: 200_000, Data: pbinDeployCode(pbinStoreRuntime)}}}
				for _, value := range tc.writes {
					txs = append(txs, &types.LegacyTx{CommonTx: types.CommonTx{
						To: &victim, GasLimit: 100_000, Data: pbinStoreCalldata(slot.slot, value),
					}})
				}
				c := pbinWipeBlock(t, m, bankKey, txs)

				requirePBinStateAfterBlock1(t, m, func(st *state.IntraBlockState) {
					code, err := st.GetCode(accounts.InternAddress(victim))
					require.NoError(t, err)
					require.Equal(t, pbinStoreRuntime, code)
					value, err := st.GetState(accounts.InternAddress(victim), accounts.InternKey(slot.slot))
					require.NoError(t, err)
					require.Equal(t, tc.want, value.Uint64())
				})

				result := pbinWitnessOf(t, pbinWitnessAPI(t, m), 1)
				requirePBinWitnessVerifies(t, c, result, 1)
			})
		}
	}
}

func TestPBinExecutionWitnessDeleteOverStorage(t *testing.T) {
	bankKey := pbinWipeBankKey(t)
	victim := common.HexToAddress("0x00000000000000000000000000000000000abcde")

	for _, slot := range pbinWipeSlots {
		for _, tc := range []struct {
			name   string
			refund uint64
		}{
			{"delete", 0},
			{"delete then refund", 1},
		} {
			t.Run(slot.name+"/"+tc.name, func(t *testing.T) {
				m := pbinWipeTester(t, bankKey, types.GenesisAlloc{
					victim: {Balance: big.NewInt(0), Storage: slot.storage},
				})
				txs := []*types.LegacyTx{{CommonTx: types.CommonTx{To: &victim, GasLimit: 100_000}}}
				if tc.refund > 0 {
					txs = append(txs, &types.LegacyTx{CommonTx: types.CommonTx{
						To: &victim, GasLimit: 21_000, Value: *uint256.NewInt(tc.refund),
					}})
				}
				c := pbinWipeBlock(t, m, bankKey, txs)

				requirePBinStateAfterBlock1(t, m, func(st *state.IntraBlockState) {
					exists, err := st.Exist(accounts.InternAddress(victim))
					require.NoError(t, err)
					require.Equal(t, tc.refund > 0, exists)
					value, err := st.GetState(accounts.InternAddress(victim), accounts.InternKey(slot.slot))
					require.NoError(t, err)
					require.True(t, value.IsZero())
				})

				result := pbinWitnessOf(t, pbinWitnessAPI(t, m), 1)
				requirePBinWitnessVerifies(t, c, result, 1)
			})
		}
	}
}

func TestPBinExecutionWitnessRecreateInBlock(t *testing.T) {
	bankKey := pbinWipeBankKey(t)
	factory := common.HexToAddress("0x00000000000000000000000000000000000fac70")
	childInit := common.FromHex("0x60016000553415600b57005b33ff")
	child := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(childInit))))

	m := pbinWipeTester(t, bankKey, types.GenesisAlloc{
		factory: {Balance: big.NewInt(0), Nonce: 1, Code: common.FromHex("0x366000600037600036600034f5345500")},
	})
	c := pbinWipeBlock(t, m, bankKey, []*types.LegacyTx{
		{CommonTx: types.CommonTx{To: &factory, GasLimit: 300_000, Data: childInit}},
		{CommonTx: types.CommonTx{To: &factory, GasLimit: 300_000, Data: childInit, Value: *uint256.NewInt(1)}},
	})

	requirePBinStateAfterBlock1(t, m, func(st *state.IntraBlockState) {
		for _, slot := range []uint64{0, 1} {
			created, err := st.GetState(accounts.InternAddress(factory), accounts.InternKey(pbinStatelessSlot(slot)))
			require.NoError(t, err)
			require.Equal(t, child, common.Address(created.Bytes20()), "CREATE2 with call value %d", slot)
		}
		address := accounts.InternAddress(child)
		nonce, err := st.GetNonce(address)
		require.NoError(t, err)
		require.Equal(t, uint64(1), nonce)
		balance, err := st.GetBalance(address)
		require.NoError(t, err)
		require.Equal(t, uint64(1), balance.Uint64())
		value, err := st.GetState(address, accounts.InternKey(common.Hash{}))
		require.NoError(t, err)
		require.Equal(t, uint64(1), value.Uint64())
	})

	result := pbinWitnessOf(t, pbinWitnessAPI(t, m), 1)
	requirePBinWitnessVerifies(t, c, result, 1)
}

func pbinWipeBankKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	return key
}

func pbinWipeTester(t *testing.T, bankKey *ecdsa.PrivateKey, extra types.GenesisAlloc) *execmoduletester.ExecModuleTester {
	t.Helper()
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)

	alloc := types.GenesisAlloc{crypto.PubkeyToAddress(bankKey.PublicKey): {Balance: big.NewInt(1e18)}}
	maps.Copy(alloc, extra)
	for i := range 256 {
		alloc[common.BytesToAddress([]byte{0x02, byte(i)})] = types.GenesisAccount{
			Balance: big.NewInt(1),
			Storage: map[common.Hash]common.Hash{pbinStatelessSlot(1 << 20): common.BigToHash(big.NewInt(1))},
		}
	}
	return execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: chain.TestChainBerlinConfig.Copy(),
		Alloc:  alloc,
	}), execmoduletester.WithKey(bankKey))
}

func pbinWipeBlock(t *testing.T, m *execmoduletester.ExecModuleTester, bankKey *ecdsa.PrivateKey, txs []*types.LegacyTx) *pbinWitnessChain {
	t.Helper()
	bank := crypto.PubkeyToAddress(bankKey.PublicKey)
	signer := types.LatestSignerForChainID(nil)
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
		if i > 0 {
			return
		}
		for _, txn := range txs {
			txn.Nonce = b.TxNonce(bank)
			txn.GasPrice = *uint256.NewInt(1_000_000_000)
			signed, err := types.SignTx(txn, *signer, bankKey)
			require.NoError(t, err)
			b.AddTx(signed)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	require.Len(t, pack.Receipts[0], len(txs))
	for i, receipt := range pack.Receipts[0] {
		require.EqualValues(t, types.ReceiptStatusSuccessful, receipt.Status, "transaction %d in block 1 failed", i)
	}
	return &pbinWitnessChain{m: m, pack: pack}
}

func requirePBinStateAfterBlock1(t *testing.T, m *execmoduletester.ExecModuleTester, check func(st *state.IntraBlockState)) {
	t.Helper()

	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	reader, err := rpchelper.CreateHistoryStateReader(t.Context(), tx, 2, 0, rawdbv3.TxNums)
	require.NoError(t, err)
	st := state.New(reader)
	defer st.Close()

	check(st)
}
