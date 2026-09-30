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
	"math"
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

func TestPBinExecutionWitnessWipePort(t *testing.T) {
	api, m := pbtCorpusChain(t)
	for _, number := range []uint64{5, 9, 10} {
		result := pbtPortWitness(t, api, m, number)
		require.NotEmpty(t, result.State, "block %d has no witness state", number)
	}
}

var pbtWipeSlots = []struct {
	name    string
	key     common.Hash
	storage map[common.Hash]common.Hash
}{
	{name: "header slot", key: pbtCorpusSlot(3), storage: map[common.Hash]common.Hash{pbtCorpusSlot(3): common.BigToHash(big.NewInt(9))}},
	{name: "storage zone slot", key: pbtCorpusSlot(1 << 20), storage: map[common.Hash]common.Hash{pbtCorpusSlot(1 << 20): common.BigToHash(big.NewInt(9))}},
	{name: "no storage", key: pbtCorpusSlot(3)},
}

func TestPBinExecutionWitnessCreateOverStorage(t *testing.T) {
	bankKey := pbtWipeBankKey(t)
	victim := types.CreateAddress(crypto.PubkeyToAddress(bankKey.PublicKey), 0)
	for _, slot := range pbtWipeSlots {
		for _, scenario := range []struct {
			name   string
			writes []uint64
			want   uint64
		}{
			{name: "create", want: 0},
			{name: "create then write", writes: []uint64{7}, want: 7},
			{name: "create then write and clear", writes: []uint64{7, 0}, want: 0},
		} {
			t.Run(slot.name+"/"+scenario.name, func(t *testing.T) {
				m := pbtWipeTester(t, bankKey, types.GenesisAlloc{victim: {Balance: big.NewInt(1), Storage: slot.storage}})
				txs := []*types.LegacyTx{{CommonTx: types.CommonTx{GasLimit: 200_000, Data: pbtCorpusDeployCode(pbtCorpusStoreRuntime)}}}
				for _, value := range scenario.writes {
					txs = append(txs, &types.LegacyTx{CommonTx: types.CommonTx{To: &victim, GasLimit: 100_000, Data: pbtCorpusStoreCalldata(slot.key, value)}})
				}
				pbtWipeBlock(t, m, bankKey, txs)
				pbtWipeStateAfterBlock(t, m, func(st *state.IntraBlockState) {
					code, err := st.GetCode(accounts.InternAddress(victim))
					require.NoError(t, err)
					require.Equal(t, pbtCorpusStoreRuntime, code)
					value, err := st.GetState(accounts.InternAddress(victim), accounts.InternKey(slot.key))
					require.NoError(t, err)
					require.Equal(t, scenario.want, value.Uint64())
				})
			})
		}
	}
}

func TestPBinExecutionWitnessDeleteOverStorage(t *testing.T) {
	bankKey := pbtWipeBankKey(t)
	victim := common.HexToAddress("0x00000000000000000000000000000000000abcde")
	for _, slot := range pbtWipeSlots {
		for _, refund := range []uint64{0, 1} {
			t.Run(slot.name+"/"+new(big.Int).SetUint64(refund).String(), func(t *testing.T) {
				m := pbtWipeTester(t, bankKey, types.GenesisAlloc{victim: {Storage: slot.storage}})
				txs := []*types.LegacyTx{{CommonTx: types.CommonTx{To: &victim, GasLimit: 100_000}}}
				if refund != 0 {
					txs = append(txs, &types.LegacyTx{CommonTx: types.CommonTx{To: &victim, GasLimit: 21_000, Value: *uint256.NewInt(refund)}})
				}
				pbtWipeBlock(t, m, bankKey, txs)
				pbtWipeStateAfterBlock(t, m, func(st *state.IntraBlockState) {
					exists, err := st.Exist(accounts.InternAddress(victim))
					require.NoError(t, err)
					require.Equal(t, refund != 0, exists)
					value, err := st.GetState(accounts.InternAddress(victim), accounts.InternKey(slot.key))
					require.NoError(t, err)
					require.True(t, value.IsZero())
				})
			})
		}
	}
}

func TestPBinExecutionWitnessRecreateInBlock(t *testing.T) {
	bankKey := pbtWipeBankKey(t)
	factory := common.HexToAddress("0x00000000000000000000000000000000000fac70")
	childInit := common.FromHex("0x60016000553415600b57005b33ff")
	child := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(childInit))))
	m := pbtWipeTester(t, bankKey, types.GenesisAlloc{factory: {Code: common.FromHex("0x366000600037600036600034f5345500")}})
	pbtWipeBlock(t, m, bankKey, []*types.LegacyTx{
		{CommonTx: types.CommonTx{To: &factory, GasLimit: 300_000, Data: childInit}},
		{CommonTx: types.CommonTx{To: &factory, GasLimit: 300_000, Data: childInit, Value: *uint256.NewInt(1)}},
	})
	pbtWipeStateAfterBlock(t, m, func(st *state.IntraBlockState) {
		for _, slot := range []uint64{0, 1} {
			value, err := st.GetState(accounts.InternAddress(factory), accounts.InternKey(pbtCorpusSlot(slot)))
			require.NoError(t, err)
			require.Equal(t, child, common.Address(value.Bytes20()))
		}
		nonce, err := st.GetNonce(accounts.InternAddress(child))
		require.NoError(t, err)
		require.Equal(t, uint64(1), nonce)
		balance, err := st.GetBalance(accounts.InternAddress(child))
		require.NoError(t, err)
		require.Equal(t, uint64(1), balance.Uint64())
		value, err := st.GetState(accounts.InternAddress(child), accounts.InternKey(common.Hash{}))
		require.NoError(t, err)
		require.Equal(t, uint64(1), value.Uint64())
	})
	result := pbtPortWitness(t, newDebugApiForTest(m), m, 1)
	require.NotEmpty(t, result.State)
}

func pbtWipeBankKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	return key
}

func pbtWipeTester(t *testing.T, bankKey *ecdsa.PrivateKey, extra types.GenesisAlloc) *execmoduletester.ExecModuleTester {
	t.Helper()
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)
	alloc := types.GenesisAlloc{crypto.PubkeyToAddress(bankKey.PublicKey): {Balance: big.NewInt(1e18)}}
	maps.Copy(alloc, extra)
	for i := range 256 {
		alloc[common.BytesToAddress([]byte{0x02, byte(i)})] = types.GenesisAccount{Balance: big.NewInt(1), Storage: map[common.Hash]common.Hash{pbtCorpusSlot(1 << 20): common.BigToHash(big.NewInt(1))}}
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: chain.TestChainBerlinConfig.Copy(), Alloc: alloc}), execmoduletester.WithKey(bankKey))
	enableCommitmentHistoryFlag(t, m.DB)
	return m
}

func pbtWipeBlock(t *testing.T, m *execmoduletester.ExecModuleTester, bankKey *ecdsa.PrivateKey, txs []*types.LegacyTx) *types.Block {
	t.Helper()
	bank := crypto.PubkeyToAddress(bankKey.PublicKey)
	signer := types.LatestSignerForChainID(nil)
	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(_ int, b *blockgen.BlockGen) {
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
	for index, receipt := range pack.Receipts[0] {
		require.EqualValues(t, types.ReceiptStatusSuccessful, receipt.Status, "transaction %d in block 1 failed", index)
	}
	repairPBinPreForkShadows(t, m, math.MaxUint64)
	return pack.Blocks[0]
}

func pbtWipeStateAfterBlock(t *testing.T, m *execmoduletester.ExecModuleTester, check func(*state.IntraBlockState)) {
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
