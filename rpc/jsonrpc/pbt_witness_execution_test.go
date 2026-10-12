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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func pbtWitnessTestBlock(t *testing.T, m *execmoduletester.ExecModuleTester, number uint64) *types.Block {
	t.Helper()
	var block *types.Block
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		var err error
		block, err = m.BlockReader.BlockByNumber(t.Context(), tx, number)
		return err
	}))
	require.NotNil(t, block)
	return block
}

func pbtExecutionWitnessAt(t *testing.T, api *DebugAPIImpl, m *execmoduletester.ExecModuleTester, number uint64) *ExecutionWitnessResult {
	t.Helper()
	trie := "pbt"
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(number)), nil, &trie)
	require.NoError(t, err)
	require.NotNil(t, result)
	block := pbtWitnessTestBlock(t, m, number)
	var parentRoot, postRoot common.Hash
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		var err error
		parent := rawdb.ReadHeaderByNumber(tx, number-1)
		post := rawdb.ReadHeaderByNumber(tx, number)
		require.NotNil(t, parent)
		require.NotNil(t, post)
		parentRoot, err = witnessAnchorForBlock(tx, parent, number-1, witnessTriePBT, m.ChainConfig)
		if err != nil {
			return err
		}
		postRoot, err = witnessAnchorForBlock(tx, post, number, witnessTriePBT, m.ChainConfig)
		return err
	}))
	require.NoError(t, verifyPBinWitnessAgainstBlock(t.Context(), result, block, parentRoot, postRoot, m.ChainConfig, m.Engine))
	return result
}

func pbtStateAfterBlock(t *testing.T, m *execmoduletester.ExecModuleTester, number uint64) *state.IntraBlockState {
	t.Helper()
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	reader, err := rpchelper.CreateHistoryStateReader(t.Context(), tx, number+1, 0, rawdbv3.TxNums)
	require.NoError(t, err)
	result := state.New(reader)
	t.Cleanup(result.Close)
	return result
}

func requirePbtBlockReceipts(t *testing.T, m *execmoduletester.ExecModuleTester, number uint64) {
	t.Helper()
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	receipts, err := rawdb.ReadReceiptsCacheV2(tx, pbtWitnessTestBlock(t, m, number), m.BlockReader.TxnumReader())
	require.NoError(t, err)
	for index, receipt := range receipts {
		require.EqualValues(t, types.ReceiptStatusSuccessful, receipt.Status, "transaction %d in block %d failed", index, number)
	}
}

func TestPBinExecutionWitnessEndToEnd(t *testing.T) {
	bank := pbtCorpusBank(t)
	var small, large common.Address
	largeRuntime := make([]byte, 31*257)
	copy(largeRuntime, pbtCorpusStoreRuntime)
	api, m := pbinWitnessFixtureWithGeneratorNNoSystemCalls(t, 1000, 7, func(i int, b *blockgen.BlockGen, addTransaction func(common.Address, *uint256.Int, []byte), addContract func(*uint256.Int, []byte), _ func(types.Transaction), _ func(common.Address, *uint256.Int, []byte)) {
		switch i {
		case 0:
			addTransaction(common.HexToAddress("0x7300000000000000000000000000000000000000"), uint256.NewInt(1), nil)
		case 1:
			small = types.CreateAddress(bank, b.TxNonce(bank))
			addContract(uint256.NewInt(0), pbtCorpusDeployCode(pbtCorpusStoreRuntime))
		case 2:
			large = types.CreateAddress(bank, b.TxNonce(bank))
			addContract(uint256.NewInt(0), pbtCorpusDeployCode(largeRuntime))
		case 3:
			addTransaction(small, uint256.NewInt(0), pbtCorpusStoreCalldata(common.BigToHash(big.NewInt(1)), 42))
		case 4:
			addTransaction(small, uint256.NewInt(0), pbtCorpusStoreCalldata(common.BigToHash(big.NewInt(1)), 0))
		case 5:
			addTransaction(large, uint256.NewInt(0), pbtCorpusStoreCalldata(common.BigToHash(big.NewInt(2)), 7))
		}
	})
	repairPBinPreForkShadows(t, m, 1000)
	for number := uint64(1); number <= 7; number++ {
		result := pbtExecutionWitnessAt(t, api, m, number)
		requirePbtBlockReceipts(t, m, number)
		if number == 7 {
			require.Empty(t, result.Codes)
		} else {
			require.NotEmpty(t, result.State)
		}
	}
	stateAfter := pbtStateAfterBlock(t, m, 1)
	receiver := common.HexToAddress("0x7300000000000000000000000000000000000000")
	balance, err := stateAfter.GetBalance(accounts.InternAddress(receiver))
	require.NoError(t, err)
	require.Equal(t, uint64(1), balance.Uint64())
	stateAfter = pbtStateAfterBlock(t, m, 2)
	code, err := stateAfter.GetCode(accounts.InternAddress(small))
	require.NoError(t, err)
	require.Equal(t, pbtCorpusStoreRuntime, code)
	stateAfter = pbtStateAfterBlock(t, m, 3)
	code, err = stateAfter.GetCode(accounts.InternAddress(large))
	require.NoError(t, err)
	require.Equal(t, largeRuntime, code)
	stateAfter = pbtStateAfterBlock(t, m, 4)
	value, err := stateAfter.GetState(accounts.InternAddress(small), accounts.InternKey(common.BigToHash(big.NewInt(1))))
	require.NoError(t, err)
	require.Equal(t, uint64(42), value.Uint64())
	stateAfter = pbtStateAfterBlock(t, m, 5)
	value, err = stateAfter.GetState(accounts.InternAddress(small), accounts.InternKey(common.BigToHash(big.NewInt(1))))
	require.NoError(t, err)
	require.True(t, value.IsZero())
	stateAfter = pbtStateAfterBlock(t, m, 6)
	value, err = stateAfter.GetState(accounts.InternAddress(large), accounts.InternKey(common.BigToHash(big.NewInt(2))))
	require.NoError(t, err)
	require.Equal(t, uint64(7), value.Uint64())
}

func TestPBinWitnessConsecutiveDeploys(t *testing.T) {
	api, m := pbinWitnessFixtureWithGeneratorN(t, 1000, 3, nil, func(i int, _ *blockgen.BlockGen, _ func(common.Address, *uint256.Int, []byte), addContract func(*uint256.Int, []byte), _ func(types.Transaction), _ func(common.Address, *uint256.Int, []byte)) {
		runtime := make([]byte, 31*(8+i))
		copy(runtime, pbtCorpusStoreRuntime)
		runtime[len(runtime)-1] = byte(i)
		addContract(uint256.NewInt(0), pbtCorpusDeployCode(runtime))
	})
	repairPBinPreForkShadows(t, m, 1000)
	witnesses := make([]*ExecutionWitnessResult, 0, 3)
	for number := uint64(1); number <= 3; number++ {
		result := pbtExecutionWitnessAt(t, api, m, number)
		witnesses = append(witnesses, result)
		require.NotEmpty(t, result.State)
		requirePbtBlockReceipts(t, m, number)
		stateAfter := pbtStateAfterBlock(t, m, number)
		address := types.CreateAddress(pbtCorpusBank(t), number-1)
		code, err := stateAfter.GetCode(accounts.InternAddress(address))
		require.NoError(t, err)
		runtime := make([]byte, 31*(8+int(number-1)))
		copy(runtime, pbtCorpusStoreRuntime)
		runtime[len(runtime)-1] = byte(number - 1)
		require.Equal(t, runtime, code)
	}
	require.NotEqual(t, witnesses[0].State, witnesses[1].State)
}

func TestPBinExecutionWitnessEmptyBlock(t *testing.T) {
	api, m := pbinWitnessFixtureWithGeneratorNNoSystemCalls(t, 1000, 2, func(int, *blockgen.BlockGen, func(common.Address, *uint256.Int, []byte), func(*uint256.Int, []byte), func(types.Transaction), func(common.Address, *uint256.Int, []byte)) {
	})
	repairPBinPreForkShadows(t, m, 1000)
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	info, err := api.resolveWitnessBlock(t.Context(), tx, rpc.BlockNumberOrHashWithNumber(2))
	require.NoError(t, err)
	accessed, _, err := api.buildAccessedState(t.Context(), tx, info.Block, m.ChainConfig, m.Engine, info.FirstTxNumInBlock, witnessModeLegacy)
	require.NoError(t, err)
	require.True(t, accessed.isEmpty())
	result := pbtExecutionWitnessAt(t, api, m, 2)
	require.NotNil(t, result)
	require.Empty(t, result.Codes)
}

func TestPBinExecutionWitnessFreshAccountAndContract(t *testing.T) {
	api, m := pbinWitnessFixtureWithGeneratorN(t, 1000, 2, nil, func(i int, _ *blockgen.BlockGen, addTransaction func(common.Address, *uint256.Int, []byte), addContract func(*uint256.Int, []byte), _ func(types.Transaction), _ func(common.Address, *uint256.Int, []byte)) {
		if i == 0 {
			addContract(uint256.NewInt(0), common.FromHex("0x6001600c60003960016000f36000"))
			return
		}
		addTransaction(common.HexToAddress("0x7300000000000000000000000000000000000000"), uint256.NewInt(0), nil)
	})
	repairPBinPreForkShadows(t, m, 1000)
	for number := uint64(1); number <= 2; number++ {
		require.NotEmpty(t, pbtExecutionWitnessAt(t, api, m, number).State)
	}
}
