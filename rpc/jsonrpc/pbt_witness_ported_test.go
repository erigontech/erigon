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
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func pbtPortBlock(t *testing.T, m *execmoduletester.ExecModuleTester, number uint64) *types.Block {
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

func pbtPortWitness(t *testing.T, api *DebugAPIImpl, m *execmoduletester.ExecModuleTester, number uint64) *ExecutionWitnessResult {
	t.Helper()
	trie := "pbt"
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(number)), nil, &trie)
	require.NoError(t, err)
	require.NotNil(t, result)
	block := pbtPortBlock(t, m, number)
	parentRoot := pbtCorpusAnchor(t, m, number-1)
	postRoot := pbtCorpusAnchor(t, m, number)
	require.NoError(t, verifyPBinWitnessAgainstBlock(t.Context(), result, block, parentRoot, postRoot, m.ChainConfig, m.Engine))
	return result
}

func TestPBinExecutionWitnessEndToEnd(t *testing.T) {
	bank := pbtCorpusBank(t)
	var small, large common.Address
	largeRuntime := make([]byte, 31*257)
	copy(largeRuntime, pbtCorpusStoreRuntime)
	api, m := pbinWitnessFixtureWithGeneratorN(t, 1000, 7, nil, func(i int, b *blockgen.BlockGen, addTransaction func(common.Address, *uint256.Int, []byte), addContract func(*uint256.Int, []byte), _ func(types.Transaction), _ func(common.Address, *uint256.Int, []byte)) {
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
		result := pbtPortWitness(t, api, m, number)
		if number < 7 {
			require.NotEmpty(t, result.State)
		}
	}
}

func TestPBinWitnessConsecutiveDeploys(t *testing.T) {
	api, m := pbinWitnessFixtureWithGeneratorN(t, 1000, 3, nil, func(i int, _ *blockgen.BlockGen, _ func(common.Address, *uint256.Int, []byte), addContract func(*uint256.Int, []byte), _ func(types.Transaction), _ func(common.Address, *uint256.Int, []byte)) {
		runtime := make([]byte, 31*(8+i))
		copy(runtime, pbtCorpusStoreRuntime)
		runtime[len(runtime)-1] = byte(i)
		addContract(uint256.NewInt(0), pbtCorpusDeployCode(runtime))
	})
	repairPBinPreForkShadows(t, m, 1000)
	for number := uint64(1); number <= 3; number++ {
		require.NotEmpty(t, pbtPortWitness(t, api, m, number).State)
	}
}

func TestPBinExecutionWitnessEmptyBlock(t *testing.T) {
	api, m := pbinWitnessFixtureWithGeneratorN(t, 1000, 2, nil, func(int, *blockgen.BlockGen, func(common.Address, *uint256.Int, []byte), func(*uint256.Int, []byte), func(types.Transaction), func(common.Address, *uint256.Int, []byte)) {
	})
	repairPBinPreForkShadows(t, m, 1000)
	result := pbtPortWitness(t, api, m, 2)
	require.NotNil(t, result)
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
		require.NotEmpty(t, pbtPortWitness(t, api, m, number).State)
	}
}
