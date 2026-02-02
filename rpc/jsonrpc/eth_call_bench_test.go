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
	"context"
	"math/big"
	"testing"

	"github.com/erigontech/erigon/rpc"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// createBenchmarkSentry creates a mock sentry for benchmark tests
func createBenchmarkSentry(tb testing.TB) *mock.MockSentry {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)

	genesis := &types.Genesis{
		Config: chain.TestChainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: big.NewInt(9000000000000000000)},
		},
		GasLimit: 10000000,
	}

	return mock.MockWithGenesis(tb, genesis, key, false)
}

// createBenchmarkSentryWithContract creates a mock sentry with a deployed contract for benchmark tests
func createBenchmarkSentryWithContract(tb testing.TB) (*mock.MockSentry, common.Address, common.Address) {
	signer := types.LatestSignerForChainID(nil)
	bankKey, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	bankAddress := crypto.PubkeyToAddress(bankKey.PublicKey)
	bankFunds := big.NewInt(1e9)
	contract := hexutil.MustDecode(contractHexString)

	genesis := &types.Genesis{
		Config: chain.TestChainConfig,
		Alloc:  types.GenesisAlloc{bankAddress: {Balance: bankFunds}},
	}

	m := mock.MockWithGenesis(tb, genesis, bankKey, false)

	var contractAddr common.Address

	chainBlocks, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, block *blockgen.BlockGen) {
		nonce := block.TxNonce(bankAddress)
		switch i {
		case 0: // Legacy transaction contract creation
			tx, err := types.SignTx(types.NewContractCreation(nonce, new(uint256.Int), 1e6, new(uint256.Int), contract), *signer, bankKey)
			if err != nil {
				tb.Fatal(err)
			}
			block.AddTx(tx)
			contractAddr = types.CreateAddress(bankAddress, nonce)
		case 1: // Legacy transaction
			txn, err := types.SignTx(types.NewTransaction(nonce, contractAddr, new(uint256.Int), 900000, new(uint256.Int), contractInvocationData(1)), *signer, bankKey)
			if err != nil {
				tb.Fatal(err)
			}
			block.AddTx(txn)
		}
	})
	if err != nil {
		tb.Fatalf("generate blocks: %v", err)
	}

	if err := m.InsertChain(chainBlocks); err != nil {
		tb.Fatal(err)
	}

	return m, bankAddress, contractAddr
}

// BenchmarkCall benchmarks the eth_call method with simple value transfers
func BenchmarkCall(b *testing.B) {
	m := createBenchmarkSentry(b)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(nil, stateCache, m), m.DB, nil, nil)

	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	value := (*hexutil.Big)(common.Big0)

	args := ethapi.CallArgs{
		From:  &from,
		To:    &to,
		Gas:   &gas,
		Value: value,
	}

	ctx := context.Background()

	// Warm up
	_, err := api.Call(ctx, args, nil, nil, nil)
	if err != nil {
		b.Fatalf("warm up call failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := api.Call(ctx, args, nil, nil, nil)
		if err != nil {
			b.Fatalf("call failed: %v", err)
		}
	}
}

// BenchmarkCallWithBlockNumber benchmarks eth_call with an explicit block number
func BenchmarkCallWithBlockNumber(b *testing.B) {
	m := createBenchmarkSentry(b)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(nil, stateCache, m), m.DB, nil, nil)

	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	value := (*hexutil.Big)(common.Big0)

	args := ethapi.CallArgs{
		From:  &from,
		To:    &to,
		Gas:   &gas,
		Value: value,
	}

	blockNumber := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	ctx := context.Background()

	// Warm up
	_, err := api.Call(ctx, args, &blockNumber, nil, nil)
	if err != nil {
		b.Fatalf("warm up call failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := api.Call(ctx, args, &blockNumber, nil, nil)
		if err != nil {
			b.Fatalf("call failed: %v", err)
		}
	}
}

// BenchmarkCallToContract benchmarks eth_call to a contract
func BenchmarkCallToContract(b *testing.B) {
	m, bankAddress, contractAddress := createBenchmarkSentryWithContract(b)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(nil, stateCache, m), m.DB, nil, nil)

	// Call the retrieve() function of the Box contract
	callData := hexutil.MustDecode("0x2e64cec1")
	callDataBytes := hexutil.Bytes(callData)
	gas := hexutil.Uint64(100000)

	args := ethapi.CallArgs{
		From: &bankAddress,
		To:   &contractAddress,
		Gas:  &gas,
		Data: &callDataBytes,
	}

	ctx := context.Background()

	// Warm up
	_, err := api.Call(ctx, args, nil, nil, nil)
	if err != nil {
		b.Fatalf("warm up call failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := api.Call(ctx, args, nil, nil, nil)
		if err != nil {
			b.Fatalf("call failed: %v", err)
		}
	}
}

// BenchmarkCallWithStateOverrides benchmarks eth_call with state overrides
func BenchmarkCallWithStateOverrides(b *testing.B) {
	m := createBenchmarkSentry(b)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(nil, stateCache, m), m.DB, nil, nil)

	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	value := (*hexutil.Big)(common.Big0)

	args := ethapi.CallArgs{
		From:  &from,
		To:    &to,
		Gas:   &gas,
		Value: value,
	}

	// Create state overrides
	balance := (*hexutil.Big)(common.Big1)
	stateOverrides := &ethapi.StateOverrides{
		accounts.InternAddress(from): ethapi.Account{
			Balance: &balance,
		},
	}

	ctx := context.Background()

	// Warm up
	_, err := api.Call(ctx, args, nil, stateOverrides, nil)
	if err != nil {
		b.Fatalf("warm up call failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := api.Call(ctx, args, nil, stateOverrides, nil)
		if err != nil {
			b.Fatalf("call failed: %v", err)
		}
	}
}

// BenchmarkCallParallel benchmarks eth_call with parallel execution
func BenchmarkCallParallel(b *testing.B) {
	m := createBenchmarkSentry(b)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(nil, stateCache, m), m.DB, nil, nil)

	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	gas := hexutil.Uint64(21000)
	value := (*hexutil.Big)(common.Big0)

	args := ethapi.CallArgs{
		From:  &from,
		To:    &to,
		Gas:   &gas,
		Value: value,
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := api.Call(ctx, args, nil, nil, nil)
			if err != nil {
				b.Errorf("call failed: %v", err)
			}
		}
	})
}

// BenchmarkCallContractParallel benchmarks eth_call to contract with parallel execution
func BenchmarkCallContractParallel(b *testing.B) {
	m, bankAddress, contractAddress := createBenchmarkSentryWithContract(b)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(nil, stateCache, m), m.DB, nil, nil)

	// Call the retrieve() function of the Box contract
	callData := hexutil.MustDecode("0x2e64cec1")
	callDataBytes := hexutil.Bytes(callData)
	gas := hexutil.Uint64(100000)

	args := ethapi.CallArgs{
		From: &bankAddress,
		To:   &contractAddress,
		Gas:  &gas,
		Data: &callDataBytes,
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := api.Call(ctx, args, nil, nil, nil)
			if err != nil {
				b.Errorf("call failed: %v", err)
			}
		}
	})
}
