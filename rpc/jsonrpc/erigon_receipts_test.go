// Copyright 2024 The Erigon Authors
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
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/filters"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/rpc"
)

func TestGetLogs(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	{
		ethApi := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, 5000000, ethconfig.Defaults.RPCTxFeeCap, 100_000, false, 100_000, 128, log.New())

		logs, err := ethApi.GetLogs(context.Background(), filters.FilterCriteria{FromBlock: big.NewInt(0), ToBlock: big.NewInt(10)})
		require.NoError(err)
		assert.Equal(uint64(10), logs[0].BlockNumber)

		// filter by wrong address
		logs, err = ethApi.GetLogs(context.Background(), filters.FilterCriteria{
			FromBlock: big.NewInt(10),
			ToBlock:   big.NewInt(10),
			Addresses: common.Addresses{common.Address{}},
		})
		require.NoError(err)
		assert.Empty(logs)

		// filter by wrong address
		logs, err = ethApi.GetLogs(m.Ctx, filters.FilterCriteria{
			FromBlock: big.NewInt(10),
			ToBlock:   big.NewInt(10),
			Topics:    [][]common.Hash{{common.HexToHash("0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb")}},
		})
		require.NoError(err)
		assert.Len(logs, 1)
	}
}

func TestErigonGetLatestLogs(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := NewErigonAPI(newBaseApiForTest(m), db, nil)
	expectedLogs, _ := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(0), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())})

	expectedErigonLogs := make(types.ErigonLogs, 0)
	for i := len(expectedLogs) - 1; i >= 0; i-- {
		expectedErigonLogs = append(expectedErigonLogs, &types.ErigonLog{
			Address:     expectedLogs[i].Address,
			Topics:      expectedLogs[i].Topics,
			Data:        expectedLogs[i].Data,
			BlockNumber: expectedLogs[i].BlockNumber,
			TxHash:      expectedLogs[i].TxHash,
			TxIndex:     expectedLogs[i].TxIndex,
			BlockHash:   expectedLogs[i].BlockHash,
			Index:       expectedLogs[i].Index,
			Removed:     expectedLogs[i].Removed,
			Timestamp:   expectedLogs[i].Timestamp,
		})
	}
	actual, err := api.GetLatestLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(0), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())}, filters.LogFilterOptions{
		LogCount: uint64(len(expectedLogs)),
	})
	if err != nil {
		t.Errorf("calling erigon_getLatestLogs: %v", err)
	}
	require.NotNil(t, actual)
	assert.Equal(expectedErigonLogs, actual)

	expectedLog := &types.ErigonLog{
		Address:     common.HexToAddress("0x3CB5b6E26e0f37F2514D45641F15Bd6fEC2E0c4c"),
		Topics:      []common.Hash{common.HexToHash("0x68f6a0f063c25c6678c443b9a484086f15ba8f91f60218695d32a5251f2050eb")},
		Data:        []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 151, 160, 176, 241, 203, 220, 75, 75, 222, 127, 170, 33, 171, 34, 107, 143, 20, 185, 234, 201},
		BlockNumber: 10,
		TxHash:      common.HexToHash("0xb6449d8e167a8826d050afe4c9f07095236ff769a985f02649b1023c2ded2059"),
		TxIndex:     0,
		BlockHash:   common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef"),
		Index:       0,
		Removed:     false,
		Timestamp:   100,
	}
	assert.Equal(expectedLog, actual[0])
}

func TestErigonGetLatestLogsIgnoreTopics(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := NewErigonAPI(newBaseApiForTest(m), db, nil)
	expectedLogs, _ := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(0), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())})

	expectedErigonLogs := make([]*types.ErigonLog, 0)
	for i := len(expectedLogs) - 1; i >= 0; i-- {
		expectedErigonLogs = append(expectedErigonLogs, &types.ErigonLog{
			Address:     expectedLogs[i].Address,
			Topics:      expectedLogs[i].Topics,
			Data:        expectedLogs[i].Data,
			BlockNumber: expectedLogs[i].BlockNumber,
			TxHash:      expectedLogs[i].TxHash,
			TxIndex:     expectedLogs[i].TxIndex,
			BlockHash:   expectedLogs[i].BlockHash,
			Index:       expectedLogs[i].Index,
			Removed:     expectedLogs[i].Removed,
			Timestamp:   expectedLogs[i].Timestamp,
		})
	}

	var lastBlock uint64
	var blockCount uint64
	containsTopics := make([][]common.Hash, 0)

	for i := range expectedLogs {
		if expectedLogs[i].BlockNumber != lastBlock {
			blockCount++
		}
		containsTopics = append(containsTopics, []common.Hash{
			expectedLogs[i].Topics[0],
		})
	}
	actual, err := api.GetLatestLogs(m.Ctx, filters.FilterCriteria{Topics: containsTopics}, filters.LogFilterOptions{
		BlockCount: blockCount,
	})
	if err != nil {
		t.Errorf("calling erigon_getLatestLogs: %v", err)
	}
	require.NotNil(t, actual)
	assert.EqualValues(expectedErigonLogs, actual)
}

var (
	// testKey is a private key to use for funding a tester account.
	testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	// testAddr is the Ethereum address of the tester account.
	testAddr = crypto.PubkeyToAddress(testKey.PublicKey)
)

func TestGetBlockReceiptsByBlockHash(t *testing.T) {
	// Define three accounts to simulate transactions with
	acc1Key, _ := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	acc2Key, _ := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	acc1Addr := crypto.PubkeyToAddress(acc1Key.PublicKey)
	acc2Addr := crypto.PubkeyToAddress(acc2Key.PublicKey)

	signer := types.LatestSignerForChainID(nil)
	// Create a chain generator with some simple transactions (blatantly stolen from @fjl/chain_markets_test)
	generator := func(i int, block *core.BlockGen) {
		switch i {
		case 0:
			// In block 1, the test bank sends account #1 some ether.
			tx, _ := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), acc1Addr, uint256.NewInt(10000), params.TxGas, nil, nil), *signer, testKey)
			block.AddTx(tx)
		case 1:
			// In block 2, the test bank sends some more ether to account #1.
			// acc1Addr passes it on to account #2.
			tx1, _ := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), acc1Addr, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, testKey)
			tx2, _ := types.SignTx(types.NewTransaction(block.TxNonce(acc1Addr), acc2Addr, uint256.NewInt(1000), params.TxGas, nil, nil), *signer, acc1Key)
			block.AddTx(tx1)
			block.AddTx(tx2)
		case 2:
			// Block 3 is empty but was mined by account #2.
			block.SetCoinbase(acc2Addr)
			block.SetExtra([]byte("yeehaw"))
		case 3:
			// Block 4 includes blocks 2 and 3 as uncle headers (with modified extra data).
			b2 := block.PrevBlock(1).Header()
			b2.Extra = []byte("foo")
			block.AddUncle(b2)
			b3 := block.PrevBlock(2).Header()
			b3.Extra = []byte("foo")
			block.AddUncle(b3)
		}
	}
	// Assemble the test environment
	m := mockWithGenerator(t, 4, generator)
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	expect := map[uint64]string{
		0: `[]`,
		1: `[{"blockHash":"0x63b978611c906a61e4a8333fedeea8d62a1c869fc9a19acf6ed0cc5139247eda","blockNumber":"0x1","contractAddress":null,"cumulativeGasUsed":"0x5208","effectiveGasPrice":"0x0","from":"0x71562b71999873db5b286df957af199ec94617f7","gasUsed":"0x5208","logs":[],"logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","status":"0x1","to":"0x703c4b2bd70c169f5717101caee543299fc946c7","transactionHash":"0x9ca7a9e6bf23353fc5ac37f5c5676db1accec4af83477ac64cdcaa37f3a837f9","transactionIndex":"0x0","type":"0x0"}]`,
		2: `[{"blockHash":"0xd3294fcc342ff74be4ae07fb25cd3b2fbb6c2b7830f212ee0723da956e70e099","blockNumber":"0x2","contractAddress":null,"cumulativeGasUsed":"0x5208","effectiveGasPrice":"0x0","from":"0x71562b71999873db5b286df957af199ec94617f7","gasUsed":"0x5208","logs":[],"logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","status":"0x1","to":"0x703c4b2bd70c169f5717101caee543299fc946c7","transactionHash":"0xf190eed1578cdcfe69badd05b7ef183397f336dc3de37baa4adbfb4bc657c11e","transactionIndex":"0x0","type":"0x0"},{"blockHash":"0xd3294fcc342ff74be4ae07fb25cd3b2fbb6c2b7830f212ee0723da956e70e099","blockNumber":"0x2","contractAddress":null,"cumulativeGasUsed":"0xa410","effectiveGasPrice":"0x0","from":"0x703c4b2bd70c169f5717101caee543299fc946c7","gasUsed":"0x5208","logs":[],"logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","status":"0x1","to":"0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e","transactionHash":"0x309a030e44058e435a2b01302006880953e2c9319009db97013eb130d7a24eab","transactionIndex":"0x1","type":"0x0"}]`,
		3: `[]`,
		4: `[]`,
	}
	err := m.DB.View(m.Ctx, func(tx kv.Tx) error {
		for i := uint64(0); i <= rawdb.ReadCurrentHeader(tx).Number.Uint64(); i++ {
			block := rawdb.ReadHeaderByNumber(tx, i)

			receiptsFromBlock, err := api.GetBlockReceiptsByBlockHash(context.Background(), block.Hash())
			if err != nil {
				return err
			}

			a, _ := json.Marshal(receiptsFromBlock)
			assert.JSONEq(t, expect[block.Number.Uint64()], string(a))
		}
		return nil
	})

	require.NoError(t, err)
}

// newTestBackend creates a chain with a number of explicitly defined blocks and
// wraps it into a mock backend.
func mockWithGenerator(t *testing.T, blocks int, generator func(int, *core.BlockGen)) *mock.MockSentry {
	m := mock.MockWithGenesis(t, &types.Genesis{
		Config: chain.TestChainConfig,
		Alloc:  types.GenesisAlloc{testAddr: {Balance: big.NewInt(1000000)}},
	}, testKey, false)
	if blocks > 0 {
		chain, _ := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, blocks, generator)
		err := m.InsertChain(chain)
		require.NoError(t, err)
	}
	return m
}
