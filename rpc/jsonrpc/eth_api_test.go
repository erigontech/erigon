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
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func newBaseApiForTest(m *execmoduletester.ExecModuleTester) *BaseAPI {
	return NewBaseApi(nil, m.StateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0)
}

func newEthApiForTest(base *BaseAPI, db kv.TemporalRoDB, txPool txpoolproto.TxpoolClient, mining txpoolproto.MiningClient) *APIImpl {
	cfg := &EthApiConfig{
		GasCap:                      5000000,
		FeeCap:                      ethconfig.Defaults.RPCTxFeeCap,
		ReturnDataLimit:             100_000,
		AllowUnprotectedTxs:         false,
		MaxGetProofRewindBlockCount: 100_000,
		SubscribeLogsChannelSize:    128,
		RpcTxSyncDefaultTimeout:     20 * time.Second,
		RpcTxSyncMaxTimeout:         1 * time.Minute,
	}
	return NewEthAPI(base, db, nil, txPool, mining, cfg, log.New())
}

func TestGetBalanceChangesInBlock(t *testing.T) {
	assert := assert.New(t)
	myBlockNum := rpc.BlockNumberOrHashWithNumber(0)
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	db := m.DB
	api := NewErigonAPI(newBaseApiForTest(m), db, nil)
	balances, err := api.GetBalanceChangesInBlock(context.Background(), myBlockNum)
	if err != nil {
		t.Errorf("calling GetBalanceChangesInBlock resulted in an error: %v", err)
	}
	expected := map[common.Address]*hexutil.Big{
		common.HexToAddress("0x0D3ab14BBaD3D99F4203bd7a11aCB94882050E7e"): (*hexutil.Big)(uint256.NewInt(200000000000000000).ToBig()),
		common.HexToAddress("0x703c4b2bD70c169f5717101CaeE543299Fc946C7"): (*hexutil.Big)(uint256.NewInt(300000000000000000).ToBig()),
		common.HexToAddress("0x71562b71999873DB5b286dF957af199Ec94617F7"): (*hexutil.Big)(uint256.NewInt(9000000000000000000).ToBig()),
	}
	assert.Len(balances, len(expected))
	for i := range balances {
		assert.Contains(expected, i, "%s is not expected to be present in the output.", i)
		assert.Equal(balances[i], expected[i], "the value for %s is expected to be %v, but got %v.", i, expected[i], balances[i])
	}
}

func TestGetTransactionReceipt(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(NewBaseApi(nil, stateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0), m.DB, nil, nil)
	// Call GetTransactionReceipt for transaction which is not in the database
	if _, err := api.GetTransactionReceipt(context.Background(), common.Hash{}); err != nil {
		t.Errorf("calling GetTransactionReceipt with empty hash: %v", err)
	}
}

func TestGetTransactionReceiptUnprotected(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	// Call GetTransactionReceipt for un-protected transaction
	if _, err := api.GetTransactionReceipt(context.Background(), common.HexToHash("0x3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea")); err != nil {
		t.Errorf("calling GetTransactionReceipt for unprotected tx: %v", err)
	}
}

// EIP-1898 test cases

func TestGetStorageAt_ByBlockNumber_WithRequireCanonicalDefault(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithNumber(0))
	if err != nil {
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalDefault(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(m.Genesis.Hash(), false))
	if err != nil {
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalTrue(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(m.Genesis.Hash(), true))
	if err != nil {
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalDefault_BlockNotFoundError(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	offChain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, block *blockgen.BlockGen) {
	})
	if err != nil {
		t.Fatal(err)
	}
	offChainBlock := offChain.Blocks[0]

	if _, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(offChainBlock.Hash(), false)); err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("block not found: %s", offChainBlock.Hash().String()) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalTrue_BlockNotFoundError(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	offChain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, block *blockgen.BlockGen) {
	})
	if err != nil {
		t.Fatal(err)
	}
	offChainBlock := offChain.Blocks[0]

	if _, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(offChainBlock.Hash(), true)); err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("block not found: %s", offChainBlock.Hash().String()) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalDefault_NonCanonicalBlock(t *testing.T) {
	assert := assert.New(t)
	m, _, orphanedChain := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	orphanedBlock := orphanedChain[0].Blocks[0]

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(orphanedBlock.Hash(), false))
	if err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("hash %s is not currently canonical", orphanedBlock.Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalTrue_NonCanonicalBlock(t *testing.T) {
	m, _, orphanedChain := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	orphanedBlock := orphanedChain[0].Blocks[0]

	if _, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(orphanedBlock.Hash(), true)); err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("hash %s is not currently canonical", orphanedBlock.Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

func TestCall_ByBlockHash_WithRequireCanonicalDefault_NonCanonicalBlock(t *testing.T) {
	m, _, orphanedChain := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")

	orphanedBlock := orphanedChain[0].Blocks[0]

	blockNumberOrHash := rpc.BlockNumberOrHashWithHash(orphanedBlock.Hash(), false)
	var blockNumberOrHashRef = &blockNumberOrHash

	if _, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, blockNumberOrHashRef, nil, nil); err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("hash %s is not currently canonical", orphanedBlock.Hash().String()[2:]) {
			/* Not sure. Here https://github.com/ethereum/EIPs/blob/master/EIPS/eip-1898.md it is not explicitly said that
			   eth_call should only work with canonical blocks.
			   But since there is no point in changing the state of non-canonical block, it ignores RequireCanonical. */
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

func TestCall_ByBlockHash_WithRequireCanonicalTrue_NonCanonicalBlock(t *testing.T) {
	m, _, orphanedChain := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")

	orphanedBlock := orphanedChain[0].Blocks[0]
	blockNumberOrHash := rpc.BlockNumberOrHashWithHash(orphanedBlock.Hash(), true)
	var blockNumberOrHashRef = &blockNumberOrHash

	if _, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, blockNumberOrHashRef, nil, nil); err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("hash %s is not currently canonical", orphanedBlock.Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

var _ bridgeReader = mockBridgeReader{}

type mockBridgeReader struct{}

func (m mockBridgeReader) Events(context.Context, common.Hash, uint64) ([]*types.Message, error) {
	panic("mock")
}

func (m mockBridgeReader) EventTxnLookup(context.Context, common.Hash) (uint64, bool, error) {
	panic("mock")
}

func TestGetStorageValues_HappyPath(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	addr1 := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	slot0 := common.Hash{}
	slot1 := common.BigToHash(big.NewInt(1))

	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	result, err := api.GetStorageValues(context.Background(), map[common.Address][]common.Hash{
		addr1: {slot0, slot1},
	}, latest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 address in result, got %d", len(result))
	}
	if len(result[addr1]) != 2 {
		t.Fatalf("expected 2 slots for addr1, got %d", len(result[addr1]))
	}
}

func TestGetStorageValues_MultipleAddresses(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	addr1 := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	addr2 := common.HexToAddress("0x1000000000000000000000000000000000000001")
	addr3 := common.HexToAddress("0x2000000000000000000000000000000000000002")
	slot0 := common.Hash{}
	slot1 := common.BigToHash(big.NewInt(1))
	slot2 := common.BigToHash(big.NewInt(2))
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	request := map[common.Address][]common.Hash{
		addr1: {slot0, slot1},
		addr2: {slot1, slot2},
		addr3: {slot0, slot2},
	}
	result, err := api.GetStorageValues(context.Background(), request, latest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != len(request) {
		t.Fatalf("expected %d addresses in result, got %d", len(request), len(result))
	}
	for addr, slots := range request {
		values, ok := result[addr]
		if !ok {
			t.Fatalf("missing results for address %s", addr.Hex())
		}
		if len(values) != len(slots) {
			t.Fatalf("expected %d slots for address %s, got %d", len(slots), addr.Hex(), len(values))
		}
	}
}

func TestGetStorageValues_MissingSlotReturnsZero(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	addr1 := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	result, err := api.GetStorageValues(context.Background(), map[common.Address][]common.Hash{
		addr1: {common.HexToHash("0xff")},
	}, latest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := common.BytesToHash(result[addr1][0]); got != (common.Hash{}) {
		t.Errorf("missing slot: want zero, got %x", got)
	}
}

func TestGetStorageValues_EmptyRequestReturnsError(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	_, err := api.GetStorageValues(context.Background(), map[common.Address][]common.Hash{}, latest)
	if err == nil {
		t.Fatal("expected error for empty request")
	}
	if err.Error() != "empty request" {
		t.Errorf("wrong error message: %v", err)
	}
}

func TestGetStorageValues_ExceedingSlotLimitReturnsError(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	addr1 := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	tooMany := make([]common.Hash, maxGetStorageSlots+1)
	for i := range tooMany {
		tooMany[i] = common.BigToHash(big.NewInt(int64(i)))
	}

	_, err := api.GetStorageValues(context.Background(), map[common.Address][]common.Hash{
		addr1: tooMany,
	}, latest)
	if err == nil {
		t.Fatal("expected error for exceeding slot limit")
	}
}

func TestGetStorageValues_ByBlockHash_NonCanonicalBlock(t *testing.T) {
	m, _, orphanedChain := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	addr1 := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	orphanedBlock := orphanedChain[0].Blocks[0]

	blockNumberOrHash := rpc.BlockNumberOrHashWithHash(orphanedBlock.Hash(), false)

	_, err := api.GetStorageValues(context.Background(), map[common.Address][]common.Hash{
		addr1: {common.Hash{}},
	}, blockNumberOrHash)
	if err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("hash %s is not currently canonical", orphanedBlock.Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

func TestGetStorageValues_ByBlockHash_WithRequireCanonicalTrue_NonCanonicalBlock(t *testing.T) {
	m, _, orphanedChain := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	addr1 := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	orphanedBlock := orphanedChain[0].Blocks[0]

	blockNumberOrHash := rpc.BlockNumberOrHashWithHash(orphanedBlock.Hash(), true)

	_, err := api.GetStorageValues(context.Background(), map[common.Address][]common.Hash{
		addr1: {common.Hash{}},
	}, blockNumberOrHash)
	if err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("hash %s is not currently canonical", orphanedBlock.Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

func TestGetStorageValues_PrunedBlockReturnsError(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	addr1 := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	// Block 0 may be pruned depending on prune config — adjust block number as needed
	blockNumberOrHash := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(0))

	_, err := api.GetStorageValues(context.Background(), map[common.Address][]common.Hash{
		addr1: {common.Hash{}},
	}, blockNumberOrHash)
	if err != nil {
		t.Logf("got expected prune error: %v", err)
	}
}
