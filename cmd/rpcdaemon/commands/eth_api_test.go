package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
)

func TestGetTransactionReceipt(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	// Call GetTransactionReceipt for transaction which is not in the database
	if _, err := api.GetTransactionReceipt(context.Background(), common.Hash{}); err != nil {
		t.Errorf("calling GetTransactionReceipt with empty hash: %v", err)
	}
}

func TestGetTransactionReceiptUnprotected(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	// Call GetTransactionReceipt for un-protected transaction
	if _, err := api.GetTransactionReceipt(context.Background(), common.HexToHash("0x3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea")); err != nil {
		t.Errorf("calling GetTransactionReceipt for unprotected tx: %v", err)
	}
}

// EIP-1898 test cases

func TestGetStorageAt_ByBlockNumber_WithRequireCanonicalDefault(t *testing.T) {
	assert := assert.New(t)
	db := rpcdaemontest.CreateTestKV(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithNumber(0))
	if err != nil {
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalDefault(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(m.Genesis.Hash(), false))
	if err != nil {
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalTrue(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(m.Genesis.Hash(), true))
	if err != nil {
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalDefault_BlockNotFoundError(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	offChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, block *core.BlockGen) {
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	offChainBlock := offChain.Blocks[0]

	if _, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(offChainBlock.Hash(), false)); err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("block %s not found", offChainBlock.Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalTrue_BlockNotFoundError(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	offChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, block *core.BlockGen) {
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	offChainBlock := offChain.Blocks[0]

	if _, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(offChainBlock.Hash(), true)); err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("block %s not found", offChainBlock.Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}

func TestGetStorageAt_ByBlockHash_WithRequireCanonicalDefault_NonCanonicalBlock(t *testing.T) {
	assert := assert.New(t)
	m, _, orphanedChain := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
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
	m, _, orphanedChain := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
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
	m, _, orphanedChain := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")

	orphanedBlock := orphanedChain[0].Blocks[0]

	if _, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, rpc.BlockNumberOrHashWithHash(orphanedBlock.Hash(), false), nil); err != nil {
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
	m, _, orphanedChain := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")

	orphanedBlock := orphanedChain[0].Blocks[0]

	if _, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, rpc.BlockNumberOrHashWithHash(orphanedBlock.Hash(), true), nil); err != nil {
		if fmt.Sprintf("%v", err) != fmt.Sprintf("hash %s is not currently canonical", orphanedBlock.Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	} else {
		t.Error("error expected")
	}
}
