package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
)

func TestGetTransactionReceipt(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	// Call GetTransactionReceipt for transaction which is not in the database
	if _, err := api.GetTransactionReceipt(context.Background(), common.Hash{}); err != nil {
		t.Errorf("calling GetTransactionReceipt with empty hash: %v", err)
	}
}

func TestGetTransactionReceiptUnprotected(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	// Call GetTransactionReceipt for un-protected transaction
	if _, err := api.GetTransactionReceipt(context.Background(), common.HexToHash("0x3f3cb8a0e13ed2481f97f53f7095b9cbc78b6ffb779f2d3e565146371a8830ea")); err != nil {
		t.Errorf("calling GetTransactionReceipt for unprotected tx: %v", err)
	}
}

// EIP-1898 test cases

func TestGetStorageAtZeroWithRequireCanonicalDefault(t *testing.T) {
	assert := assert.New(t)
	db := rpcdaemontest.CreateTestKV(t)
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithNumber(0))
	if err != nil { // GetStorageAt
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAtWithRequireCanonicalDefault(t *testing.T) {
	assert := assert.New(t)
	m := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(m.Genesis.Hash(), false))
	if err != nil { // GetStorageAt
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAtWithRequireCanonicalTrue(t *testing.T) {
	assert := assert.New(t)
	m := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(m.Genesis.Hash(), true))
	if err != nil { // GetStorageAt
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAtWithRequireCanonicalDefaultBlockNotFoundError(t *testing.T) {
	m := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	orphanedChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, block *core.BlockGen) {
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.InsertChain(orphanedChain); err != nil {
		t.Fatal(err)
	}

	if _, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(orphanedChain.Blocks[0].Hash(), false)); err != nil { // GetStorageAt
		if fmt.Sprintf("%v", err) != fmt.Sprintf("block %s not found", orphanedChain.Blocks[0].Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	}
}

func TestGetStorageAtWithRequireCanonicalTrueBlockNotFoundError(t *testing.T) {
	m := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	orphanedChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, block *core.BlockGen) {
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.InsertChain(orphanedChain); err != nil {
		t.Fatal(err)
	}

	if _, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(orphanedChain.Blocks[0].Hash(), true)); err != nil { // GetStorageAt
		if fmt.Sprintf("%v", err) != fmt.Sprintf("block %s not found", orphanedChain.Blocks[0].Hash().String()[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	}
}

func TestGetStorageAtWithRequireCanonicalDefaultNonCanonicalBlock(t *testing.T) {
	assert := assert.New(t)
	m := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	blockHash := "0x3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b"

	result, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(common.HexToHash(blockHash), false))
	if err != nil { // GetStorageAt
		t.Errorf("calling GetStorageAt: %v", err)
	}

	assert.Equal(common.HexToHash("0x0").String(), result)
}

func TestGetStorageAtWithRequireCanonicalTrueNonCanonicalBlock(t *testing.T) {
	m := rpcdaemontest.CreateTestSentry(t)
	db := m.DB
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	addr := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")

	blockHash := "0x3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b"

	if _, err := api.GetStorageAt(context.Background(), addr, "0x0", rpc.BlockNumberOrHashWithHash(common.HexToHash(blockHash), true)); err != nil { // GetStorageAt
		if fmt.Sprintf("%v", err) != fmt.Sprintf("hash %s is not currently canonical", blockHash[2:]) {
			t.Errorf("wrong error: %v", err)
		}
	}
}
