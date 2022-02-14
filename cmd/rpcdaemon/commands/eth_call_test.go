package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

func TestEstimateGas(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	if _, err := api.EstimateGas(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, nil); err != nil {
		t.Errorf("calling EstimateGas: %v", err)
	}
}

func TestEthCallNonCanonical(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	if _, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, rpc.BlockNumberOrHashWithHash(common.HexToHash("0x3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b"), true), nil); err != nil {
		if fmt.Sprintf("%v", err) != "hash 3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b is not currently canonical" {
			t.Errorf("wrong error: %v", err)
		}
	}
}

func TestGetBlockByTimeStampLatestTime(t *testing.T) {
	ctx := context.Background()
	db := rpcdaemontest.CreateTestKV(t)

	tx, err := db.BeginRo(ctx)
	if err != nil {
		t.Errorf("fail at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil)

	latestBlock := rawdb.ReadCurrentBlock(tx)
	latestBlockTimeStamp := latestBlock.Header().Time

	block, err := api.GetBlockByTimeStamp(ctx, latestBlockTimeStamp)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block.Header().Time != latestBlockTimeStamp || block.Hash() != latestBlock.Hash() {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected time stamp: %d\nblock hash retrieved: %s time samp retrieve: %d", latestBlock.Hash(), latestBlockTimeStamp, block.Hash(), block.Header().Time)
	}
}

func TestGetBlockByTimeStampOldestTime(t *testing.T) {
	ctx := context.Background()
	db := rpcdaemontest.CreateTestKV(t)

	tx, err := db.BeginRo(ctx)
	if err != nil {
		t.Errorf("failed at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil)

	oldestBlock, err := rawdb.ReadBlockByNumber(tx, 0)
	if err != nil {
		t.Error("couldn't retrieve oldest block")
	}
	oldestBlockTimeStamp := oldestBlock.Header().Time

	block, err := api.GetBlockByTimeStamp(ctx, oldestBlockTimeStamp)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block.Header().Time != 0 || block.Hash() != oldestBlock.Hash() {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected time stamp: %d\nblock hash retrieved: %s time samp retrieve: %d", oldestBlock.Hash(), oldestBlockTimeStamp, block.Hash(), block.Header().Time)
	}
}

func TestGetBlockByTimeHigherThanLatestBlock(t *testing.T) {
	ctx := context.Background()
	db := rpcdaemontest.CreateTestKV(t)

	tx, err := db.BeginRo(ctx)
	if err != nil {
		t.Errorf("fail at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil)

	latestBlock := rawdb.ReadCurrentBlock(tx)
	latestBlockTimeStamp := latestBlock.Header().Time

	block, err := api.GetBlockByTimeStamp(ctx, latestBlockTimeStamp+999999999999)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block.Header().Time != latestBlockTimeStamp || block.Hash() != latestBlock.Hash() {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected time stamp: %d\nblock hash retrieved: %s time samp retrieve: %d", latestBlock.Hash(), latestBlockTimeStamp, block.Hash(), block.Header().Time)
	}
}

func TestGetBlockByTimeMiddle(t *testing.T) {
	ctx := context.Background()
	db := rpcdaemontest.CreateTestKV(t)

	tx, err := db.BeginRo(ctx)
	if err != nil {
		t.Errorf("fail at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil)

	currentHeader := rawdb.ReadCurrentHeader(tx)
	oldestHeader := rawdb.ReadHeaderByNumber(tx, 0)

	middleNumber := (currentHeader.Number.Uint64() + oldestHeader.Number.Uint64()) / 2
	middleBlock, err := rawdb.ReadBlockByNumber(tx, middleNumber)
	if err != nil {
		t.Error("couldn't retrieve middle block")
	}

	middleTimeStamp := middleBlock.Header().Time

	block, err := api.GetBlockByTimeStamp(ctx, middleTimeStamp)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block.Header().Time != middleTimeStamp || block.Hash() != middleBlock.Hash() {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected time stamp: %d\nblock hash retrieved: %s time samp retrieve: %d", middleBlock.Hash(), middleTimeStamp, block.Hash(), block.Header().Time)
	}
}

func TestGetBlockByTimeStamp(t *testing.T) {
	ctx := context.Background()
	db := rpcdaemontest.CreateTestKV(t)

	tx, err := db.BeginRo(ctx)
	if err != nil {
		t.Errorf("fail at beginning tx")
	}
	defer tx.Rollback()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewErigonAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil)

	highestBlockNumber := rawdb.ReadCurrentHeader(tx).Number
	pickedBlock, err := rawdb.ReadBlockByNumber(tx, highestBlockNumber.Uint64()/3)
	if err != nil {
		t.Errorf("couldn't get block %v", pickedBlock.Number())
	}

	if pickedBlock == nil {
		t.Error("couldn't retrieve picked block")
	}

	pickedBlockTimeStamp := pickedBlock.Header().Time

	block, err := api.GetBlockByTimeStamp(ctx, pickedBlockTimeStamp)
	if err != nil {
		t.Errorf("couldn't retrieve block %v", err)
	}

	if block.Header().Time != pickedBlockTimeStamp || block.Hash() != pickedBlock.Hash() {
		t.Errorf("Retrieved the wrong block.\nexpected block hash: %s expected time stamp: %d\nblock hash retrieved: %s time samp retrieve: %d", pickedBlock.Hash(), pickedBlockTimeStamp, block.Hash(), block.Header().Time)
	}
}
