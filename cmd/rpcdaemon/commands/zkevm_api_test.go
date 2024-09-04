package commands

import (
	"context"
	"math/big"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/stretchr/testify/assert"
)

var (
	key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	address  = crypto.PubkeyToAddress(key.PublicKey)
	address1 = crypto.PubkeyToAddress(key1.PublicKey)
	address2 = crypto.PubkeyToAddress(key2.PublicKey)
	gspec    = &types.Genesis{
		Config: params.TestChainConfig,
		Alloc: types.GenesisAlloc{
			address:  {Balance: big.NewInt(9000000000000000000)},
			address1: {Balance: big.NewInt(200000000000000000)},
			address2: {Balance: big.NewInt(300000000000000000)},
		},
		GasLimit: 10000000,
	}
	chainID = big.NewInt(1337)
	ctx     = context.Background()

	addr1BalanceCheck = "70a08231" + "000000000000000000000000" + address1.Hex()[2:]
	addr2BalanceCheck = "70a08231" + "000000000000000000000000" + address2.Hex()[2:]
	transferAddr2     = "70a08231" + "000000000000000000000000" + address1.Hex()[2:] + "0000000000000000000000000000000000000000000000000000000000000064"
)

func TestLatestConsolidatedBlockNumber(t *testing.T) {
	assert := assert.New(t)
	////////////////
	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	contractBackend.Commit()
	///////////

	db := contractBackend.DB()
	agg := contractBackend.Agg()

	baseApi := NewBaseApi(nil, stateCache, contractBackend.BlockReader(), agg, false, rpccfg.DefaultEvmCallTimeout, contractBackend.Engine(), datadir.New(t.TempDir()))
	ethImpl := NewEthAPI(baseApi, db, nil, nil, nil, 5000000, 100_000, &ethconfig.Defaults)
	var l1Syncer *syncer.L1Syncer
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	for i := 1; i <= 10; i++ {
		hDB.WriteBlockBatch(uint64(i), 1)
	}
	err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, 1)
	assert.NoError(err)
	tx.Commit()
	blockNumber, err := zkEvmImpl.ConsolidatedBlockNumber(ctx)
	assert.NoError(err)
	t.Log("blockNumber: ", blockNumber)

	var expectedL2BlockNumber hexutil.Uint64 = 10
	assert.Equal(expectedL2BlockNumber, blockNumber)
}

func TestIsBlockConsolidated(t *testing.T) {
	assert := assert.New(t)
	////////////////
	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	contractBackend.Commit()
	///////////

	db := contractBackend.DB()
	agg := contractBackend.Agg()

	baseApi := NewBaseApi(nil, stateCache, contractBackend.BlockReader(), agg, false, rpccfg.DefaultEvmCallTimeout, contractBackend.Engine(), datadir.New(t.TempDir()))
	ethImpl := NewEthAPI(baseApi, db, nil, nil, nil, 5000000, 100_000, &ethconfig.Defaults)
	var l1Syncer *syncer.L1Syncer
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	isConsolidated, err := zkEvmImpl.IsBlockConsolidated(ctx, 11)
	assert.NoError(err)
	t.Logf("blockNumber: 11 -> %v", isConsolidated)
	assert.False(isConsolidated)
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	for i := 1; i <= 10; i++ {
		err := hDB.WriteBlockBatch(uint64(i), 1)
		assert.NoError(err)
	}
	err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, 1)
	assert.NoError(err)
	tx.Commit()
	for i := 1; i <= 10; i++ {
		isConsolidated, err := zkEvmImpl.IsBlockConsolidated(ctx, rpc.BlockNumber(i))
		assert.NoError(err)
		t.Logf("blockNumber: %d -> %v", i, isConsolidated)
		assert.True(isConsolidated)
	}
	isConsolidated, err = zkEvmImpl.IsBlockConsolidated(ctx, 11)
	assert.NoError(err)
	t.Logf("blockNumber: 11 -> %v", isConsolidated)
	assert.False(isConsolidated)
}

func TestIsBlockVirtualized(t *testing.T) {
	assert := assert.New(t)
	////////////////
	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	contractBackend.Commit()
	///////////

	db := contractBackend.DB()
	agg := contractBackend.Agg()

	baseApi := NewBaseApi(nil, stateCache, contractBackend.BlockReader(), agg, false, rpccfg.DefaultEvmCallTimeout, contractBackend.Engine(), datadir.New(t.TempDir()))
	ethImpl := NewEthAPI(baseApi, db, nil, nil, nil, 5000000, 100_000, &ethconfig.Defaults)
	var l1Syncer *syncer.L1Syncer
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	isVirtualized, err := zkEvmImpl.IsBlockVirtualized(ctx, 50)
	assert.NoError(err)
	t.Logf("blockNumber: 50 -> %v", isVirtualized)
	assert.False(isVirtualized)
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	for i := 1; i <= 10; i++ {
		err := hDB.WriteBlockBatch(uint64(i), 1)
		assert.NoError(err)
		err = hDB.WriteBlockBatch(uint64(i+10), 2)
		assert.NoError(err)
		err = hDB.WriteBlockBatch(uint64(i+20), 3)
		assert.NoError(err)
		err = hDB.WriteBlockBatch(uint64(i+30), 4)
		assert.NoError(err)
	}
	err = hDB.WriteSequence(1, 4, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e0"))
	assert.NoError(err)
	tx.Commit()
	for i := 1; i <= 40; i++ {
		isVirtualized, err := zkEvmImpl.IsBlockVirtualized(ctx, rpc.BlockNumber(i))
		assert.NoError(err)
		t.Logf("blockNumber: %d -> %v", i, isVirtualized)
		assert.True(isVirtualized)
	}
	isVirtualized, err = zkEvmImpl.IsBlockVirtualized(ctx, 50)
	assert.NoError(err)
	t.Logf("blockNumber: 50 -> %v", isVirtualized)
	assert.False(isVirtualized)
}

func TestBatchNumberByBlockNumber(t *testing.T) {
	assert := assert.New(t)
	////////////////
	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	contractBackend.Commit()
	///////////

	db := contractBackend.DB()
	agg := contractBackend.Agg()

	baseApi := NewBaseApi(nil, stateCache, contractBackend.BlockReader(), agg, false, rpccfg.DefaultEvmCallTimeout, contractBackend.Engine(), datadir.New(t.TempDir()))
	ethImpl := NewEthAPI(baseApi, db, nil, nil, nil, 5000000, 100_000, &ethconfig.Defaults)
	var l1Syncer *syncer.L1Syncer
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	batchNumber, err := zkEvmImpl.BatchNumberByBlockNumber(ctx, rpc.BlockNumber(10))
	assert.Error(err)
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	for i := 0; i < 10; i++ {
		err := hDB.WriteBlockBatch(uint64(i), 1)
		assert.NoError(err)
		err = hDB.WriteBlockBatch(uint64(i+10), 2)
		assert.NoError(err)
		err = hDB.WriteBlockBatch(uint64(i+20), 3)
		assert.NoError(err)
		err = hDB.WriteBlockBatch(uint64(i+30), 4)
		assert.NoError(err)
	}
	tx.Commit()
	for i := 0; i < 40; i++ {
		batchNumber, err := zkEvmImpl.BatchNumberByBlockNumber(ctx, rpc.BlockNumber(i))
		assert.NoError(err)
		t.Log("i/10: ", i/10)
		if i/10 < 1 {
			assert.Equal(hexutil.Uint64(1), batchNumber)
		} else if i/10 == 1 {
			assert.Equal(hexutil.Uint64(2), batchNumber)
		} else if i/10 == 2 {
			assert.Equal(hexutil.Uint64(3), batchNumber)
		} else if i/10 == 3 {
			assert.Equal(hexutil.Uint64(4), batchNumber)
		} else {
			panic("batch out of range")
		}
	}
	batchNumber, err = zkEvmImpl.BatchNumberByBlockNumber(ctx, rpc.BlockNumber(40))
	assert.Error(err)
	batchNumber, err = zkEvmImpl.BatchNumberByBlockNumber(ctx, rpc.BlockNumber(50))
	assert.Error(err)
	t.Log("batchNumber", batchNumber)
}

func TestBatchNumber(t *testing.T) {
	assert := assert.New(t)
	////////////////
	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	contractBackend.Commit()
	///////////

	db := contractBackend.DB()
	agg := contractBackend.Agg()

	baseApi := NewBaseApi(nil, stateCache, contractBackend.BlockReader(), agg, false, rpccfg.DefaultEvmCallTimeout, contractBackend.Engine(), datadir.New(t.TempDir()))
	ethImpl := NewEthAPI(baseApi, db, nil, nil, nil, 5000000, 100_000, &ethconfig.Defaults)
	var l1Syncer *syncer.L1Syncer
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	for i := 1; i <= 10; i++ {
		err := hDB.WriteBlockBatch(uint64(i), uint64(i))
		assert.NoError(err)
	}
	err = hDB.WriteSequence(4, 4, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e0"))
	assert.NoError(err)
	err = hDB.WriteSequence(7, 7, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba86"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e1"))
	assert.NoError(err)
	for i:=1; i<=4; i++ {
		err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, uint64(i))
		assert.NoError(err)
	}
	tx.Commit()
	batchNumber, err := zkEvmImpl.BatchNumber(ctx)
	assert.NoError(err)
	assert.Equal(hexutil.Uint64(10), batchNumber)
}

func TestVirtualBatchNumber(t *testing.T) {
	assert := assert.New(t)
	////////////////
	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	contractBackend.Commit()
	///////////

	db := contractBackend.DB()
	agg := contractBackend.Agg()

	baseApi := NewBaseApi(nil, stateCache, contractBackend.BlockReader(), agg, false, rpccfg.DefaultEvmCallTimeout, contractBackend.Engine(), datadir.New(t.TempDir()))
	ethImpl := NewEthAPI(baseApi, db, nil, nil, nil, 5000000, 100_000, &ethconfig.Defaults)
	var l1Syncer *syncer.L1Syncer
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	for i := 1; i <= 10; i++ {
		err := hDB.WriteBlockBatch(uint64(i), uint64(i))
		assert.NoError(err)
	}
	err = hDB.WriteSequence(4, 4, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e0"))
	assert.NoError(err)
	err = hDB.WriteSequence(7, 7, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba86"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e1"))
	assert.NoError(err)
	for i:=1; i<=4; i++ {
		err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, uint64(i))
		assert.NoError(err)
	}
	tx.Commit()
	virtualBatchNumber, err := zkEvmImpl.VirtualBatchNumber(ctx)
	assert.NoError(err)
	assert.Equal(hexutil.Uint64(7), virtualBatchNumber)
}

func TestVerifiedBatchNumber(t *testing.T) {
	assert := assert.New(t)
	////////////////
	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	contractBackend.Commit()
	///////////

	db := contractBackend.DB()
	agg := contractBackend.Agg()

	baseApi := NewBaseApi(nil, stateCache, contractBackend.BlockReader(), agg, false, rpccfg.DefaultEvmCallTimeout, contractBackend.Engine(), datadir.New(t.TempDir()))
	ethImpl := NewEthAPI(baseApi, db, nil, nil, nil, 5000000, 100_000, &ethconfig.Defaults)
	var l1Syncer *syncer.L1Syncer
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	for i := 1; i <= 10; i++ {
		err := hDB.WriteBlockBatch(uint64(i), uint64(i))
		assert.NoError(err)
	}
	err = hDB.WriteSequence(4, 4, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e0"))
	assert.NoError(err)
	err = hDB.WriteSequence(7, 7, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba86"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e1"))
	assert.NoError(err)
	for i:=1; i<=4; i++ {
		err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, uint64(i))
		assert.NoError(err)
	}
	tx.Commit()
	verifiedBatchNumber, err := zkEvmImpl.VerifiedBatchNumber(ctx)
	assert.NoError(err)
	assert.Equal(hexutil.Uint64(4), verifiedBatchNumber)
}