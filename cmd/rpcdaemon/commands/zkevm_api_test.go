package commands

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcache"
	"github.com/holiman/uint256"
	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands/mocks"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	rpctypes "github.com/ledgerwatch/erigon/zk/rpcdaemon"
	"github.com/ledgerwatch/erigon/zk/syncer"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
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
	for i := 1; i <= 4; i++ {
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
	for i := 1; i <= 4; i++ {
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
	for i := 1; i <= 4; i++ {
		err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, uint64(i))
		assert.NoError(err)
	}
	tx.Commit()
	verifiedBatchNumber, err := zkEvmImpl.VerifiedBatchNumber(ctx)
	assert.NoError(err)
	assert.Equal(hexutil.Uint64(4), verifiedBatchNumber)
}

func TestGetBatchByNumber(t *testing.T) {
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

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	EthermanMock := mocks.NewMockIEtherman(mockCtrl)

	l1Syncer := syncer.NewL1Syncer(
		ctx,
		[]syncer.IEtherman{EthermanMock},
		[]common.Address{},
		[][]common.Hash{},
		10,
		0,
		"latest",
	)
	cfg := &ethconfig.Defaults
	cfg.Zk.L1RollupId = 1
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	erigonDB := erigon_db.NewErigonDb(tx)

	gers := []common.Hash{common.HexToHash("0xf010e584db63e18e207a2a2a09cfef322b8f8f185df5093ed17794ac365ef60e"), common.HexToHash("0x12021ea011bd6ebffee86ea47e2c3d08e4fe734ba7251f2ddbc9fa648af3b1e6"), common.HexToHash("0x055bbf062f8add981fd54801e5c36d404da37b8300a7babc2bd2585a54a2195a"), common.HexToHash("0x252feef2a0468f334e0efa3ec67ceb04dbe3d64204242b3774ce1850f8042760")}
	parentHashes := []common.Hash{common.HexToHash("0x502b94aa765e198ecd736bcb3ec673e1fcb5985d8e610b1ba06bcf9fbdb965b2"), common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e")}
	hashes := []common.Hash{common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e"), common.HexToHash("0x002241472c8ffeb86cd3c2bfe928cc41a47fdeffa0e98f56c1da3db6d50d29cb")}
	stateRoots := []common.Hash{common.HexToHash("0x70ee58f4d74b706ce88307800983c06c0479f9808d38db5d751d7306f510c9b8"), common.HexToHash("0xba46d17db3364a059cc6efada4a1cc7bea472c559247aafdd920fbd017031fee"), common.HexToHash("0x7dbca3d3f5841bb8a5da985655235587c212826b0f21127e4f3470230d05b0f8"), common.HexToHash("0x551b6fdb2b0c156b104a946f815c3e2c87324be35fba14cf0ed3e4c1287d89bf")}
	txsRoot := []common.Hash{common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")}
	coinBase := common.HexToAddress("0x761d53b47334bee6612c0bd1467fb881435375b2")
	times := []uint64{1714427000, 1714427003, 1714427006, 1714427009}
	gasLimits := []uint64{1125899906842624, 1125899906842624, 1125899906842624, 1125899906842624}
	mainnetExitRoots := []common.Hash{common.HexToHash("0x6d2478612063b2ecb19b1c75dda5add47630bbae42a2e84f7ccd33c1540db1de"), common.HexToHash("0x17aa73f0a1b0e1acd7ec05a686cfc83a746b3230480db81105d571272aff5936"), common.HexToHash("0xf1dcb7fa915388a4a7bac1da56bd37d3590524ad84cbe0ff42d22ec2be8dcb1d"), common.HexToHash("0x63ab7d9f3c87bc4bbcff42748b09ef7cf87e7e084f6d457d277fee13a4759872")}
	rollupExitRoots := []common.Hash{common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0x2b911c2ea39040de58c4ddcc431c62ffde4abf63d36be05b7e5e8724056061b8"), common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0xc8500f8630165b35e61c846262a2ffa3cbe5608305115ec2c79f65bbad91d0b6")}
	txs := [][]types.Transaction{{}, {}, {}, {}}

	for i := 0; i < 4; i++ {
		err := hDB.WriteBlockBatch(uint64(i+1), 1)
		assert.NoError(err)
		err = hDB.WriteGlobalExitRoot(gers[i])
		assert.NoError(err)
		err = hDB.WriteBlockGlobalExitRoot(uint64(i+1), gers[i])
		assert.NoError(err)
		l1InforTree := &zktypes.L1InfoTreeUpdate{
			Index:           uint64(i),
			GER:             gers[i],
			MainnetExitRoot: mainnetExitRoots[i],
			RollupExitRoot:  rollupExitRoots[i],
			ParentHash:      parentHashes[i],
			Timestamp:       times[i],
			BlockNumber:     uint64(i + 1),
		}
		err = hDB.WriteL1InfoTreeUpdateToGer(l1InforTree)
		assert.NoError(err)
	}

	err = hDB.WriteBlockBatch(5, 2)
	assert.NoError(err)
	err = hDB.WriteSequence(4, 1, common.HexToHash("0x22ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba97"), stateRoots[len(stateRoots)-1])
	assert.NoError(err)
	err = hDB.WriteSequence(5, 2, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e0"))
	assert.NoError(err)

	err = hDB.WriteForkId(1, 7)
	assert.NoError(err)

	for i := 1; i <= 2; i++ {
		err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, uint64(i))
		assert.NoError(err)
	}

	for i := 0; i < 4; i++ {
		_, err := erigonDB.WriteHeader(big.NewInt(int64(i+1)), hashes[i], stateRoots[i], txsRoot[i], parentHashes[i], coinBase, times[i], gasLimits[i])
		assert.NoError(err)
		err = erigonDB.WriteBody(big.NewInt(int64(i+1)), hashes[i], txs[i])
		assert.NoError(err)
	}
	tx.Commit()
	var response []byte
	accInputHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	response = append(response, accInputHash.Bytes()...)
	response = append(response, common.Hash{}.Bytes()...)
	response = append(response, common.FromHex(fmt.Sprintf("0x%064x", 1))...)
	EthermanMock.EXPECT().CallContract(ctx, ethereum.CallMsg{
		To:   &common.Address{},
		Data: common.FromHex("0x25280169" + fmt.Sprintf("%064x", 1) + fmt.Sprintf("%064x", 1)),
	}, nil).Return(response, nil).AnyTimes()
	rawBatch, err := zkEvmImpl.GetBatchByNumber(ctx, 1, nil)
	assert.NoError(err)
	var batch *rpctypes.Batch
	err = json.Unmarshal(rawBatch, &batch)
	assert.NoError(err)
	t.Logf("batch: %+v", batch)
	assert.Equal(rpctypes.ArgUint64(1), batch.Number)
	assert.Equal(common.HexToAddress("0x761d53b47334bEe6612c0Bd1467FB881435375B2"), batch.Coinbase)
	assert.Equal(common.HexToHash("0x551b6fdb2b0c156b104a946f815c3e2c87324be35fba14cf0ed3e4c1287d89bf"), batch.StateRoot)
	assert.Equal(gers[len(gers)-1], batch.GlobalExitRoot)
	assert.Equal(mainnetExitRoots[len(mainnetExitRoots)-1], batch.MainnetExitRoot)
	assert.Equal(rollupExitRoots[len(rollupExitRoots)-1], batch.RollupExitRoot)
	assert.Equal(common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"), batch.AccInputHash)
	assert.Equal(common.HexToHash("0x22ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba97"), *batch.SendSequencesTxHash)
	assert.Equal(rpctypes.ArgUint64(1714427009), batch.Timestamp)
	assert.Equal(true, batch.Closed)
	assert.Equal("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec", batch.Blocks[0])
	assert.Equal("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf", batch.Blocks[1])
	assert.Equal("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e", batch.Blocks[2])
	assert.Equal(0, len(batch.Transactions))
	assert.Equal("0x0b66301478000000000b00000003000000000b00000003000000000b0000000300000000", "0x"+hex.EncodeToString(batch.BatchL2Data))
}

func TestGetBatchDataByNumber(t *testing.T) {
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

	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, nil, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	erigonDB := erigon_db.NewErigonDb(tx)

	gers := []common.Hash{common.HexToHash("0xf010e584db63e18e207a2a2a09cfef322b8f8f185df5093ed17794ac365ef60e"), common.HexToHash("0x12021ea011bd6ebffee86ea47e2c3d08e4fe734ba7251f2ddbc9fa648af3b1e6"), common.HexToHash("0x055bbf062f8add981fd54801e5c36d404da37b8300a7babc2bd2585a54a2195a"), common.HexToHash("0x252feef2a0468f334e0efa3ec67ceb04dbe3d64204242b3774ce1850f8042760")}
	parentHashes := []common.Hash{common.HexToHash("0x502b94aa765e198ecd736bcb3ec673e1fcb5985d8e610b1ba06bcf9fbdb965b2"), common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e")}
	hashes := []common.Hash{common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e"), common.HexToHash("0x002241472c8ffeb86cd3c2bfe928cc41a47fdeffa0e98f56c1da3db6d50d29cb")}
	stateRoots := []common.Hash{common.HexToHash("0x70ee58f4d74b706ce88307800983c06c0479f9808d38db5d751d7306f510c9b8"), common.HexToHash("0xba46d17db3364a059cc6efada4a1cc7bea472c559247aafdd920fbd017031fee"), common.HexToHash("0x7dbca3d3f5841bb8a5da985655235587c212826b0f21127e4f3470230d05b0f8"), common.HexToHash("0x551b6fdb2b0c156b104a946f815c3e2c87324be35fba14cf0ed3e4c1287d89bf")}
	txsRoot := []common.Hash{common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")}
	coinBase := common.HexToAddress("0x761d53b47334bee6612c0bd1467fb881435375b2")
	times := []uint64{1714427000, 1714427003, 1714427006, 1714427009}
	gasLimits := []uint64{1125899906842624, 1125899906842624, 1125899906842624, 1125899906842624}
	mainnetExitRoots := []common.Hash{common.HexToHash("0x6d2478612063b2ecb19b1c75dda5add47630bbae42a2e84f7ccd33c1540db1de"), common.HexToHash("0x17aa73f0a1b0e1acd7ec05a686cfc83a746b3230480db81105d571272aff5936"), common.HexToHash("0xf1dcb7fa915388a4a7bac1da56bd37d3590524ad84cbe0ff42d22ec2be8dcb1d"), common.HexToHash("0x63ab7d9f3c87bc4bbcff42748b09ef7cf87e7e084f6d457d277fee13a4759872")}
	rollupExitRoots := []common.Hash{common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0x2b911c2ea39040de58c4ddcc431c62ffde4abf63d36be05b7e5e8724056061b8"), common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0xc8500f8630165b35e61c846262a2ffa3cbe5608305115ec2c79f65bbad91d0b6")}
	txs := [][]types.Transaction{{}, {}, {}, {}}

	for i := 0; i < 4; i++ {
		err := hDB.WriteBlockBatch(uint64(i+1), 1)
		assert.NoError(err)
		err = hDB.WriteGlobalExitRoot(gers[i])
		assert.NoError(err)
		err = hDB.WriteBlockGlobalExitRoot(uint64(i+1), gers[i])
		assert.NoError(err)
		l1InforTree := &zktypes.L1InfoTreeUpdate{
			Index:           uint64(i),
			GER:             gers[i],
			MainnetExitRoot: mainnetExitRoots[i],
			RollupExitRoot:  rollupExitRoots[i],
			ParentHash:      parentHashes[i],
			Timestamp:       times[i],
			BlockNumber:     uint64(i + 1),
		}
		err = hDB.WriteL1InfoTreeUpdateToGer(l1InforTree)
		assert.NoError(err)
	}

	err = hDB.WriteSequence(4, 1, common.HexToHash("0x22ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba97"), stateRoots[len(stateRoots)-1])
	assert.NoError(err)
	err = hDB.WriteSequence(5, 2, common.HexToHash("0x21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85"), common.HexToHash("0xcefad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e0"))
	assert.NoError(err)
	err = hDB.WriteSequence(6, 3, common.HexToHash("0x20ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba74"), common.HexToHash("0x10fad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e1"))
	assert.NoError(err)
	err = hDB.WriteSequence(7, 4, common.HexToHash("0x19ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba63"), common.HexToHash("0x20fad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e2"))
	assert.NoError(err)
	err = hDB.WriteSequence(8, 5, common.HexToHash("0x18ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba52"), common.HexToHash("0x30fad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e3"))
	assert.NoError(err)
	err = hDB.WriteSequence(9, 6, common.HexToHash("0x17ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba41"), common.HexToHash("0x40fad4e508c098b9a7e1d8feb19955fb02ba9675585078710969d3440f5054e4"))
	assert.NoError(err)
	for i := 0; i < 5; i++ {
		err = hDB.WriteBlockBatch(uint64(i+5), uint64(i+2))
		assert.NoError(err)
	}

	for i:=1; i<7; i++ {
		err = hDB.WriteForkId(uint64(i), 7)
		assert.NoError(err)
	}

	for i := 1; i <= 2; i++ {
		err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, uint64(i))
		assert.NoError(err)
	}
	err = stages.SaveStageProgress(tx, stages.Execution, 9)
	assert.NoError(err)
	err = rawdb.WriteCanonicalHash(tx, common.Hash{}, 9)
	assert.NoError(err)

	for i := 0; i < 4; i++ {
		_, err := erigonDB.WriteHeader(big.NewInt(int64(i+1)), hashes[i], stateRoots[i], txsRoot[i], parentHashes[i], coinBase, times[i], gasLimits[i])
		assert.NoError(err)
		err = erigonDB.WriteBody(big.NewInt(int64(i+1)), hashes[i], txs[i])
		assert.NoError(err)
	}
	_, err = erigonDB.WriteHeader(big.NewInt(8), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba01"), common.HexToHash("0x57ddb9a336815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7bf47"), common.HexToHash("0x67ddb9a356813c3f4c1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba57"), common.HexToHash("0x87ddb9a356812c3fac1026b6dec5df31245fbadb485c9ba5a3e3398a04b7ba68"), coinBase, 1714427021, 1125899906842624)
	assert.NoError(err)
	err = erigonDB.WriteBody(big.NewInt(8), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba01"), []types.Transaction{})
	assert.NoError(err)
	_, err = erigonDB.WriteHeader(big.NewInt(9), common.HexToHash("0x27ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba81"), common.HexToHash("0x37ddb9a336815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba43"), common.HexToHash("0x87ddb9a356815c3f4c1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba56"), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df31245fbadb485c9ba5a3e3398a04b7ba48"), coinBase, 1714427024, 1125899906842624)
	assert.NoError(err)
	err = erigonDB.WriteBody(big.NewInt(9), common.HexToHash("0x27ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba81"), []types.Transaction{})
	assert.NoError(err)

	batchesL2Data := [][]byte{common.FromHex("27ae5ba08d7291c96c8cbddcc148bf48a6d68c7974b94356f53754ef6171d75727ae5ba08d7291c96c8cbddcc148bf48a6d68c7974b94356f53754ef6171d757"),
		common.FromHex("28ae5ba08d7291c96c8cbddcc148bf48a6d68c7974b94356f53754ef6171d75727ae5ba08d7291c96c8cbddcc148bf48a6d68c7974b94356f53754ef6171d758"),
		common.FromHex("29ae5ba08d7291c96c8cbddcc148bf48a6d68c7974b94356f53754ef6171d75727ae5ba08d7291c96c8cbddcc148bf48a6d68c7974b94356f53754ef6171d759"),
		common.FromHex("2aae5ba08d7291c96c8cbddcc148bf48a6d68c7974b94356f53754ef6171d75727ae5ba08d7291c96c8cbddcc148bf48a6d68c7974b94356f53754ef6171d75a"),
	}
	for i, bd := range batchesL2Data {
		err = hDB.WriteL1BatchData(uint64(i+2), bd)
		assert.NoError(err)
	}
	tx.Commit()
	rawBatch, err := zkEvmImpl.GetBatchDataByNumbers(ctx, rpc.RpcNumberArray{Numbers: []rpc.BlockNumber{1, 2, 3, 4, 5, 6, 7}})
	assert.NoError(err)
	var bData map[string][]rpctypes.BatchDataSlim
	err = json.Unmarshal(rawBatch, &bData)
	assert.NoError(err)
	batchesData := bData["data"]
	t.Logf("batch: %+v", batchesData)
	for i, b := range batchesData {
		assert.Equal(rpctypes.ArgUint64(i+1), b.Number)
		if i == 0 {
			assert.Equal("0x0b66301478000000000b00000003000000000b00000003000000000b0000000300000000", b.BatchL2Data.Hex(), "Batch 1 doesn't match")
		} else if i <= len(batchesData)-3 {
			assert.Equal("0x"+hex.EncodeToString(batchesL2Data[i-1]), b.BatchL2Data.Hex(), fmt.Sprintf("Batch %d doesn't match", i+1))
		} else if i <= len(batchesData)-2 {
			assert.Equal("0x0b0000000300000000", b.BatchL2Data.Hex(), fmt.Sprintf("Batch %d doesn't match", i+1))
		} else {
			assert.Equal("0x", b.BatchL2Data.Hex(), fmt.Sprintf("Batch %d doesn't match", i+1))
		}
		if i <= len(batchesData)-2 {
			assert.Equal(false, b.Empty, fmt.Sprintf("Batch %d doesn't match", i+1))
		} else {
			assert.Equal(true, b.Empty, fmt.Sprintf("Batch %d doesn't match", i+1))
		}
	}
}

func TestGetExitRootsByGER(t *testing.T) {
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

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	EthermanMock := mocks.NewMockIEtherman(mockCtrl)

	l1Syncer := syncer.NewL1Syncer(
		ctx,
		[]syncer.IEtherman{EthermanMock},
		[]common.Address{},
		[][]common.Hash{},
		10,
		0,
		"latest",
	)
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)

	gers := []common.Hash{common.HexToHash("0xf010e584db63e18e207a2a2a09cfef322b8f8f185df5093ed17794ac365ef60e"), common.HexToHash("0x12021ea011bd6ebffee86ea47e2c3d08e4fe734ba7251f2ddbc9fa648af3b1e6"), common.HexToHash("0x055bbf062f8add981fd54801e5c36d404da37b8300a7babc2bd2585a54a2195a"), common.HexToHash("0x252feef2a0468f334e0efa3ec67ceb04dbe3d64204242b3774ce1850f8042760")}
	parentHashes := []common.Hash{common.HexToHash("0x502b94aa765e198ecd736bcb3ec673e1fcb5985d8e610b1ba06bcf9fbdb965b2"), common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e")}
	stateRoots := []common.Hash{common.HexToHash("0x70ee58f4d74b706ce88307800983c06c0479f9808d38db5d751d7306f510c9b8"), common.HexToHash("0xba46d17db3364a059cc6efada4a1cc7bea472c559247aafdd920fbd017031fee"), common.HexToHash("0x7dbca3d3f5841bb8a5da985655235587c212826b0f21127e4f3470230d05b0f8"), common.HexToHash("0x551b6fdb2b0c156b104a946f815c3e2c87324be35fba14cf0ed3e4c1287d89bf")}
	times := []uint64{1714427000, 1714427003, 1714427006, 1714427009}
	mainnetExitRoots := []common.Hash{common.HexToHash("0x6d2478612063b2ecb19b1c75dda5add47630bbae42a2e84f7ccd33c1540db1de"), common.HexToHash("0x17aa73f0a1b0e1acd7ec05a686cfc83a746b3230480db81105d571272aff5936"), common.HexToHash("0xf1dcb7fa915388a4a7bac1da56bd37d3590524ad84cbe0ff42d22ec2be8dcb1d"), common.HexToHash("0x63ab7d9f3c87bc4bbcff42748b09ef7cf87e7e084f6d457d277fee13a4759872")}
	rollupExitRoots := []common.Hash{common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0x2b911c2ea39040de58c4ddcc431c62ffde4abf63d36be05b7e5e8724056061b8"), common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0xc8500f8630165b35e61c846262a2ffa3cbe5608305115ec2c79f65bbad91d0b6")}

	for i := 0; i < 4; i++ {
		err := hDB.WriteBlockBatch(uint64(i+1), 1)
		assert.NoError(err)
		err = hDB.WriteGlobalExitRoot(gers[i])
		assert.NoError(err)
		err = hDB.WriteBlockGlobalExitRoot(uint64(i+1), gers[i])
		assert.NoError(err)
		l1InforTree := &zktypes.L1InfoTreeUpdate{
			Index:           uint64(i),
			GER:             gers[i],
			MainnetExitRoot: mainnetExitRoots[i],
			RollupExitRoot:  rollupExitRoots[i],
			ParentHash:      parentHashes[i],
			Timestamp:       times[i],
			BlockNumber:     uint64(i + 1),
		}
		err = hDB.WriteL1InfoTreeUpdateToGer(l1InforTree)
		assert.NoError(err)
	}

	err = hDB.WriteSequence(4, 1, common.HexToHash("0x22ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba97"), stateRoots[len(stateRoots)-1])
	assert.NoError(err)

	tx.Commit()
	for i, g := range gers {
		exitRoots, err := zkEvmImpl.GetExitRootsByGER(ctx, g)
		assert.NoError(err)
		t.Logf("exitRoots: %+v", exitRoots)
		assert.Equal(rpctypes.ArgUint64(i+1), exitRoots.BlockNumber)
		assert.Equal(mainnetExitRoots[i], exitRoots.MainnetExitRoot)
		assert.Equal(rollupExitRoots[i], exitRoots.RollupExitRoot)
		assert.Equal(rpctypes.ArgUint64(times[i]), exitRoots.Timestamp)
	}
}

func TestLatestGlobalExitRoot(t *testing.T) {
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

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	EthermanMock := mocks.NewMockIEtherman(mockCtrl)

	l1Syncer := syncer.NewL1Syncer(
		ctx,
		[]syncer.IEtherman{EthermanMock},
		[]common.Address{},
		[][]common.Hash{},
		10,
		0,
		"latest",
	)
	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, l1Syncer, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)

	gers := []common.Hash{common.HexToHash("0xf010e584db63e18e207a2a2a09cfef322b8f8f185df5093ed17794ac365ef60e"), common.HexToHash("0x12021ea011bd6ebffee86ea47e2c3d08e4fe734ba7251f2ddbc9fa648af3b1e6"), common.HexToHash("0x055bbf062f8add981fd54801e5c36d404da37b8300a7babc2bd2585a54a2195a"), common.HexToHash("0x252feef2a0468f334e0efa3ec67ceb04dbe3d64204242b3774ce1850f8042760")}
	parentHashes := []common.Hash{common.HexToHash("0x502b94aa765e198ecd736bcb3ec673e1fcb5985d8e610b1ba06bcf9fbdb965b2"), common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e")}
	stateRoots := []common.Hash{common.HexToHash("0x70ee58f4d74b706ce88307800983c06c0479f9808d38db5d751d7306f510c9b8"), common.HexToHash("0xba46d17db3364a059cc6efada4a1cc7bea472c559247aafdd920fbd017031fee"), common.HexToHash("0x7dbca3d3f5841bb8a5da985655235587c212826b0f21127e4f3470230d05b0f8"), common.HexToHash("0x551b6fdb2b0c156b104a946f815c3e2c87324be35fba14cf0ed3e4c1287d89bf")}
	times := []uint64{1714427000, 1714427003, 1714427006, 1714427009}
	mainnetExitRoots := []common.Hash{common.HexToHash("0x6d2478612063b2ecb19b1c75dda5add47630bbae42a2e84f7ccd33c1540db1de"), common.HexToHash("0x17aa73f0a1b0e1acd7ec05a686cfc83a746b3230480db81105d571272aff5936"), common.HexToHash("0xf1dcb7fa915388a4a7bac1da56bd37d3590524ad84cbe0ff42d22ec2be8dcb1d"), common.HexToHash("0x63ab7d9f3c87bc4bbcff42748b09ef7cf87e7e084f6d457d277fee13a4759872")}
	rollupExitRoots := []common.Hash{common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0x2b911c2ea39040de58c4ddcc431c62ffde4abf63d36be05b7e5e8724056061b8"), common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0xc8500f8630165b35e61c846262a2ffa3cbe5608305115ec2c79f65bbad91d0b6")}

	for i := 0; i < 4; i++ {
		err := hDB.WriteBlockBatch(uint64(i+1), 1)
		assert.NoError(err)
		err = hDB.WriteGlobalExitRoot(gers[i])
		assert.NoError(err)
		err = hDB.WriteBlockGlobalExitRoot(uint64(i+1), gers[i])
		assert.NoError(err)
		l1InforTree := &zktypes.L1InfoTreeUpdate{
			Index:           uint64(i),
			GER:             gers[i],
			MainnetExitRoot: mainnetExitRoots[i],
			RollupExitRoot:  rollupExitRoots[i],
			ParentHash:      parentHashes[i],
			Timestamp:       times[i],
			BlockNumber:     uint64(i + 1),
		}
		err = hDB.WriteLatestUsedGer(uint64(i+1), gers[i])
		assert.NoError(err)
		err = hDB.WriteL1InfoTreeUpdateToGer(l1InforTree)
		assert.NoError(err)
	}

	err = hDB.WriteSequence(4, 1, common.HexToHash("0x22ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba97"), stateRoots[len(stateRoots)-1])
	assert.NoError(err)

	tx.Commit()
	ger, err := zkEvmImpl.GetLatestGlobalExitRoot(ctx)
	assert.NoError(err)
	t.Logf("ger: %+v", ger)
	assert.Equal(gers[len(gers)-1], ger)
}

func TestGetVersionHistory(t *testing.T) {
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
	ti := time.Now()
	for i := 0; i < 10; i++ {
		tiAux := ti
		ok, err := hDB.WriteErigonVersion(fmt.Sprintf("v%d", i+1), tiAux.Add(time.Duration(1)*time.Second))
		assert.NoError(err)
		assert.True(ok)
	}
	tx.Commit()

	rawHistory, err := zkEvmImpl.GetVersionHistory(ctx)
	assert.NoError(err)
	var history map[string]time.Time
	err = json.Unmarshal(rawHistory, &history)
	assert.NoError(err)
	t.Logf("Version history: %+v", history)
	for i := 0; i < 10; i++ {
		tiAux := ti
		timeReceived := history[fmt.Sprintf("v%d", i+1)]
		assert.Equal(tiAux.Add(time.Duration(1)*time.Second).Unix(), timeReceived.Unix())
	}
}

func TestGetExitRootTable(t *testing.T) {
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

	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, nil, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)

	gers := []common.Hash{common.HexToHash("0xf010e584db63e18e207a2a2a09cfef322b8f8f185df5093ed17794ac365ef60e"), common.HexToHash("0x12021ea011bd6ebffee86ea47e2c3d08e4fe734ba7251f2ddbc9fa648af3b1e6"), common.HexToHash("0x055bbf062f8add981fd54801e5c36d404da37b8300a7babc2bd2585a54a2195a"), common.HexToHash("0x252feef2a0468f334e0efa3ec67ceb04dbe3d64204242b3774ce1850f8042760")}
	parentHashes := []common.Hash{common.HexToHash("0x502b94aa765e198ecd736bcb3ec673e1fcb5985d8e610b1ba06bcf9fbdb965b2"), common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e")}
	times := []uint64{1714427000, 1714427003, 1714427006, 1714427009}
	mainnetExitRoots := []common.Hash{common.HexToHash("0x6d2478612063b2ecb19b1c75dda5add47630bbae42a2e84f7ccd33c1540db1de"), common.HexToHash("0x17aa73f0a1b0e1acd7ec05a686cfc83a746b3230480db81105d571272aff5936"), common.HexToHash("0xf1dcb7fa915388a4a7bac1da56bd37d3590524ad84cbe0ff42d22ec2be8dcb1d"), common.HexToHash("0x63ab7d9f3c87bc4bbcff42748b09ef7cf87e7e084f6d457d277fee13a4759872")}
	rollupExitRoots := []common.Hash{common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0x2b911c2ea39040de58c4ddcc431c62ffde4abf63d36be05b7e5e8724056061b8"), common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0xc8500f8630165b35e61c846262a2ffa3cbe5608305115ec2c79f65bbad91d0b6")}
	l1InfoRoots := []common.Hash{common.HexToHash("0x52b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653d4"), common.HexToHash("0x3f911c2ea39040de58c4ddcc431c62ffde4abf63d36be05b7e5e8724056061a8"), common.HexToHash("0xaab6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653ff"), common.HexToHash("0x12500f8630165b35e61c846262a2ffa3cbe5608305115ec2c79f65bbad91d045")}

	for i := 0; i < 4; i++ {
		err := hDB.WriteBlockBatch(uint64(i+1), 1)
		assert.NoError(err)
		err = hDB.WriteGlobalExitRoot(gers[i])
		assert.NoError(err)
		err = hDB.WriteBlockGlobalExitRoot(uint64(i+1), gers[i])
		assert.NoError(err)
		l1InforTree := &zktypes.L1InfoTreeUpdate{
			Index:           uint64(i + 1),
			GER:             gers[i],
			MainnetExitRoot: mainnetExitRoots[i],
			RollupExitRoot:  rollupExitRoots[i],
			ParentHash:      parentHashes[i],
			Timestamp:       times[i],
			BlockNumber:     uint64(i + 1),
		}
		err = hDB.WriteL1InfoTreeUpdateToGer(l1InforTree)
		assert.NoError(err)
		err = hDB.WriteL1InfoTreeUpdate(l1InforTree)
		assert.NoError(err)
		err = hDB.WriteL1InfoTreeRoot(l1InfoRoots[i], l1InforTree.Index)
		assert.NoError(err)
	}
	tx.Commit()

	exitRootEntries, err := zkEvmImpl.GetExitRootTable(ctx)
	assert.NoError(err)
	t.Logf("exitRootEntries: %+v", exitRootEntries)
	for i, er := range exitRootEntries {
		assert.Equal(uint64(i+1), er.BlockNumber)
		assert.Equal(gers[i], er.Ger)
		assert.Equal(uint64(i+1), er.Index)
		assert.Equal(l1InfoRoots[i], er.InfoRoot)
		assert.Equal(mainnetExitRoots[i], er.MainnetExitRoot)
		assert.Equal(times[i], er.MinTimestamp)
		assert.Equal(parentHashes[i], er.ParentHash)
		assert.Equal(rollupExitRoots[i], er.RollupExitRoot)
	}
}

var (
	testKey0, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testKey1, _ = crypto.HexToECDSA("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
	testKey2, _ = crypto.HexToECDSA("28b2b0318721be8c8339199172cd7cc8f5e273800a35616ec893083a4b32c02e")
	testKey3, _ = crypto.HexToECDSA("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")
)

func TestGetFullBlockByNumber(t *testing.T) {
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

	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, nil, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	erigonDB := erigon_db.NewErigonDb(tx)

	gers := []common.Hash{common.HexToHash("0xf010e584db63e18e207a2a2a09cfef322b8f8f185df5093ed17794ac365ef60e"), common.HexToHash("0x12021ea011bd6ebffee86ea47e2c3d08e4fe734ba7251f2ddbc9fa648af3b1e6"), common.HexToHash("0x055bbf062f8add981fd54801e5c36d404da37b8300a7babc2bd2585a54a2195a"), common.HexToHash("0x252feef2a0468f334e0efa3ec67ceb04dbe3d64204242b3774ce1850f8042760")}
	parentHashes := []common.Hash{common.HexToHash("0x502b94aa765e198ecd736bcb3ec673e1fcb5985d8e610b1ba06bcf9fbdb965b2"), common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e")}
	hashes := []common.Hash{common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e"), common.HexToHash("0x002241472c8ffeb86cd3c2bfe928cc41a47fdeffa0e98f56c1da3db6d50d29cb")}
	stateRoots := []common.Hash{common.HexToHash("0x70ee58f4d74b706ce88307800983c06c0479f9808d38db5d751d7306f510c9b8"), common.HexToHash("0xba46d17db3364a059cc6efada4a1cc7bea472c559247aafdd920fbd017031fee"), common.HexToHash("0x7dbca3d3f5841bb8a5da985655235587c212826b0f21127e4f3470230d05b0f8"), common.HexToHash("0x551b6fdb2b0c156b104a946f815c3e2c87324be35fba14cf0ed3e4c1287d89bf")}
	txsRoot := []common.Hash{common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")}
	coinBase := common.HexToAddress("0x761d53b47334bee6612c0bd1467fb881435375b2")
	times := []uint64{1714427000, 1714427003, 1714427006, 1714427009}
	gasLimits := []uint64{1125899906842624, 1125899906842624, 1125899906842624, 1125899906842624}
	mainnetExitRoots := []common.Hash{common.HexToHash("0x6d2478612063b2ecb19b1c75dda5add47630bbae42a2e84f7ccd33c1540db1de"), common.HexToHash("0x17aa73f0a1b0e1acd7ec05a686cfc83a746b3230480db81105d571272aff5936"), common.HexToHash("0xf1dcb7fa915388a4a7bac1da56bd37d3590524ad84cbe0ff42d22ec2be8dcb1d"), common.HexToHash("0x63ab7d9f3c87bc4bbcff42748b09ef7cf87e7e084f6d457d277fee13a4759872")}
	rollupExitRoots := []common.Hash{common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0x2b911c2ea39040de58c4ddcc431c62ffde4abf63d36be05b7e5e8724056061b8"), common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0xc8500f8630165b35e61c846262a2ffa3cbe5608305115ec2c79f65bbad91d0b6")}

	signer1 := types.MakeSigner(params.TestChainConfig, 1)
	signer2 := types.MakeSigner(params.TestChainConfig, 2)
	signer3 := types.MakeSigner(params.TestChainConfig, 3)
	signer4 := types.MakeSigner(params.TestChainConfig, 4)

	var tx0 types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx1 types.Transaction = types.NewTransaction(1, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx2 types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx3 types.Transaction = types.NewTransaction(1, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx4 types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx5 types.Transaction = types.NewTransaction(1, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx6 types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx7 types.Transaction = types.NewTransaction(1, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx8 types.Transaction = types.NewTransaction(2, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})

	tx0, _ = types.SignTx(tx0, *signer1, testKey0)
	tx1, _ = types.SignTx(tx1, *signer1, testKey0)
	tx2, _ = types.SignTx(tx2, *signer2, testKey1)
	tx3, _ = types.SignTx(tx3, *signer2, testKey1)
	tx4, _ = types.SignTx(tx4, *signer3, testKey2)
	tx5, _ = types.SignTx(tx5, *signer3, testKey2)
	tx6, _ = types.SignTx(tx6, *signer4, testKey3)
	tx7, _ = types.SignTx(tx7, *signer4, testKey3)
	tx8, _ = types.SignTx(tx8, *signer4, testKey3)

	txs := [][]types.Transaction{{tx0, tx1}, {tx2, tx3}, {tx4, tx5}, {tx6, tx7, tx8}}

	for i := 0; i < 4; i++ {
		err := hDB.WriteBlockBatch(uint64(i+1), 1)
		assert.NoError(err)
		err = hDB.WriteGlobalExitRoot(gers[i])
		assert.NoError(err)
		err = hDB.WriteBlockGlobalExitRoot(uint64(i+1), gers[i])
		assert.NoError(err)
		l1InforTree := &zktypes.L1InfoTreeUpdate{
			Index:           uint64(i),
			GER:             gers[i],
			MainnetExitRoot: mainnetExitRoots[i],
			RollupExitRoot:  rollupExitRoots[i],
			ParentHash:      parentHashes[i],
			Timestamp:       times[i],
			BlockNumber:     uint64(i + 1),
		}
		err = hDB.WriteL1InfoTreeUpdateToGer(l1InforTree)
		assert.NoError(err)
	}

	err = hDB.WriteSequence(4, 1, common.HexToHash("0x22ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba97"), stateRoots[len(stateRoots)-1])
	assert.NoError(err)

	for i := 0; i < 4; i++ {
		_, err := erigonDB.WriteHeader(big.NewInt(int64(i+1)), hashes[i], stateRoots[i], txsRoot[i], parentHashes[i], coinBase, times[i], gasLimits[i])
		assert.NoError(err)
		err = erigonDB.WriteBody(big.NewInt(int64(i+1)), hashes[i], txs[i])
		assert.NoError(err)
	}
	_, err = erigonDB.WriteHeader(big.NewInt(8), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba01"), common.HexToHash("0x57ddb9a336815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7bf47"), common.HexToHash("0x67ddb9a356813c3f4c1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba57"), common.HexToHash("0x87ddb9a356812c3fac1026b6dec5df31245fbadb485c9ba5a3e3398a04b7ba68"), coinBase, 1714427021, 1125899906842624)
	assert.NoError(err)
	err = erigonDB.WriteBody(big.NewInt(8), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba01"), []types.Transaction{})
	assert.NoError(err)
	_, err = erigonDB.WriteHeader(big.NewInt(9), common.HexToHash("0x27ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba81"), common.HexToHash("0x37ddb9a336815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba43"), common.HexToHash("0x87ddb9a356815c3f4c1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba56"), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df31245fbadb485c9ba5a3e3398a04b7ba48"), coinBase, 1714427024, 1125899906842624)
	assert.NoError(err)
	err = erigonDB.WriteBody(big.NewInt(9), common.HexToHash("0x27ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba81"), []types.Transaction{})
	assert.NoError(err)

	tx.Commit()

	// Block 1
	block1, err := zkEvmImpl.GetFullBlockByNumber(ctx, rpc.BlockNumber(1), true)
	assert.NoError(err)
	t.Logf("block 1: %+v", block1)
	assert.Equal(2, len(block1.Transactions))
	for i, tx := range block1.Transactions {
		assert.Equal(rpctypes.ArgUint64(i), tx.Tx.Nonce)
		assert.Equal(rpctypes.ArgUint64(21000), tx.Tx.Gas)
		assert.Equal(common.HexToAddress("0x71562b71999873DB5b286dF957af199Ec94617F7"), tx.Tx.From)
		assert.Equal(rpctypes.ArgUint64(1), *tx.Tx.BlockNumber)
	}
	block1, err = zkEvmImpl.GetFullBlockByNumber(ctx, rpc.BlockNumber(1), false)
	assert.NoError(err)
	assert.Equal(2, len(block1.Transactions))
	assert.Equal(tx0.Hash(), *block1.Transactions[0].Hash)
	assert.Equal(tx1.Hash(), *block1.Transactions[1].Hash)

	// Block 2
	block2, err := zkEvmImpl.GetFullBlockByNumber(ctx, rpc.BlockNumber(2), true)
	assert.NoError(err)
	t.Logf("block 2: %+v", block2)
	assert.Equal(2, len(block2.Transactions))
	for i, tx := range block2.Transactions {
		assert.Equal(rpctypes.ArgUint64(i), tx.Tx.Nonce)
		assert.Equal(rpctypes.ArgUint64(21000), tx.Tx.Gas)
		assert.Equal(common.HexToAddress("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"), tx.Tx.From)
		assert.Equal(rpctypes.ArgUint64(2), *tx.Tx.BlockNumber)
	}
	block2, err = zkEvmImpl.GetFullBlockByNumber(ctx, rpc.BlockNumber(2), false)
	assert.NoError(err)
	assert.Equal(2, len(block2.Transactions))
	assert.Equal(tx2.Hash(), *block2.Transactions[0].Hash)
	assert.Equal(tx3.Hash(), *block2.Transactions[1].Hash)

	// Block 3
	block3, err := zkEvmImpl.GetFullBlockByNumber(ctx, rpc.BlockNumber(3), true)
	assert.NoError(err)
	t.Logf("block 3: %+v", block3)
	assert.Equal(2, len(block3.Transactions))
	for i, tx := range block3.Transactions {
		assert.Equal(rpctypes.ArgUint64(i), tx.Tx.Nonce)
		assert.Equal(rpctypes.ArgUint64(21000), tx.Tx.Gas)
		assert.Equal(common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"), tx.Tx.From)
		assert.Equal(rpctypes.ArgUint64(3), *tx.Tx.BlockNumber)
	}
	block3, err = zkEvmImpl.GetFullBlockByNumber(ctx, rpc.BlockNumber(3), false)
	assert.NoError(err)
	assert.Equal(2, len(block3.Transactions))
	assert.Equal(tx4.Hash(), *block3.Transactions[0].Hash)
	assert.Equal(tx5.Hash(), *block3.Transactions[1].Hash)

	// Block 4
	block4, err := zkEvmImpl.GetFullBlockByNumber(ctx, rpc.BlockNumber(4), true)
	assert.NoError(err)
	t.Logf("block 4: %+v", block4)
	assert.Equal(3, len(block4.Transactions))
	for i, tx := range block4.Transactions {
		assert.Equal(rpctypes.ArgUint64(i), tx.Tx.Nonce)
		assert.Equal(rpctypes.ArgUint64(21000), tx.Tx.Gas)
		assert.Equal(common.HexToAddress("0x70997970C51812dc3A010C7d01b50e0d17dc79C8"), tx.Tx.From)
		assert.Equal(rpctypes.ArgUint64(4), *tx.Tx.BlockNumber)
	}
	block4, err = zkEvmImpl.GetFullBlockByNumber(ctx, rpc.BlockNumber(4), false)
	assert.NoError(err)
	assert.Equal(3, len(block4.Transactions))
	assert.Equal(tx6.Hash(), *block4.Transactions[0].Hash)
	assert.Equal(tx7.Hash(), *block4.Transactions[1].Hash)
	assert.Equal(tx8.Hash(), *block4.Transactions[2].Hash)
}

func TestGetFullBlockByHash(t *testing.T) {
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

	zkEvmImpl := NewZkEvmAPI(ethImpl, db, 100_000, &ethconfig.Defaults, nil, "")
	tx, err := db.BeginRw(ctx)
	assert.NoError(err)
	hDB := hermez_db.NewHermezDb(tx)
	erigonDB := erigon_db.NewErigonDb(tx)

	gers := []common.Hash{common.HexToHash("0xf010e584db63e18e207a2a2a09cfef322b8f8f185df5093ed17794ac365ef60e"), common.HexToHash("0x12021ea011bd6ebffee86ea47e2c3d08e4fe734ba7251f2ddbc9fa648af3b1e6"), common.HexToHash("0x055bbf062f8add981fd54801e5c36d404da37b8300a7babc2bd2585a54a2195a"), common.HexToHash("0x252feef2a0468f334e0efa3ec67ceb04dbe3d64204242b3774ce1850f8042760")}
	parentHashes := []common.Hash{common.HexToHash("0x502b94aa765e198ecd736bcb3ec673e1fcb5985d8e610b1ba06bcf9fbdb965b2"), common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e")}
	hashes := []common.Hash{common.HexToHash("0x55a33ac3bf2cc61ceafdee10415448de84c2e64dc75b3f622fd61d250c1362ec"), common.HexToHash("0x88b7f001ebab21b77fe747af424320e23c039decd5e3f2bb2e074b6956079bdf"), common.HexToHash("0x89f4d0933bdcaf5a43266a701e081e4364e2f6d78ae8a82baea4b73e4531821e"), common.HexToHash("0x002241472c8ffeb86cd3c2bfe928cc41a47fdeffa0e98f56c1da3db6d50d29cb")}
	stateRoots := []common.Hash{common.HexToHash("0x70ee58f4d74b706ce88307800983c06c0479f9808d38db5d751d7306f510c9b8"), common.HexToHash("0xba46d17db3364a059cc6efada4a1cc7bea472c559247aafdd920fbd017031fee"), common.HexToHash("0x7dbca3d3f5841bb8a5da985655235587c212826b0f21127e4f3470230d05b0f8"), common.HexToHash("0x551b6fdb2b0c156b104a946f815c3e2c87324be35fba14cf0ed3e4c1287d89bf")}
	txsRoot := []common.Hash{common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"), common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")}
	coinBase := common.HexToAddress("0x761d53b47334bee6612c0bd1467fb881435375b2")
	times := []uint64{1714427000, 1714427003, 1714427006, 1714427009}
	gasLimits := []uint64{1125899906842624, 1125899906842624, 1125899906842624, 1125899906842624}
	mainnetExitRoots := []common.Hash{common.HexToHash("0x6d2478612063b2ecb19b1c75dda5add47630bbae42a2e84f7ccd33c1540db1de"), common.HexToHash("0x17aa73f0a1b0e1acd7ec05a686cfc83a746b3230480db81105d571272aff5936"), common.HexToHash("0xf1dcb7fa915388a4a7bac1da56bd37d3590524ad84cbe0ff42d22ec2be8dcb1d"), common.HexToHash("0x63ab7d9f3c87bc4bbcff42748b09ef7cf87e7e084f6d457d277fee13a4759872")}
	rollupExitRoots := []common.Hash{common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0x2b911c2ea39040de58c4ddcc431c62ffde4abf63d36be05b7e5e8724056061b8"), common.HexToHash("0x50b6637901ac94283cb4f2dcd3606c42a421444ce9643a55ecf95ec9ba5653a7"), common.HexToHash("0xc8500f8630165b35e61c846262a2ffa3cbe5608305115ec2c79f65bbad91d0b6")}

	signer1 := types.MakeSigner(params.TestChainConfig, 1)
	signer2 := types.MakeSigner(params.TestChainConfig, 2)
	signer3 := types.MakeSigner(params.TestChainConfig, 3)
	signer4 := types.MakeSigner(params.TestChainConfig, 4)

	var tx0 types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx1 types.Transaction = types.NewTransaction(1, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx2 types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx3 types.Transaction = types.NewTransaction(1, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx4 types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx5 types.Transaction = types.NewTransaction(1, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx6 types.Transaction = types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx7 types.Transaction = types.NewTransaction(1, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})
	var tx8 types.Transaction = types.NewTransaction(2, common.Address{}, uint256.NewInt(0), 21000, uint256.NewInt(0), []byte{})

	tx0, _ = types.SignTx(tx0, *signer1, testKey0)
	tx1, _ = types.SignTx(tx1, *signer1, testKey0)
	tx2, _ = types.SignTx(tx2, *signer2, testKey1)
	tx3, _ = types.SignTx(tx3, *signer2, testKey1)
	tx4, _ = types.SignTx(tx4, *signer3, testKey2)
	tx5, _ = types.SignTx(tx5, *signer3, testKey2)
	tx6, _ = types.SignTx(tx6, *signer4, testKey3)
	tx7, _ = types.SignTx(tx7, *signer4, testKey3)
	tx8, _ = types.SignTx(tx8, *signer4, testKey3)

	txs := [][]types.Transaction{{tx0, tx1}, {tx2, tx3}, {tx4, tx5}, {tx6, tx7, tx8}}

	for i := 0; i < 4; i++ {
		err := hDB.WriteBlockBatch(uint64(i+1), 1)
		assert.NoError(err)
		err = hDB.WriteGlobalExitRoot(gers[i])
		assert.NoError(err)
		err = hDB.WriteBlockGlobalExitRoot(uint64(i+1), gers[i])
		assert.NoError(err)
		l1InforTree := &zktypes.L1InfoTreeUpdate{
			Index:           uint64(i),
			GER:             gers[i],
			MainnetExitRoot: mainnetExitRoots[i],
			RollupExitRoot:  rollupExitRoots[i],
			ParentHash:      parentHashes[i],
			Timestamp:       times[i],
			BlockNumber:     uint64(i + 1),
		}
		err = hDB.WriteL1InfoTreeUpdateToGer(l1InforTree)
		assert.NoError(err)
	}

	err = hDB.WriteSequence(4, 1, common.HexToHash("0x22ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba97"), stateRoots[len(stateRoots)-1])
	assert.NoError(err)

	for i := 0; i < 4; i++ {
		_, err := erigonDB.WriteHeader(big.NewInt(int64(i+1)), hashes[i], stateRoots[i], txsRoot[i], parentHashes[i], coinBase, times[i], gasLimits[i])
		assert.NoError(err)
		err = erigonDB.WriteBody(big.NewInt(int64(i+1)), hashes[i], txs[i])
		assert.NoError(err)
	}
	_, err = erigonDB.WriteHeader(big.NewInt(8), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba01"), common.HexToHash("0x57ddb9a336815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7bf47"), common.HexToHash("0x67ddb9a356813c3f4c1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba57"), common.HexToHash("0x87ddb9a356812c3fac1026b6dec5df31245fbadb485c9ba5a3e3398a04b7ba68"), coinBase, 1714427021, 1125899906842624)
	assert.NoError(err)
	err = erigonDB.WriteBody(big.NewInt(8), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba01"), []types.Transaction{})
	assert.NoError(err)
	_, err = erigonDB.WriteHeader(big.NewInt(9), common.HexToHash("0x27ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba81"), common.HexToHash("0x37ddb9a336815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba43"), common.HexToHash("0x87ddb9a356815c3f4c1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba56"), common.HexToHash("0x67ddb9a356815c3fac1026b6dec5df31245fbadb485c9ba5a3e3398a04b7ba48"), coinBase, 1714427024, 1125899906842624)
	assert.NoError(err)
	err = erigonDB.WriteBody(big.NewInt(9), common.HexToHash("0x27ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a84b7ba81"), []types.Transaction{})
	assert.NoError(err)

	tx.Commit()

	// Block 1
	block1, err := zkEvmImpl.GetFullBlockByHash(ctx, hashes[0], true)
	assert.NoError(err)
	t.Logf("block 1: %+v", block1)
	assert.Equal(2, len(block1.Transactions))
	for i, tx := range block1.Transactions {
		assert.Equal(rpctypes.ArgUint64(i), tx.Tx.Nonce)
		assert.Equal(rpctypes.ArgUint64(21000), tx.Tx.Gas)
		assert.Equal(common.HexToAddress("0x71562b71999873DB5b286dF957af199Ec94617F7"), tx.Tx.From)
		assert.Equal(rpctypes.ArgUint64(1), *tx.Tx.BlockNumber)
	}
	block1, err = zkEvmImpl.GetFullBlockByHash(ctx, hashes[0], false)
	assert.NoError(err)
	assert.Equal(2, len(block1.Transactions))
	assert.Equal(tx0.Hash(), *block1.Transactions[0].Hash)
	assert.Equal(tx1.Hash(), *block1.Transactions[1].Hash)

	// Block 2
	block2, err := zkEvmImpl.GetFullBlockByHash(ctx, hashes[1], true)
	assert.NoError(err)
	t.Logf("block 2: %+v", block2)
	assert.Equal(2, len(block2.Transactions))
	for i, tx := range block2.Transactions {
		assert.Equal(rpctypes.ArgUint64(i), tx.Tx.Nonce)
		assert.Equal(rpctypes.ArgUint64(21000), tx.Tx.Gas)
		assert.Equal(common.HexToAddress("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"), tx.Tx.From)
		assert.Equal(rpctypes.ArgUint64(2), *tx.Tx.BlockNumber)
	}
	block2, err = zkEvmImpl.GetFullBlockByHash(ctx, hashes[1], false)
	assert.NoError(err)
	assert.Equal(2, len(block2.Transactions))
	assert.Equal(tx2.Hash(), *block2.Transactions[0].Hash)
	assert.Equal(tx3.Hash(), *block2.Transactions[1].Hash)

	// Block 3
	block3, err := zkEvmImpl.GetFullBlockByHash(ctx, hashes[2], true)
	assert.NoError(err)
	t.Logf("block 3: %+v", block3)
	assert.Equal(2, len(block3.Transactions))
	for i, tx := range block3.Transactions {
		assert.Equal(rpctypes.ArgUint64(i), tx.Tx.Nonce)
		assert.Equal(rpctypes.ArgUint64(21000), tx.Tx.Gas)
		assert.Equal(common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D"), tx.Tx.From)
		assert.Equal(rpctypes.ArgUint64(3), *tx.Tx.BlockNumber)
	}
	block3, err = zkEvmImpl.GetFullBlockByHash(ctx, hashes[2], false)
	assert.NoError(err)
	assert.Equal(2, len(block3.Transactions))
	assert.Equal(tx4.Hash(), *block3.Transactions[0].Hash)
	assert.Equal(tx5.Hash(), *block3.Transactions[1].Hash)

	// Block 4
	block4, err := zkEvmImpl.GetFullBlockByHash(ctx, hashes[3], true)
	assert.NoError(err)
	t.Logf("block 4: %+v", block4)
	assert.Equal(3, len(block4.Transactions))
	for i, tx := range block4.Transactions {
		assert.Equal(rpctypes.ArgUint64(i), tx.Tx.Nonce)
		assert.Equal(rpctypes.ArgUint64(21000), tx.Tx.Gas)
		assert.Equal(common.HexToAddress("0x70997970C51812dc3A010C7d01b50e0d17dc79C8"), tx.Tx.From)
		assert.Equal(rpctypes.ArgUint64(4), *tx.Tx.BlockNumber)
	}
	block4, err = zkEvmImpl.GetFullBlockByHash(ctx, hashes[3], false)
	assert.NoError(err)
	assert.Equal(3, len(block4.Transactions))
	assert.Equal(tx6.Hash(), *block4.Transactions[0].Hash)
	assert.Equal(tx7.Hash(), *block4.Transactions[1].Hash)
	assert.Equal(tx8.Hash(), *block4.Transactions[2].Hash)
}
