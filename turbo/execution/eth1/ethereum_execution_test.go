package eth1

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/clique"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconsensusconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/engineapi/engine_helpers"
	"github.com/erigontech/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	stages2 "github.com/erigontech/erigon/turbo/stages"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func skipOnUnsupportedPlatform(t testing.TB) {
	if !(runtime.GOOS == "linux" && runtime.GOARCH == "amd64") {
		t.Skip("Silkworm is only supported on linux/amd64")
	}
}

type MockTest struct {
	Ctx            context.Context
	Log            log.Logger
	DB             kv.RwDB
	Dirs           datadir.Dirs
	Engine         consensus.Engine
	GenesisSpec    *types.Genesis
	GenesisBlock   *types.Block
	ChainConfig    *chain.Config
	SentriesClient *sentry_multi_client.MultiClient
	BlockReader    *freezeblocks.BlockReader
	BlockWriter    *blockio.BlockWriter
	Key            *ecdsa.PrivateKey
	// SentryClient         direct.SentryClient
	Address              libcommon.Address
	Eth1ExecutionService *EthereumExecutionModule

	Notifications *shards.Notifications
	Agg           *libstate.Aggregator
}

func setup(t testing.TB) *MockTest {

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

	m := &MockTest{
		Key:     key,
		Address: crypto.PubkeyToAddress(key.PublicKey),
	}

	tempDir := t.TempDir()

	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, os.ModePerm)

	// tempDir := t.TempDir()
	// log.Warn("TempDir", "dir", tempDir)

	println("Address: ", m.Address.Hex())

	m.Dirs = datadir.New(tempDir)
	// log.Warn("DataDir", "dir", dirs.DataDir)

	// db := mdbx2.NewMDBX(log.New()).Path(dirs.DataDir).Exclusive().InMem(dirs.DataDir).Label(kv.ChainDB).MustOpen()
	// // db := mdbx2.NewMDBX(log.New()).Path(dirs.Chaindata).Exclusive().Label(kv.ChainDB).MustOpen()
	// agg, err := state.NewAggregator(context.Background(), dirs.SnapHistory, dirs.Tmp, 3_125_000, db, log.Root())
	// if err != nil {
	// 	panic(err)
	// }

	m.DB, m.Agg = temporaltest.NewTestDB(nil, m.Dirs)

	// histV3, db, agg := temporaltest.NewTestDB(nil, dirs)
	// if histV3 {
	// 	panic("HistoryV3 is not supported")
	// }

	// Genesis block
	// network := "mainnet"
	// genesis := core.GenesisBlockByChainName(network)

	m.GenesisSpec = core.DeveloperGenesisBlock(1, m.Address)
	// genspec := &types.Genesis{
	// 	ExtraData: make([]byte, clique.ExtraVanity+length.Addr+clique.ExtraSeal),
	// 	Alloc: map[libcommon.Address]types.GenesisAccount{
	// 		m.Address: {Balance: big.NewInt(10000000000000000)},
	// 	},
	// 	Config: params.AllCliqueProtocolChanges,
	// }
	// copy(genspec.ExtraData[clique.ExtraVanity:], m.Address[:])
	// m.GenesisSpec = genspec

	var err error
	m.ChainConfig, m.GenesisBlock, err = core.CommitGenesisBlock(m.DB, m.GenesisSpec, m.Dirs, m.Log)
	require.NoError(t, err)
	// expect := params.GenesisHashByChainName(network)
	// require.NotNil(t, expect, network)
	// require.EqualValues(t, genesisBlock.Hash(), *expect, network)

	// tx, err := m.DB.BeginRw(context.Background())
	// require.NoError(t, err)
	// defer tx.Rollback()
	// m.ChainConfig, m.GenesisBlock, err = core.WriteGenesisBlock(tx, m.GenesisSpec, nil, m.Dirs, log.New())
	// require.NoError(t, err)
	// tx.Commit()

	// 0. Setup
	cfg := ethconfig.Defaults
	cfg.StateStream = true
	cfg.BatchSize = 1 * datasize.MB
	cfg.Sync.BodyDownloadTimeoutSeconds = 10
	cfg.DeprecatedTxPool.Disable = true
	cfg.DeprecatedTxPool.StartOnInit = true
	cfg.Dirs = m.Dirs
	cfg.Genesis = m.GenesisSpec

	// 11. Logger
	m.Log = log.Root()
	m.Log.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	m.Notifications = &shards.Notifications{
		Events:      shards.NewEvents(),
		Accumulator: shards.NewAccumulator(),
	}

	// 1. Block reader/writer
	allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.Defaults.Snapshot, m.Dirs.Snap, 0, m.Log)
	allBorSnapshots := heimdall.NewRoSnapshots(ethconfig.Defaults.Snapshot, m.Dirs.Snap, 0, m.Log)

	bridgeStore := bridge.NewSnapshotStore(bridge.NewDbStore(m.DB), allBorSnapshots, m.GenesisSpec.Config.Bor)
	heimdallStore := heimdall.NewSnapshotStore(heimdall.NewDbStore(m.DB), allBorSnapshots)
	m.BlockReader = freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots, heimdallStore, bridgeStore)
	m.BlockWriter = blockio.NewBlockWriter()

	// 14. Context
	m.Ctx = context.Background()

	// 12. Consensu Engine
	m.Engine = ethconsensusconfig.CreateConsensusEngineBareBones(m.Ctx, m.ChainConfig, m.Log)
	const blockBufferSize = 128

	statusDataProvider := sentry.NewStatusDataProvider(
		m.DB,
		m.ChainConfig,
		m.GenesisBlock,
		m.ChainConfig.ChainID.Uint64(),
		m.Log,
	)
	// limit "new block" broadcasts to at most 10 random peers at time
	maxBlockBroadcastPeers := func(header *types.Header) uint { return 0 }

	m.SentriesClient, err = sentry_multi_client.NewMultiClient(
		m.DB,
		m.ChainConfig,
		m.Engine,
		[]proto_sentry.SentryClient{}, /*sentries*/
		cfg.Sync,
		m.BlockReader,
		blockBufferSize,
		statusDataProvider,
		false,
		maxBlockBroadcastPeers,
		false, /* disableBlockDownload */
		m.Log,
	)
	require.NoError(t, err)

	// 4. Fork validator
	inMemoryExecution := func(txc wrap.TxContainer, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		terseLogger := log.New()
		terseLogger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync := stages2.NewInMemoryExecution(m.Ctx, m.DB, &cfg, m.SentriesClient, m.Dirs,
			notifications, m.BlockReader, m.BlockWriter, m.Agg, nil, terseLogger)
		chainReader := stagedsync.NewChainReaderImpl(m.ChainConfig, txc.Tx, m.BlockReader, m.Log)
		// We start the mining step
		if err := stages2.StateStep(m.Ctx, chainReader, m.Engine, txc, stateSync, header, body, unwindPoint, headersChain, bodiesChain, false /*histV3*/); err != nil {
			m.Log.Warn("Could not validate block", "err", err)
			return errors.Join(consensus.ErrInvalidBlock, err)
		}
		var progress uint64
		progress, err = stages.GetStageProgress(txc.Tx, stages.Execution)
		if err != nil {
			return err
		}
		if progress < header.Number.Uint64() {
			return fmt.Errorf("unsuccessful execution, progress %d < expected %d", progress, header.Number.Uint64())
		}
		return nil
	}
	forkValidator := engine_helpers.NewForkValidator(m.Ctx, 1 /*currentBlockNumber*/, inMemoryExecution, m.Dirs.Tmp, m.BlockReader)

	// 3. Staged Sync
	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))
	m.Agg.SetSnapshotBuildSema(blockSnapBuildSema)

	var snapshotsDownloader proto_downloader.DownloaderClient
	blockRetire := freezeblocks.NewBlockRetire(1, m.Dirs, m.BlockReader, m.BlockWriter, m.DB, nil, nil, m.ChainConfig, &cfg, m.Notifications.Events, blockSnapBuildSema, m.Log)

	pipelineStages := stages2.NewPipelineStages(m.Ctx, m.DB, &cfg, p2p.Config{}, m.SentriesClient, m.Notifications,
		snapshotsDownloader, m.BlockReader, blockRetire, m.Agg, nil /*silkworm*/, forkValidator, m.Log, true /*checkStateRoot*/)
	stagedSync := stagedsync.New(cfg.Sync, pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, m.Log)

	m.Eth1ExecutionService = NewEthereumExecutionModule(m.BlockReader, m.DB, stagedSync, forkValidator, m.ChainConfig, nil /*builderFunc*/, nil /*hook*/, m.Notifications.Accumulator, m.Notifications.StateChangesConsumer, m.Log, m.Engine, cfg.Sync, m.Ctx)

	return m
}

func SampleBlock(t testing.TB, parent *types.Header, db kv.RwDB) *types.Block {
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	tx.Commit()
	return types.NewBlockWithHeader(&types.Header{
		Number:     new(big.Int).Add(parent.Number, big.NewInt(1)),
		Difficulty: clique.DiffInTurn,
		ParentHash: parent.Hash(),
		//Beneficiary: crypto.PubkeyToAddress(crypto.MustGenerateKey().PublicKey),
		TxHash:      types.EmptyRootHash,
		ReceiptHash: types.EmptyRootHash,
		GasLimit:    10000000,
		GasUsed:     0,
		Time:        parent.Time + 12,
		Extra:       make([]byte, clique.ExtraVanity+clique.ExtraSeal),
		UncleHash:   types.EmptyUncleHash,
		// Root:        rootHash,
	})
}

func TestExecutionModuleInitialization(t *testing.T) {
	skipOnUnsupportedPlatform(t)

	m := setup(t)
	require.NotNil(t, m.Eth1ExecutionService)
	require.NotNil(t, m.GenesisBlock)
}

func TestExecutionModuleSingleBlockInsertion(t *testing.T) {
	skipOnUnsupportedPlatform(t)

	m := setup(t)

	newBlock := SampleBlock(t, m.GenesisBlock.Header(), m.DB)
	request := &execution.InsertBlocksRequest{
		Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{newBlock}),
	}

	result, err := m.Eth1ExecutionService.InsertBlocks(m.Ctx, request)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, result.Result, execution.ExecutionStatus_Success)
}

func TestExecutionModuleSingleBlockChainValidation(t *testing.T) {
	skipOnUnsupportedPlatform(t)

	m := setup(t)

	// Generate a batch of blocks, each properly signed
	getHeader := func(hash libcommon.Hash, number uint64) (h *types.Header) {
		if err := m.DB.View(m.Ctx, func(tx kv.Tx) (err error) {
			h, err = m.BlockReader.Header(m.Ctx, tx, hash, number)
			return err
		}); err != nil {
			panic(err)
		}
		return h
	}
	signer := types.LatestSignerForChainID(nil)

	chain, err := core.GenerateChain(m.ChainConfig, m.GenesisBlock, m.Engine, m.DB, 1, func(i int, block *core.BlockGen) {
		// The chain maker doesn't have access to a chain, so the difficulty will be
		// lets unset (nil). Set it here to the correct value.
		block.SetDifficulty(clique.DiffInTurn)

		// We want to simulate an empty middle block, having the same state as the
		// first one. The last is needs a state change again to force a reorg.
		if i != 1 {
			baseFee, _ := uint256.FromBig(block.GetHeader().BaseFee)
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(m.Address), libcommon.Address{0x00}, new(uint256.Int), params.TxGas, baseFee, nil), *signer, m.Key)
			if err != nil {
				panic(err)
			}
			block.AddTxWithChain(getHeader, m.Engine, tx)
		}
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	for i, block := range chain.Blocks {
		header := block.Header()
		if i > 0 {
			header.ParentHash = chain.Blocks[i-1].Hash()
		}
		header.Extra = make([]byte, clique.ExtraVanity+clique.ExtraSeal)
		header.Difficulty = clique.DiffInTurn

		sig, _ := crypto.Sign(clique.SealHash(header).Bytes(), m.Key)
		copy(header.Extra[len(header.Extra)-clique.ExtraSeal:], sig)
		chain.Headers[i] = header
		chain.Blocks[i] = block.WithSeal(header)
	}

	request := &execution.InsertBlocksRequest{
		Blocks: eth1_utils.ConvertBlocksToRPC(chain.Blocks),
	}

	result, err := m.Eth1ExecutionService.InsertBlocks(m.Ctx, request)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, result.Result, execution.ExecutionStatus_Success)

	newBlock := chain.Blocks[0]

	validationRequest := &execution.ValidationRequest{
		Hash:   gointerfaces.ConvertHashToH256(newBlock.Hash()),
		Number: newBlock.Number().Uint64(),
	}

	validationResult, err := m.Eth1ExecutionService.ValidateChain(m.Ctx, validationRequest)
	require.NoError(t, err)
	require.NotNil(t, validationResult)
	require.Equal(t, validationResult.ValidationStatus, execution.ExecutionStatus_Success)
}
