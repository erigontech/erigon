package eth1

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/mdbx-go/mdbx"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv"
	mdbx2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon-lib/wrap"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/silkworm"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

var testCases = map[string]struct {
	useSilkworm bool
}{
	"silkworm on":  {useSilkworm: true},
	"silkworm off": {useSilkworm: false},
}

func SilkwormForkValidatorTestSettings() silkworm.ForkValidatorSettings {
	return silkworm.ForkValidatorSettings{
		BatchSize:               512 * 1024 * 1024,
		EtlBufferSize:           256 * 1024 * 1024,
		SyncLoopThrottleSeconds: 0,
		StopBeforeSendersStage:  false,
	}
}

type TestingCtrl interface {
	require.TestingT
	Errorf(format string, args ...interface{})
	FailNow()
	Fatalf(format string, args ...any)
	TempDir() string
}

func setup(t TestingCtrl, useSilkworm bool) (*EthereumExecutionModule, *types.Block, *silkworm.Silkworm) {

	var tempDir string
	if useSilkworm {
		tempDir = "/tmp/eth1test-silkworm"
	} else {
		tempDir = "/tmp/eth1test"
	}

	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, os.ModePerm)

	// tempDir := t.TempDir()
	// log.Warn("TempDir", "dir", tempDir)

	dirs := datadir.New(tempDir)
	// log.Warn("DataDir", "dir", dirs.DataDir)

	db := mdbx2.NewMDBX(log.New()).Path(dirs.DataDir).Exclusive().InMem(dirs.DataDir).Label(kv.ChainDB).MustOpen()
	// db := mdbx2.NewMDBX(log.New()).Path(dirs.Chaindata).Exclusive().Label(kv.ChainDB).MustOpen()
	agg, err := state.NewAggregator(context.Background(), dirs.SnapHistory, dirs.Tmp, 3_125_000, db, log.Root())
	if err != nil {
		panic(err)
	}

	// histV3, db, agg := temporaltest.NewTestDB(nil, dirs)
	// if histV3 {
	// 	panic("HistoryV3 is not supported")
	// }

	// Genesis block
	network := "mainnet"
	genesis := core.GenesisBlockByChainName(network)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	chainConfig, genesisBlock, err := core.WriteGenesisBlock(tx, genesis, nil, dirs.Chaindata, log.New())
	require.NoError(t, err)
	expect := params.GenesisHashByChainName(network)
	require.NotNil(t, expect, network)
	require.EqualValues(t, genesisBlock.Hash(), *expect, network)

	tx.Commit()

	// 0. Setup
	cfg := ethconfig.Defaults
	cfg.StateStream = true
	cfg.BatchSize = 1 * datasize.MB
	cfg.Sync.BodyDownloadTimeoutSeconds = 10
	cfg.DeprecatedTxPool.Disable = true
	cfg.DeprecatedTxPool.StartOnInit = true
	cfg.Dirs = dirs
	cfg.Genesis = genesis

	// 11. Logger
	logger := log.Root()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	notifications := &shards.Notifications{
		Events:      shards.NewEvents(),
		Accumulator: shards.NewAccumulator(),
	}

	// 1. Block reader/writer
	allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.Defaults.Snapshot, dirs.Snap, 0, logger)
	allBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.Defaults.Snapshot, dirs.Snap, 0, logger)
	blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)
	blockWriter := blockio.NewBlockWriter(false /*histV3*/)

	// 14. Context
	ctx := context.Background()

	// 12. Consensu Engine
	engine := ethconsensusconfig.CreateConsensusEngineBareBones(ctx, chainConfig, logger)
	const blockBufferSize = 128

	statusDataProvider := sentry.NewStatusDataProvider(
		db,
		chainConfig,
		genesisBlock,
		chainConfig.ChainID.Uint64(),
		logger,
	)
	// limit "new block" broadcasts to at most 10 random peers at time
	maxBlockBroadcastPeers := func(header *types.Header) uint { return 10 }

	sentriesClient, err := sentry_multi_client.NewMultiClient(
		db,
		chainConfig,
		engine,
		[]direct.SentryClient{}, /*sentries*/
		cfg.Sync,
		blockReader,
		blockBufferSize,
		statusDataProvider,
		false,
		maxBlockBroadcastPeers,
		false, /* disableBlockDownload */
		logger,
	)
	require.NoError(t, err)

	// 4. Fork validator
	inMemoryExecution := func(txc wrap.TxContainer, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		terseLogger := log.New()
		terseLogger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync := stages2.NewInMemoryExecution(ctx, db, &cfg, sentriesClient, dirs,
			notifications, blockReader, blockWriter, agg, nil, terseLogger)
		chainReader := stagedsync.NewChainReaderImpl(chainConfig, txc.Tx, blockReader, logger)
		// We start the mining step
		if err := stages2.StateStep(ctx, chainReader, engine, txc, stateSync, header, body, unwindPoint, headersChain, bodiesChain, false /*histV3*/); err != nil {
			logger.Warn("Could not validate block", "err", err)
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
	forkValidator := engine_helpers.NewForkValidator(ctx, 1 /*currentBlockNumber*/, inMemoryExecution, dirs.Tmp, blockReader)

	// 3. Staged Sync
	var snapshotsDownloader proto_downloader.DownloaderClient
	blockRetire := freezeblocks.NewBlockRetire(1, dirs, blockReader, blockWriter, db, chainConfig, notifications.Events, logger)

	pipelineStages := stages2.NewPipelineStages(ctx, db, &cfg, p2p.Config{}, sentriesClient, notifications,
		snapshotsDownloader, blockReader, blockRetire, agg, nil /*silkworm*/, forkValidator, logger, true /*checkStateRoot*/)
	stagedSync := stagedsync.New(cfg.Sync, pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, logger)

	// 5. Silkworm
	var silkwormInstance *silkworm.Silkworm
	var silkwormForkValidator *silkworm.ForkValidatorService
	if useSilkworm {
		silkwormInstance, err = silkworm.New(dirs.DataDir, mdbx.Version(), uint32(1), log.LvlCrit)
		require.NoError(t, err)
		silkwormForkValidator = silkworm.NewForkValidatorService(silkwormInstance, db, SilkwormForkValidatorTestSettings())
		err = silkwormForkValidator.Start()
		require.NoError(t, err)
	}

	// Globals
	globalSigner = types.LatestSigner(chainConfig)

	return NewEthereumExecutionModule(blockReader, db, stagedSync, forkValidator, silkwormForkValidator, chainConfig, nil /*builderFunc*/, nil /*hook*/, notifications.Accumulator, notifications.StateChangesConsumer, logger, engine, false /*historyV3*/, ctx),
		genesisBlock,
		silkwormInstance
}

func teardown(executionModule *EthereumExecutionModule, silkwormInstance *silkworm.Silkworm) {
	if executionModule != nil && executionModule.silkwormForkValidator != nil {

		executionModule.silkwormForkValidator.Stop()
	}

	if silkwormInstance != nil {
		silkwormInstance.Close()
	}
}

var (
	block1RootHash = libcommon.HexToHash("34eca9cd7324e3a1df317e439a18119ad9a3c988fbf4d20783bb7bee56bafd64")
	block2RootHash = libcommon.HexToHash("66d801330ca4ebb926acd4cf890ba4bd015fb5a4716c414fec6ff739c0d4a2a1")
	block3RootHash = libcommon.HexToHash("2b79f72d542fe0f65da292c052be82af08dda0d415b5fdce0cd191121ca0d971")
	block4RootHash = libcommon.HexToHash("e8d36b208e37daa1be5893d4806dcce4573fc9d275271c1f569e399c77d0c157")
)

var globalSigner *types.Signer

func SampleBlock(parent *types.Header, rootHash libcommon.Hash) *types.Block {

	return types.NewBlockWithHeader(&types.Header{
		Number:     new(big.Int).Add(parent.Number, big.NewInt(1)),
		Difficulty: new(big.Int).Add(parent.Number, big.NewInt(17000000000+rand.Int63n(1000000000))),
		ParentHash: parent.Hash(),
		//Beneficiary: crypto.PubkeyToAddress(crypto.MustGenerateKey().PublicKey),
		TxHash:      types.EmptyRootHash,
		ReceiptHash: types.EmptyRootHash,
		GasLimit:    10000000,
		GasUsed:     0,
		Time:        parent.Time + 12,
		Root:        rootHash,
	})

	// key, _ := crypto.GenerateKey()
	// addr := crypto.PubkeyToAddress(key.PublicKey)

	// amount := uint256.NewInt(1)
	// gasPrice := uint256.NewInt(300000)

	// gasFeeCap := uint256.NewInt(100000)
	// chainId := uint256.NewInt(0)
	// transaction := types.NewEIP1559Transaction(*chainId, 1, addr, amount, uint64(210_000), gasPrice, new(uint256.Int), gasFeeCap, nil)

	// transaction := types.NewTransaction(1, addr, amount, 123457, gasPrice, make([]byte, 100))

	// signedTx, _ := types.SignTx(transaction, *globalSigner, key)
	// signedTx, _ := transaction.FakeSign(addr)

	// if signedTx.Protected() {
	// 	log.Info("Transaction is protected")
	// }

	// return types.NewBlock(
	// 	&types.Header{
	// 		Number: new(big.Int).Add(parent.Number, big.NewInt(1)),
	// 		// add random difficulty
	// 		Difficulty: new(big.Int).Add(parent.Number, big.NewInt(17000000000+rand.Int63n(1000000))),
	// 		ParentHash: parent.Hash(),
	// 		//Beneficiary: crypto.PubkeyToAddress(crypto.MustGenerateKey().PublicKey),
	// 		TxHash:      types.EmptyRootHash,
	// 		ReceiptHash: types.EmptyRootHash,
	// 		GasLimit:    10000000,
	// 		GasUsed:     0,
	// 		Time:        parent.Time + 12,
	// 		Root:        rootHash,
	// 	},
	// 	[]types.Transaction{
	// 		// transaction,
	// 	},
	// 	[]*types.Header{},
	// 	[]*types.Receipt{},
	// 	[]*types.Withdrawal{})
}

func TestExecutionModuleInitialization(t *testing.T) {
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			executionModule, _, silkwormInstance := setup(t, tc.useSilkworm)
			require.NotNil(t, executionModule)

			teardown(executionModule, silkwormInstance)
		})
	}
}

func TestExecutionModuleBlockInsertion(t *testing.T) {
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			executionModule, genesisBlock, silkwormInstance := setup(t, tc.useSilkworm)

			newBlock := SampleBlock(genesisBlock.Header(), block1RootHash)

			request := &execution.InsertBlocksRequest{
				Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{newBlock}),
			}

			result, err := executionModule.InsertBlocks(executionModule.bacgroundCtx, request)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, result.Result, execution.ExecutionStatus_Success)

			teardown(executionModule, silkwormInstance)
		})
	}
}

func TestExecutionModuleValidateChainSingleBlock(t *testing.T) {
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			executionModule, genesisBlock, silkwormInstance := setup(t, tc.useSilkworm)

			newBlock := SampleBlock(genesisBlock.Header(), block1RootHash)

			request := &execution.InsertBlocksRequest{
				Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{newBlock}),
			}

			result, err := executionModule.InsertBlocks(executionModule.bacgroundCtx, request)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, result.Result, execution.ExecutionStatus_Success)

			validationRequest := &execution.ValidationRequest{
				Hash:   gointerfaces.ConvertHashToH256(newBlock.Hash()),
				Number: newBlock.Number().Uint64(),
			}

			validationResult, err := executionModule.ValidateChain(executionModule.bacgroundCtx, validationRequest)
			require.NoError(t, err)
			require.NotNil(t, validationResult)
			require.Equal(t, validationResult.ValidationStatus, execution.ExecutionStatus_Success)

			teardown(executionModule, silkwormInstance)
		})
	}
}

func TestExecutionModuleForkchoiceUpdateSingleBlock(t *testing.T) {
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			executionModule, genesisBlock, silkwormInstance := setup(t, tc.useSilkworm)

			newBlock := SampleBlock(genesisBlock.Header(), block1RootHash)

			request := &execution.InsertBlocksRequest{
				Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{newBlock}),
			}

			result, err := executionModule.InsertBlocks(executionModule.bacgroundCtx, request)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, result.Result, execution.ExecutionStatus_Success)

			validationRequest := &execution.ValidationRequest{
				Hash:   gointerfaces.ConvertHashToH256(newBlock.Hash()),
				Number: newBlock.Number().Uint64(),
			}

			validationResult, err := executionModule.ValidateChain(executionModule.bacgroundCtx, validationRequest)
			require.NoError(t, err)
			require.NotNil(t, validationResult)
			require.Equal(t, validationResult.ValidationStatus, execution.ExecutionStatus_Success)

			forkchoiceRequest := &execution.ForkChoice{
				HeadBlockHash:      gointerfaces.ConvertHashToH256(newBlock.Hash()),
				Timeout:            10_000,
				FinalizedBlockHash: gointerfaces.ConvertHashToH256(genesisBlock.Hash()),
				SafeBlockHash:      gointerfaces.ConvertHashToH256(genesisBlock.Hash()),
			}

			fcuReceipt, err := executionModule.UpdateForkChoice(executionModule.bacgroundCtx, forkchoiceRequest)
			require.NoError(t, err)
			require.NotNil(t, fcuReceipt)
			require.Equal(t, fcuReceipt.Status, execution.ExecutionStatus_Success)
			require.Equal(t, "", fcuReceipt.ValidationError)
			require.Equal(t, fcuReceipt.LatestValidHash, gointerfaces.ConvertHashToH256(newBlock.Hash()))

			teardown(executionModule, silkwormInstance)
		})
	}
}

func TestExecutionModuleForkchoiceUpdateNoPreviousVerify(t *testing.T) {
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			executionModule, genesisBlock, silkwormInstance := setup(t, tc.useSilkworm)

			newBlock := SampleBlock(genesisBlock.Header(), block1RootHash)

			request := &execution.InsertBlocksRequest{
				Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{newBlock}),
			}

			result, err := executionModule.InsertBlocks(executionModule.bacgroundCtx, request)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, result.Result, execution.ExecutionStatus_Success)

			forkchoiceRequest := &execution.ForkChoice{
				HeadBlockHash:      gointerfaces.ConvertHashToH256(newBlock.Hash()),
				Timeout:            10_000,
				FinalizedBlockHash: gointerfaces.ConvertHashToH256(genesisBlock.Hash()),
				SafeBlockHash:      gointerfaces.ConvertHashToH256(genesisBlock.Hash()),
			}

			fcuReceipt, err := executionModule.UpdateForkChoice(executionModule.bacgroundCtx, forkchoiceRequest)
			require.NoError(t, err)
			require.NotNil(t, fcuReceipt)
			require.Equal(t, fcuReceipt.Status, execution.ExecutionStatus_Success)

			teardown(executionModule, silkwormInstance)
		})
	}
}

func BenchmarkExecutionModuleValidateSingleBlock(b *testing.B) {
	for name, tc := range testCases {
		b.Run(name, func(b *testing.B) {
			executionModule, genesisBlock, silkwormInstance := setup(b, tc.useSilkworm)

			blocks := []*types.Block{
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
				SampleBlock(genesisBlock.Header(), block1RootHash),
			}

			for _, block := range blocks {
				request := &execution.InsertBlocksRequest{
					Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{block}),
				}

				result, err := executionModule.InsertBlocks(executionModule.bacgroundCtx, request)
				require.NoError(b, err)
				require.NotNil(b, result)
				require.Equal(b, result.Result, execution.ExecutionStatus_Success)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {

				selectedBlockNo := i % len(blocks)

				validationRequest := &execution.ValidationRequest{
					Hash:   gointerfaces.ConvertHashToH256(blocks[selectedBlockNo].Hash()),
					Number: blocks[0].Number().Uint64(),
				}

				validationResult, err := executionModule.ValidateChain(executionModule.bacgroundCtx, validationRequest)
				require.NoError(b, err)
				require.NotNil(b, validationResult)
				require.Equal(b, validationResult.ValidationStatus, execution.ExecutionStatus_Success)
			}

			teardown(executionModule, silkwormInstance)
		})
	}
}

func BenchmarkExecutionModuleInsertValidateFcu(b *testing.B) {
	for name, tc := range testCases {
		b.Run(name, func(b *testing.B) {
			executionModule, genesisBlock, silkwormInstance := setup(b, tc.useSilkworm)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				//insert block
				block := SampleBlock(genesisBlock.Header(), block1RootHash)
				request := &execution.InsertBlocksRequest{
					Blocks: eth1_utils.ConvertBlocksToRPC([]*types.Block{block}),
				}
				result, err := executionModule.InsertBlocks(executionModule.bacgroundCtx, request)
				require.NoError(b, err)
				require.NotNil(b, result)
				require.Equal(b, execution.ExecutionStatus_Success, result.Result)

				//validate block
				validationRequest := &execution.ValidationRequest{
					Hash:   gointerfaces.ConvertHashToH256(block.Hash()),
					Number: block.Number().Uint64(),
				}
				validationResult, err := executionModule.ValidateChain(executionModule.bacgroundCtx, validationRequest)
				require.NoError(b, err)
				require.NotNil(b, validationResult)
				require.Equal(b, execution.ExecutionStatus_Success, validationResult.ValidationStatus)

				//update forkchoice
				forkchoiceRequest := &execution.ForkChoice{
					HeadBlockHash:      gointerfaces.ConvertHashToH256(block.Hash()),
					Timeout:            10_000,
					FinalizedBlockHash: gointerfaces.ConvertHashToH256(genesisBlock.Hash()),
					SafeBlockHash:      gointerfaces.ConvertHashToH256(genesisBlock.Hash()),
				}

				fcuReceipt, err := executionModule.UpdateForkChoice(executionModule.bacgroundCtx, forkchoiceRequest)
				require.NoError(b, err)
				require.NotNil(b, fcuReceipt)
				require.Equal(b, execution.ExecutionStatus_Success, fcuReceipt.Status)

				//wait until execution module is ready
				for {
					ready, err := executionModule.Ready(executionModule.bacgroundCtx, &emptypb.Empty{})
					require.NoError(b, err)
					if ready.Ready {
						break
					}
				}
			}
			teardown(executionModule, silkwormInstance)
		})
	}
}
