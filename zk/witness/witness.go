package witness

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/systemcontracts"
	eritypes "github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/trie"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/l1_data"
	zkUtils "github.com/erigontech/erigon/zk/utils"
	"github.com/iden3/go-iden3-crypto/keccak256"

	"github.com/erigontech/erigon-lib/kv/membatchwithdb"
)

var (
	ErrEndBeforeStart = errors.New("end block must be higher than start block")
)

type Generator struct {
	tx                 kv.Tx
	dirs               datadir.Dirs
	historyV3          bool
	agg                *libstate.Aggregator
	blockReader        services.FullBlockReader
	chainCfg           *chain.Config
	zkConfig           *ethconfig.Zk
	engine             consensus.EngineReader
	forcedContracts    []libcommon.Address
	witnessUnwindLimit uint64
}

func NewGenerator(
	dirs datadir.Dirs,
	historyV3 bool,
	agg *libstate.Aggregator,
	blockReader services.FullBlockReader,
	chainCfg *chain.Config,
	zkConfig *ethconfig.Zk,
	engine consensus.EngineReader,
	forcedContracs []libcommon.Address,
	witnessUnwindLimit uint64,
) *Generator {
	return &Generator{
		dirs:               dirs,
		historyV3:          historyV3,
		agg:                agg,
		blockReader:        blockReader,
		chainCfg:           chainCfg,
		zkConfig:           zkConfig,
		engine:             engine,
		forcedContracts:    forcedContracs,
		witnessUnwindLimit: witnessUnwindLimit,
	}
}

func (g *Generator) GetWitnessByBadBatch(tx kv.Tx, ctx context.Context, batchNum uint64, debug, witnessFull bool) (witness []byte, err error) {
	t := zkUtils.StartTimer("witness", "getwitnessbybadbatch")
	defer t.LogTimer()

	reader := hermez_db.NewHermezDbReader(tx)
	// we need the header of the block prior to this batch to build up the blocks
	previousHeight, _, err := reader.GetHighestBlockInBatch(batchNum - 1)
	if err != nil {
		return nil, err
	}
	previousHeader := rawdb.ReadHeaderByNumber(tx, previousHeight)
	if previousHeader == nil {
		return nil, fmt.Errorf("failed to get header for block %d", previousHeight)
	}

	// 1. get l1 batch data for the bad batch
	fork, err := reader.GetForkId(batchNum)
	if err != nil {
		return nil, err
	}

	decoded, err := l1_data.BreakDownL1DataByBatch(batchNum, fork, reader)
	if err != nil {
		return nil, err
	}

	nextNum := previousHeader.Number.Uint64()
	parentHash := previousHeader.Hash()
	timestamp := previousHeader.Time
	blocks := make([]*eritypes.Block, len(decoded.DecodedData))
	for i, d := range decoded.DecodedData {
		timestamp += uint64(d.DeltaTimestamp)
		nextNum++
		newHeader := &eritypes.Header{
			ParentHash: parentHash,
			Coinbase:   decoded.Coinbase,
			Difficulty: new(big.Int).SetUint64(0),
			Number:     new(big.Int).SetUint64(nextNum),
			GasLimit:   zkUtils.GetBlockGasLimitForFork(fork),
			Time:       timestamp,
		}

		parentHash = newHeader.Hash()
		transactions := d.Transactions
		block := eritypes.NewBlock(newHeader, transactions, nil, nil, nil)
		blocks[i] = block
	}

	return g.generateWitness(tx, ctx, batchNum, blocks, debug, witnessFull)
}

func (g *Generator) GetWitnessByBlockRange(tx kv.Tx, ctx context.Context, startBlock, endBlock uint64, debug, witnessFull bool) ([]byte, error) {
	t := zkUtils.StartTimer("witness", "getwitnessbyblockrange")
	defer t.LogTimer()

	if startBlock > endBlock {
		return nil, ErrEndBeforeStart
	}
	if endBlock == 0 {
		witness := trie.NewWitness([]trie.WitnessOperator{})
		return GetWitnessBytes(witness, debug)
	}
	hermezDb := hermez_db.NewHermezDbReader(tx)

	idx := 0
	blocks := make([]*eritypes.Block, endBlock-startBlock+1)
	var firstBatch uint64 = 0
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		block, err := rawdb.ReadBlockByNumber(tx, blockNum)
		if err != nil {
			return nil, err
		}
		firstBatch, err = hermezDb.GetBatchNoByL2Block(block.NumberU64())
		if err != nil {
			return nil, err
		}
		blocks[idx] = block
		idx++
	}

	return g.generateWitness(tx, ctx, firstBatch, blocks, debug, witnessFull)
}

func (g *Generator) generateWitness(tx kv.Tx, ctx context.Context, batchNum uint64, blocks []*eritypes.Block, debug, witnessFull bool) ([]byte, error) {
	now := time.Now()
	defer func() {
		diff := time.Since(now)
		if len(blocks) == 0 {
			return
		}
		log.Info("Generating witness timing", "batch", batchNum, "blockFrom", blocks[0].NumberU64(), "blockTo", blocks[len(blocks)-1].NumberU64(), "taken", diff)
	}()

	areExecutorUrlsEmpty := len(g.zkConfig.ExecutorUrls) == 0 || g.zkConfig.ExecutorUrls[0] == ""
	shouldGenerateMockWitness := g.zkConfig.MockWitnessGeneration && areExecutorUrlsEmpty
	if shouldGenerateMockWitness {
		return g.generateMockWitness(batchNum, blocks, debug)
	}

	endBlock := blocks[len(blocks)-1].NumberU64()
	startBlock := blocks[0].NumberU64()

	latestBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return nil, err
	}

	if latestBlock < endBlock {
		return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlock, endBlock)
	}

	rwtx := membatchwithdb.NewMemoryBatchWithSize(tx, g.dirs.Tmp, g.zkConfig.WitnessMemdbSize)
	defer rwtx.Rollback()
	if err = zkUtils.PopulateMemoryMutationTables(rwtx); err != nil {
		return nil, err
	}

	sBlock := blocks[0]
	if sBlock == nil {
		return nil, nil
	}

	if startBlock-1 < latestBlock {
		if latestBlock-startBlock > g.witnessUnwindLimit {
			return nil, fmt.Errorf("requested block is too old, block must be within %d blocks of the head block number (currently %d)", g.witnessUnwindLimit, latestBlock)
		}

		if err := UnwindForWitness(ctx, rwtx, startBlock, latestBlock, g.dirs, g.historyV3, g.agg); err != nil {
			return nil, fmt.Errorf("UnwindForWitness: %w", err)
		}

		tx = rwtx
	}

	prevHeader, err := g.blockReader.HeaderByNumber(ctx, tx, startBlock-1)
	if err != nil {
		return nil, err
	}

	tds := state.NewTrieDbState(prevHeader.Root, tx, startBlock-1, nil)
	tds.SetResolveReads(true)
	tds.StartNewBuffer()
	trieStateWriter := tds.NewTrieStateWriter()

	getHeader := func(hash libcommon.Hash, number uint64) *eritypes.Header {
		h, e := g.blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}

	prevStateRoot := prevHeader.Root

	reader := state.NewPlainState(tx, blocks[0].NumberU64(), systemcontracts.SystemContractCodeLookup[g.chainCfg.ChainName])
	defer reader.Close()

	// used to ensure that any info tree updates for this batch are included in the witness - re-use of an index for example
	// won't write to storage so will be missing from the witness but the prover needs it
	forcedInfoTreeUpdates := make([]libcommon.Hash, 0)

	for _, block := range blocks {
		blockNum := block.NumberU64()
		reader.SetBlockNr(blockNum)

		tds.SetStateReader(reader)

		hermezDb := hermez_db.NewHermezDbReader(tx)

		if err := PrepareGersForWitness(block, hermezDb, tds, trieStateWriter); err != nil {
			return nil, fmt.Errorf("PrepareGersForWitness: %w", err)
		}

		engine, ok := g.engine.(consensus.Engine)

		if !ok {
			return nil, fmt.Errorf("engine is not consensus.Engine")
		}

		getHashFn := core.GetHashFn(block.Header(), getHeader)

		chainReader := stagedsync.NewChainReaderImpl(g.chainCfg, tx, nil, log.New())

		vmConfig := vm.Config{
			StatelessExec: true,
		}
		if _, err = core.ExecuteBlockEphemerallyZk(g.chainCfg, &vmConfig, getHashFn, engine, block, tds, trieStateWriter, chainReader, nil, hermezDb, &prevStateRoot); err != nil {
			return nil, fmt.Errorf("ExecuteBlockEphemerallyZk: %w", err)
		}

		forcedInfoTreeUpdate, err := CheckForForcedInfoTreeUpdate(hermezDb, blockNum)
		if err != nil {
			return nil, fmt.Errorf("CheckForForcedInfoTreeUpdate: %w", err)
		}
		if forcedInfoTreeUpdate != nil {
			forcedInfoTreeUpdates = append(forcedInfoTreeUpdates, *forcedInfoTreeUpdate)
		}

		prevStateRoot = block.Root()
	}

	witness, err := BuildWitnessFromTrieDbState(ctx, rwtx, tds, reader, g.forcedContracts, forcedInfoTreeUpdates, witnessFull)
	if err != nil {
		return nil, fmt.Errorf("BuildWitnessFromTrieDbState: %w", err)
	}

	return GetWitnessBytes(witness, debug)
}

func (g *Generator) generateMockWitness(batchNum uint64, blocks []*eritypes.Block, debug bool) ([]byte, error) {
	mockWitness := []byte("mockWitness")
	startBlockNumber := blocks[0].NumberU64()
	endBlockNumber := blocks[len(blocks)-1].NumberU64()

	if debug {
		log.Info(
			"Generated mock witness",
			"witness", mockWitness,
			"batch", batchNum,
			"startBlockNumber", startBlockNumber,
			"endBlockNumber", endBlockNumber,
		)
	}

	return mockWitness, nil
}

func CheckForForcedInfoTreeUpdate(reader *hermez_db.HermezDbReader, blockNum uint64) (*libcommon.Hash, error) {
	// check if there were any info tree index updates for this block number
	index, err := reader.GetBlockL1InfoTreeIndex(blockNum)
	if err != nil {
		return nil, fmt.Errorf("failed to check for block info tree index: %w", err)
	}
	var result *libcommon.Hash
	if index != 0 {
		// we need to load this info tree index to get the storage slot address to force witness inclusion
		infoTreeIndex, err := reader.GetL1InfoTreeUpdate(index)
		if err != nil {
			return nil, fmt.Errorf("failed to get info tree index: %w", err)
		}
		if infoTreeIndex == nil {
			log.Warn("Info tree index not found when generating witness", "index", index, "blockNum", blockNum)
			return result, nil
		}
		d1 := common.LeftPadBytes(infoTreeIndex.GER.Bytes(), 32)
		d2 := common.LeftPadBytes(state.GLOBAL_EXIT_ROOT_STORAGE_POS.Bytes(), 32)
		mapKey := keccak256.Hash(d1, d2)
		mkh := libcommon.BytesToHash(mapKey)
		result = &mkh
	}

	return result, nil
}
