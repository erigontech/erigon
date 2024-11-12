package witness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	eritypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/trie"
	dstypes "github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/l1_data"
	zkStages "github.com/ledgerwatch/erigon/zk/stages"
	zkUtils "github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"
)

var (
	maxGetProofRewindBlockCount uint64 = 500_000

	ErrEndBeforeStart = errors.New("end block must be higher than start block")
)

type Generator struct {
	tx          kv.Tx
	dirs        datadir.Dirs
	historyV3   bool
	agg         *libstate.Aggregator
	blockReader services.FullBlockReader
	chainCfg    *chain.Config
	zkConfig    *ethconfig.Zk
	engine      consensus.EngineReader
}

func NewGenerator(
	dirs datadir.Dirs,
	historyV3 bool,
	agg *libstate.Aggregator,
	blockReader services.FullBlockReader,
	chainCfg *chain.Config,
	zkConfig *ethconfig.Zk,
	engine consensus.EngineReader,
) *Generator {
	return &Generator{
		dirs:        dirs,
		historyV3:   historyV3,
		agg:         agg,
		blockReader: blockReader,
		chainCfg:    chainCfg,
		zkConfig:    zkConfig,
		engine:      engine,
	}
}

func (g *Generator) GetWitnessByBatch(tx kv.Tx, ctx context.Context, batchNum uint64, debug, witnessFull bool) (witness []byte, err error) {
	t := zkUtils.StartTimer("witness", "getwitnessbybatch")
	defer t.LogTimer()

	reader := hermez_db.NewHermezDbReader(tx)
	badBatch, err := reader.GetInvalidBatch(batchNum)
	if err != nil {
		return nil, err
	}
	if badBatch {
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
	} else {
		blockNumbers, err := reader.GetL2BlockNosByBatch(batchNum)
		if err != nil {
			return nil, err
		}
		if len(blockNumbers) == 0 {
			return nil, fmt.Errorf("no blocks found for batch %d", batchNum)
		}
		blocks := make([]*eritypes.Block, len(blockNumbers))
		idx := 0
		for _, blockNum := range blockNumbers {
			block, err := rawdb.ReadBlockByNumber(tx, blockNum)
			if err != nil {
				return nil, err
			}
			blocks[idx] = block
			idx++
		}
		return g.generateWitness(tx, ctx, batchNum, blocks, debug, witnessFull)
	}
}

func (g *Generator) GetWitnessByBlockRange(tx kv.Tx, ctx context.Context, startBlock, endBlock uint64, debug, witnessFull bool) ([]byte, error) {
	t := zkUtils.StartTimer("witness", "getwitnessbyblockrange")
	defer t.LogTimer()

	if startBlock > endBlock {
		return nil, ErrEndBeforeStart
	}
	if endBlock == 0 {
		witness := trie.NewWitness([]trie.WitnessOperator{})
		return getWitnessBytes(witness, debug)
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

	batch := membatchwithdb.NewMemoryBatchWithSize(tx, g.dirs.Tmp, g.zkConfig.WitnessMemdbSize)
	defer batch.Rollback()
	if err = zkUtils.PopulateMemoryMutationTables(batch); err != nil {
		return nil, err
	}

	sBlock := blocks[0]
	if sBlock == nil {
		return nil, nil
	}

	if startBlock-1 < latestBlock {
		if latestBlock-startBlock > maxGetProofRewindBlockCount {
			return nil, fmt.Errorf("requested block is too old, block must be within %d blocks of the head block number (currently %d)", maxGetProofRewindBlockCount, latestBlock)
		}

		unwindState := &stagedsync.UnwindState{UnwindPoint: startBlock - 1}
		stageState := &stagedsync.StageState{BlockNumber: latestBlock}

		hashStageCfg := stagedsync.StageHashStateCfg(nil, g.dirs, g.historyV3, g.agg)
		if err := stagedsync.UnwindHashStateStage(unwindState, stageState, batch, hashStageCfg, ctx, log.New(), true); err != nil {
			return nil, fmt.Errorf("unwind hash state: %w", err)
		}

		interHashStageCfg := zkStages.StageZkInterHashesCfg(nil, true, true, false, g.dirs.Tmp, g.blockReader, nil, g.historyV3, g.agg, nil)

		if err = zkStages.UnwindZkIntermediateHashesStage(unwindState, stageState, batch, interHashStageCfg, ctx, true); err != nil {
			return nil, fmt.Errorf("unwind intermediate hashes: %w", err)
		}

		tx = batch
	}

	prevHeader, err := g.blockReader.HeaderByNumber(ctx, tx, startBlock-1)
	if err != nil {
		return nil, err
	}

	tds := state.NewTrieDbState(prevHeader.Root, tx, startBlock-1, nil)
	tds.SetResolveReads(true)
	tds.StartNewBuffer()
	trieStateWriter := tds.TrieStateWriter()

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

	for _, block := range blocks {
		blockNum := block.NumberU64()
		reader.SetBlockNr(blockNum)

		tds.SetStateReader(reader)

		hermezDb := hermez_db.NewHermezDbReader(tx)

		//[zkevm] get batches between last block and this one
		// plus this blocks ger
		lastBatchInserted, err := hermezDb.GetBatchNoByL2Block(blockNum - 1)
		if err != nil {
			return nil, fmt.Errorf("failed to get batch for block %d: %v", blockNum-1, err)
		}

		currentBatch, err := hermezDb.GetBatchNoByL2Block(blockNum)
		if err != nil {
			return nil, fmt.Errorf("failed to get batch for block %d: %v", blockNum, err)
		}

		gersInBetween, err := hermezDb.GetBatchGlobalExitRoots(lastBatchInserted, currentBatch)
		if err != nil {
			return nil, err
		}

		var globalExitRoots []dstypes.GerUpdate

		if gersInBetween != nil {
			globalExitRoots = append(globalExitRoots, *gersInBetween...)
		}

		blockGer, err := hermezDb.GetBlockGlobalExitRoot(blockNum)
		if err != nil {
			return nil, err
		}
		emptyHash := libcommon.Hash{}

		if blockGer != emptyHash {
			blockGerUpdate := dstypes.GerUpdate{
				GlobalExitRoot: blockGer,
				Timestamp:      block.Header().Time,
			}
			globalExitRoots = append(globalExitRoots, blockGerUpdate)
		}

		for _, ger := range globalExitRoots {
			// [zkevm] - add GER if there is one for this batch
			if err := zkUtils.WriteGlobalExitRoot(tds, trieStateWriter, ger.GlobalExitRoot, ger.Timestamp); err != nil {
				return nil, err
			}
		}

		engine, ok := g.engine.(consensus.Engine)

		if !ok {
			return nil, fmt.Errorf("engine is not consensus.Engine")
		}

		vmConfig := vm.Config{}

		getHashFn := core.GetHashFn(block.Header(), getHeader)

		chainReader := stagedsync.NewChainReaderImpl(g.chainCfg, tx, nil, log.New())

		_, err = core.ExecuteBlockEphemerallyZk(g.chainCfg, &vmConfig, getHashFn, engine, block, tds, trieStateWriter, chainReader, nil, hermezDb, &prevStateRoot)
		if err != nil {
			return nil, err
		}

		prevStateRoot = block.Root()
	}

	var rl trie.RetainDecider
	// if full is true, we will send all the nodes to the witness
	rl = &trie.AlwaysTrueRetainDecider{}

	if !witnessFull {
		rl, err = tds.ResolveSMTRetainList()
		if err != nil {
			return nil, err
		}
	}

	eridb := db2.NewEriDb(batch)
	smtTrie := smt.NewSMT(eridb, false)

	witness, err := smt.BuildWitness(smtTrie, rl, ctx)
	if err != nil {
		return nil, fmt.Errorf("build witness: %v", err)
	}

	return getWitnessBytes(witness, debug)
}

func getWitnessBytes(witness *trie.Witness, debug bool) ([]byte, error) {
	var buf bytes.Buffer
	_, err := witness.WriteInto(&buf, debug)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
