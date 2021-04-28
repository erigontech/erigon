// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package core implements the Ethereum consensus protocol.
package core

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/mclock"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/params"
)

var (
	//headBlockGauge = metrics.NewRegisteredGauge("chain/head/block", nil)
	//headHeaderGauge    = metrics.NewRegisteredGauge("chain/head/header", nil)
	//headFastBlockGauge = metrics.NewRegisteredGauge("chain/head/receipt", nil)

	//accountReadTimer   = metrics.NewRegisteredTimer("chain/account/reads", nil)
	//accountHashTimer   = metrics.NewRegisteredTimer("chain/account/hashes", nil)
	//accountUpdateTimer = metrics.NewRegisteredTimer("chain/account/updates", nil)
	//accountCommitTimer = metrics.NewRegisteredTimer("chain/account/commits", nil)
	//
	//storageReadTimer   = metrics.NewRegisteredTimer("chain/storage/reads", nil)
	//storageHashTimer   = metrics.NewRegisteredTimer("chain/storage/hashes", nil)
	//storageUpdateTimer = metrics.NewRegisteredTimer("chain/storage/updates", nil)
	//storageCommitTimer = metrics.NewRegisteredTimer("chain/storage/commits", nil)
	//
	//blockInsertTimer     = metrics.NewRegisteredTimer("chain/inserts", nil)
	//blockValidationTimer = metrics.NewRegisteredTimer("chain/validation", nil)
	blockExecutionTimer = metrics.NewRegisteredTimer("chain/execution", nil)
	//blockExecutionNumber = metrics.NewRegisteredGauge("chain/execution/number", nil)
	//blockWriteTimer      = metrics.NewRegisteredTimer("chain/write", nil)

	//blockReorgMeter         = metrics.NewRegisteredMeter("chain/reorg/executes", nil)
	//blockReorgAddMeter      = metrics.NewRegisteredMeter("chain/reorg/add", nil)
	//blockReorgDropMeter     = metrics.NewRegisteredMeter("chain/reorg/drop", nil)
	blockReorgInvalidatedTx = metrics.NewRegisteredMeter("chain/reorg/invalidTx", nil)

	//blockPrefetchExecuteTimer   = metrics.NewRegisteredTimer("chain/prefetch/executes", nil)
	//blockPrefetchInterruptMeter = metrics.NewRegisteredMeter("chain/prefetch/interrupts", nil)

	//errInsertionInterrupted = errors.New("insertion is interrupted")

	// ErrNotFound is returned when sought data isn't found.
	//ErrNotFound = errors.New("data not found")
)

const (
	receiptsCacheLimit = 32
	maxFutureBlocks    = 256
	TriesInMemory      = 128
)

// CacheConfig contains the configuration values for the trie caching/pruning
// that's resident in a blockchain.
type CacheConfig struct {
	Pruning bool

	BlocksBeforePruning uint64
	BlocksToPrune       uint64
	PruneTimeout        time.Duration
	DownloadOnly        bool
	NoHistory           bool
}

// defaultCacheConfig are the default caching values if none are specified by the
// user (also used during testing).
var defaultCacheConfig = &CacheConfig{
	Pruning:             false,
	BlocksBeforePruning: 1024,
	DownloadOnly:        false,
	NoHistory:           false,
}

// BlockChain represents the canonical chain given a database with a genesis
// block. The Blockchain manages chain imports, reverts, chain reorganisations.
//
// Importing blocks in to the block chain happens according to the set of rules
// defined by the two stage Validator. Processing of blocks is done using the
// Processor which processes the included transaction. The validation of the state
// is done in the second part of the Validator. Failing results in aborting of
// the import.
//
// The BlockChain also helps in returning blocks from **any** chain included
// in the database as well as blocks that represents the canonical chain. It's
// important to note that GetBlock can return any block and does not need to be
// included in the canonical one where as GetBlockByNumber always represents the
// canonical chain.
type BlockChain struct {
	chainConfig *params.ChainConfig // Chain & network configuration
	cacheConfig *CacheConfig        // Cache configuration for pruning

	db ethdb.Database // Low level persistent database to store final content in

	hc            *HeaderChain
	rmLogsFeed    event.Feed
	chainFeed     event.Feed
	chainSideFeed event.Feed
	chainHeadFeed event.Feed
	logsFeed      event.Feed
	blockProcFeed event.Feed
	scope         event.SubscriptionScope
	genesisBlock  *types.Block

	Chainmu sync.RWMutex // blockchain insertion lock

	currentBlock atomic.Value // Current head of the block chain

	receiptsCache *lru.Cache // Cache for the most recent receipts per block
	futureBlocks  *lru.Cache // future blocks are blocks added for later processing

	quit    chan struct{}  // blockchain quit channel
	wg      sync.WaitGroup // chain processing wait group for shutting down
	running int32          // 0 if chain is running, 1 when stopped (must be called atomically)
	quitMu  sync.RWMutex

	engine   consensus.Engine
	vmConfig vm.Config

	shouldPreserve      func(*types.Block) bool        // Function used to determine whether should preserve the given block.
	TerminateInsert     func(common.Hash, uint64) bool // Testing hook used to terminate ancient receipt chain insertion.
	enableReceipts      bool                           // Whether receipts need to be written to the database
	enableTxLookupIndex bool                           // Whether we store tx lookup index into the database
	enablePreimages     bool                           // Whether we store preimages into the database
	resolveReads        bool
	pruner              Pruner

	senderCacher *TxSenderCacher
}

// NewBlockChain returns a fully initialised block chain using information
// available in the database. It initialises the default Ethereum Validator and
// Processor.
func NewBlockChain(db ethdb.Database, cacheConfig *CacheConfig, chainConfig *params.ChainConfig, engine consensus.Engine, vmConfig vm.Config, shouldPreserve func(block *types.Block) bool, senderCacher *TxSenderCacher) (*BlockChain, error) {
	if cacheConfig == nil {
		cacheConfig = defaultCacheConfig
	}

	receiptsCache, _ := lru.New(receiptsCacheLimit)
	futureBlocks, _ := lru.New(maxFutureBlocks)

	bc := &BlockChain{
		chainConfig:         chainConfig,
		cacheConfig:         cacheConfig,
		db:                  db,
		quit:                make(chan struct{}),
		shouldPreserve:      shouldPreserve,
		receiptsCache:       receiptsCache,
		futureBlocks:        futureBlocks,
		engine:              engine,
		vmConfig:            vmConfig,
		enableTxLookupIndex: true,
		enableReceipts:      false,
		enablePreimages:     true,
		senderCacher:        senderCacher,
	}

	var err error
	bc.hc, err = NewHeaderChain(db, chainConfig, engine)
	if err != nil {
		return nil, err
	}
	bc.genesisBlock = bc.GetBlockByNumber(0)
	if bc.genesisBlock == nil {
		return nil, ErrNoGenesis
	}

	var nilBlock *types.Block
	bc.currentBlock.Store(nilBlock)

	if err := bc.loadLastState(); err != nil {
		log.Error("loadLoadState", "err", err)
	}
	return bc, nil
}

func (bc *BlockChain) SetResolveReads(rr bool) {
	bc.resolveReads = rr
}

func (bc *BlockChain) EnableReceipts(er bool) {
	bc.enableReceipts = er
}

func (bc *BlockChain) EnableTxLookupIndex(et bool) {
	bc.enableTxLookupIndex = et
}

func (bc *BlockChain) EnablePreimages(ep bool) {
	bc.enablePreimages = ep
}

// GetVMConfig returns the block chain VM config.
func (bc *BlockChain) GetVMConfig() *vm.Config {
	return &bc.vmConfig
}

// loadLastState loads the last known chain state from the database. This method
// assumes that the chain manager mutex is held.
func (bc *BlockChain) loadLastState() error {
	// Restore the last known head block
	head := rawdb.ReadHeadBlockHash(bc.db)
	if head == (common.Hash{}) {
		return fmt.Errorf("empty or corrupt database")
	}
	// Make sure the entire head block is available
	currentBlock, err := rawdb.ReadBlockByHash(bc.db, head)
	if err != nil {
		return err
	}
	if currentBlock == nil {
		// Corrupt or empty database, init from scratch
		return fmt.Errorf("head block missing, hash %x", head)
	}
	// Make sure the state associated with the block is available
	// Everything seems to be fine, set as the head block
	bc.currentBlock.Store(currentBlock)
	//headBlockGauge.Update(int64(currentBlock.NumberU64()))

	// Restore the last known head header
	currentHeader := currentBlock.Header()
	if head := rawdb.ReadHeadHeaderHash(bc.db); head != (common.Hash{}) {
		if header, _ := rawdb.ReadHeaderByHash(bc.db, head); header != nil {
			currentHeader = header
		}
	}
	bc.hc.SetCurrentHeader(bc.db, currentHeader)
	// Issue a status log for the user

	headerTd, _ := rawdb.ReadTd(bc.db, currentHeader.Hash(), currentHeader.Number.Uint64())
	blockTd, _ := rawdb.ReadTd(bc.db, currentBlock.Hash(), currentBlock.NumberU64())

	log.Info("Most recent local header", "number", currentHeader.Number, "hash", currentHeader.Hash(), "td", headerTd, "age", common.PrettyAge(time.Unix(int64(currentHeader.Time), 0)))
	log.Info("Most recent local block", "number", currentBlock.Number(), "hash", currentBlock.Hash(), "td", blockTd, "age", common.PrettyAge(time.Unix(int64(currentBlock.Time()), 0)))

	return nil
}

// CurrentBlock retrieves the current head block of the canonical chain. The
// block is retrieved from the blockchain's internal cache.
func (bc *BlockChain) CurrentBlock() *types.Block {
	headHash := rawdb.ReadHeadBlockHash(bc.db)
	headNumber := rawdb.ReadHeaderNumber(bc.db, headHash)
	return rawdb.ReadBlock(bc.db, headHash, *headNumber)
}

// Genesis retrieves the chain's genesis block.
func (bc *BlockChain) Genesis() *types.Block {
	return bc.genesisBlock
}

// GetBlock retrieves a block from the database by hash and number,
// caching it if found.
func (bc *BlockChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	// Short circuit if the block's already in the cache, retrieve otherwise
	block := rawdb.ReadBlock(bc.db, hash, number)
	if block == nil {
		return nil
	}
	return block
}

// GetBlockByHash retrieves a block from the database by hash, caching it if found.
func (bc *BlockChain) GetBlockByHash(hash common.Hash) *types.Block {
	number := bc.hc.GetBlockNumber(bc.db, hash)
	if number == nil {
		return nil
	}
	return bc.GetBlock(hash, *number)
}

// GetBlockByNumber retrieves a block from the database by number, caching it
// (associated with its hash) if found.
func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block {
	hash, err := rawdb.ReadCanonicalHash(bc.db, number)
	if err != nil {
		log.Warn("ReadCanonicalHash failed", "err", err)
		return nil
	}

	if hash == (common.Hash{}) {
		return nil
	}
	return bc.GetBlock(hash, number)
}

// GetReceiptsByHash retrieves the receipts for all transactions in a given block.
func (bc *BlockChain) GetReceiptsByHash(hash common.Hash) types.Receipts {
	if receipts, ok := bc.receiptsCache.Get(hash); ok {
		return receipts.(types.Receipts)
	}
	number := rawdb.ReadHeaderNumber(bc.db, hash)
	if number == nil {
		return nil
	}
	receipts := rawdb.ReadReceipts(bc.db, hash, *number)
	if receipts == nil {
		return nil
	}
	bc.receiptsCache.Add(hash, receipts)
	return receipts
}

// Stop stops the blockchain service. If any imports are currently in progress
// it will abort them using the procInterrupt.
func (bc *BlockChain) Stop() {
	if !atomic.CompareAndSwapInt32(&bc.running, 0, 1) {
		return
	}
	// Unsubscribe all subscriptions registered from blockchain
	bc.scope.Close()
	close(bc.quit)

	bc.quitMu.Lock()
	bc.wg.Wait()
	bc.quitMu.Unlock()

	if bc.pruner != nil {
		bc.pruner.Stop()
	}
	if bc.senderCacher != nil {
		bc.senderCacher.Close()
	}
	log.Info("Blockchain stopped")
}

// statsReportLimit is the time limit during import and export after which we
// always print out progress. This avoids the user wondering what's going on.
const statsReportLimit = 8 * time.Second
const commitLimit = 60 * time.Second

func (st *InsertStats) NeedToCommit(chain []*types.Block, index int) bool {
	var (
		now     = mclock.Now()
		elapsed = time.Duration(now) - time.Duration(st.StartTime)
	)
	if index == len(chain)-1 || elapsed >= commitLimit {
		return true
	}
	return false
}

// report prints statistics if some number of blocks have been processed
// or more than a few seconds have passed since the last message.
func (st *InsertStats) Report(logPrefix string, chain []*types.Block, index int, toCommit bool) {
	// Fetch the timings for the batch
	var (
		now     = mclock.Now()
		elapsed = time.Duration(now) - time.Duration(st.StartTime)
	)
	// If we're at the last block of the batch or report period reached, log
	if index == len(chain)-1 || elapsed >= statsReportLimit || toCommit {
		// Count the number of transactions in this segment
		var txs int
		for _, block := range chain[st.lastIndex : index+1] {
			txs += len(block.Transactions())
		}
		end := chain[index]
		context := []interface{}{
			"blocks", st.Processed, "txs", txs,
			"elapsed", common.PrettyDuration(elapsed),
			"number", end.Number(), "hash", end.Hash(),
		}
		if timestamp := time.Unix(int64(end.Time()), 0); time.Since(timestamp) > time.Minute {
			context = append(context, []interface{}{"age", common.PrettyAge(timestamp)}...)
		}
		if st.queued > 0 {
			context = append(context, []interface{}{"queued", st.queued}...)
		}
		if st.ignored > 0 {
			context = append(context, []interface{}{"ignored", st.ignored}...)
		}
		log.Info(fmt.Sprintf("[%s] Imported new chain segment", logPrefix), context...)
		*st = InsertStats{StartTime: now, lastIndex: index + 1}
	}
}

// reportBlock logs a bad block error.
func (bc *BlockChain) ReportBlock(block *types.Block, receipts types.Receipts, err error) {
	rawdb.WriteBadBlock(bc.db, block)

	var receiptString string
	for i, receipt := range receipts {
		receiptString += fmt.Sprintf("\t %d: cumulative: %v gas: %v contract: %v status: %v tx: %v logs: %v bloom: %x state: %x\n",
			i, receipt.CumulativeGasUsed, receipt.GasUsed, receipt.ContractAddress.Hex(),
			receipt.Status, receipt.TxHash.Hex(), receipt.Logs, receipt.Bloom, receipt.PostState)
	}
	log.Error(fmt.Sprintf(`
########## BAD BLOCK #########
Chain config: %v

Number: %v
Hash: 0x%x
%v

Error: %v
Callers: %v
##############################
`, bc.chainConfig, block.Number(), block.Hash(), receiptString, err, debug.Callers(20)))
}

// CurrentHeader retrieves the current head header of the canonical chain. The
// header is retrieved from the HeaderChain's internal cache.
func (bc *BlockChain) CurrentHeader() *types.Header {
	return bc.hc.CurrentHeader()
}

// GetTd retrieves a block's total difficulty in the canonical chain from the
// database by hash and number, caching it if found.
func (bc *BlockChain) GetTd(hash common.Hash, number uint64) *big.Int {
	return bc.hc.GetTd(bc.db, hash, number)
}

// GetTdByHash retrieves a block's total difficulty in the canonical chain from the
// database by hash, caching it if found.
func (bc *BlockChain) GetTdByHash(hash common.Hash) *big.Int {
	return bc.hc.GetTdByHash(hash)
}

// GetHeader retrieves a block header from the database by hash and number,
// caching it if found.
func (bc *BlockChain) GetHeader(hash common.Hash, number uint64) *types.Header {
	return bc.hc.GetHeader(hash, number)
}

// GetHeaderByHash retrieves a block header from the database by hash, caching it if
// found.
func (bc *BlockChain) GetHeaderByHash(hash common.Hash) *types.Header {
	return bc.hc.GetHeaderByHash(hash)
}

// HasHeader checks if a block header is present in the database or not, caching
// it if present.
func (bc *BlockChain) HasHeader(hash common.Hash, number uint64) bool {
	return bc.hc.HasHeader(hash, number)
}

// GetCanonicalHash returns the canonical hash for a given block number
func (bc *BlockChain) GetCanonicalHash(number uint64) common.Hash {
	return bc.hc.GetCanonicalHash(number)
}

// GetBlockHashesFromHash retrieves a number of block hashes starting at a given
// hash, fetching towards the genesis block.
func (bc *BlockChain) GetBlockHashesFromHash(hash common.Hash, max uint64) []common.Hash {
	return bc.hc.GetBlockHashesFromHash(hash, max)
}

// GetHeaderByNumber retrieves a block header from the database by number,
// caching it (associated with its hash) if found.
func (bc *BlockChain) GetHeaderByNumber(number uint64) *types.Header {
	return bc.hc.GetHeaderByNumber(number)
}

// Config retrieves the chain's fork configuration.
func (bc *BlockChain) Config() *params.ChainConfig { return bc.chainConfig }

// Engine retrieves the blockchain's consensus engine.
func (bc *BlockChain) Engine() consensus.Engine { return bc.engine }

func (bc *BlockChain) SetEngine(engine consensus.Engine) {
	bc.engine = engine
}

// SubscribeRemovedLogsEvent registers a subscription of RemovedLogsEvent.
func (bc *BlockChain) SubscribeRemovedLogsEvent(ch chan<- RemovedLogsEvent) event.Subscription {
	return bc.scope.Track(bc.rmLogsFeed.Subscribe(ch))
}

// SubscribeChainEvent registers a subscription of ChainEvent.
func (bc *BlockChain) SubscribeChainEvent(ch chan<- ChainEvent) event.Subscription {
	return bc.scope.Track(bc.chainFeed.Subscribe(ch))
}

// SubscribeChainHeadEvent registers a subscription of ChainHeadEvent.
func (bc *BlockChain) SubscribeChainHeadEvent(ch chan<- ChainHeadEvent) event.Subscription {
	return bc.scope.Track(bc.chainHeadFeed.Subscribe(ch))
}

// SubscribeChainSideEvent registers a subscription of ChainSideEvent.
func (bc *BlockChain) SubscribeChainSideEvent(ch chan<- ChainSideEvent) event.Subscription {
	return bc.scope.Track(bc.chainSideFeed.Subscribe(ch))
}

// SubscribeLogsEvent registers a subscription of []*types.Log.
func (bc *BlockChain) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	return bc.scope.Track(bc.logsFeed.Subscribe(ch))
}

// SubscribeBlockProcessingEvent registers a subscription of bool where true means
// block processing has started while false means it has stopped.
func (bc *BlockChain) SubscribeBlockProcessingEvent(ch chan<- bool) event.Subscription {
	return bc.scope.Track(bc.blockProcFeed.Subscribe(ch))
}

func (bc *BlockChain) ChainDb() ethdb.Database {
	return bc.db
}

func (bc *BlockChain) NoHistory() bool {
	return bc.cacheConfig.NoHistory
}

type Pruner interface {
	Start() error
	Stop()
}

// ExecuteBlockEphemerally runs a block from provided stateReader and
// writes the result to the provided stateWriter
func ExecuteBlockEphemerally(
	chainConfig *params.ChainConfig,
	vmConfig *vm.Config,
	getHeader func(hash common.Hash, number uint64) *types.Header,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
) (types.Receipts, error) {
	defer blockExecutionTimer.UpdateSince(time.Now())
	block.Uncles()
	ibs := state.New(stateReader)
	header := block.Header()
	var receipts types.Receipts
	usedGas := new(uint64)
	gp := new(GasPool)
	if chainConfig.IsAleut(block.NumberU64()) {
		gp.AddGas(block.GasLimit() * params.ElasticityMultiplier)
	} else {
		gp.AddGas(block.GasLimit())
	}

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	noop := state.NewNoopWriter()
	for i, tx := range block.Transactions() {
		if !vmConfig.NoReceipts {
			ibs.Prepare(tx.Hash(), block.Hash(), i)
		}
		writeTrace := false
		if vmConfig.Debug && vmConfig.Tracer == nil {
			vmConfig.Tracer = vm.NewStructLogger(&vm.LogConfig{})
			writeTrace = true
		}

		receipt, err := ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, noop, header, tx, usedGas, *vmConfig)
		if writeTrace {
			w, err1 := os.Create(fmt.Sprintf("txtrace_%x.txt", tx.Hash()))
			if err1 != nil {
				panic(err1)
			}
			encoder := json.NewEncoder(w)
			logs := FormatLogs(vmConfig.Tracer.(*vm.StructLogger).StructLogs())
			if err2 := encoder.Encode(logs); err2 != nil {
				panic(err2)
			}
			if err2 := w.Close(); err2 != nil {
				panic(err2)
			}
			vmConfig.Tracer = nil
		}
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d from block %d [%v]: %w", i, block.NumberU64(), tx.Hash().Hex(), err)
		}
		if !vmConfig.NoReceipts {
			receipts = append(receipts, receipt)
		}
	}

	if chainConfig.IsByzantium(header.Number.Uint64()) && !vmConfig.NoReceipts {
		receiptSha := types.DeriveSha(receipts)
		if receiptSha != block.Header().ReceiptHash {
			return nil, fmt.Errorf("mismatched receipt headers for block %d", block.NumberU64())
		}
	}

	if !vmConfig.ReadOnly {
		if err := FinalizeBlockExecution(engine, block.Header(), block.Transactions(), block.Uncles(), stateWriter, chainConfig, ibs); err != nil {
			return nil, err
		}
	}
	if *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}
	if !vmConfig.NoReceipts {
		bloom := types.CreateBloom(receipts)
		if bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}

	return receipts, nil
}

func FinalizeBlockExecution(engine consensus.Engine, header *types.Header, txs types.Transactions, uncles []*types.Header, stateWriter state.WriterWithChangeSets, cc *params.ChainConfig, ibs *state.IntraBlockState) error {
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	engine.Finalize(cc, header, ibs, txs, uncles)

	ctx := cc.WithEIPsFlags(context.Background(), header.Number.Uint64())
	if err := ibs.CommitBlock(ctx, stateWriter); err != nil {
		return fmt.Errorf("committing block %d failed: %v", header.Number.Uint64(), err)
	}
	if err := stateWriter.WriteChangeSets(); err != nil {
		return fmt.Errorf("writing changesets for block %d failed: %v", header.Number.Uint64(), err)
	}
	return nil
}
