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
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/mclock"
	"github.com/ledgerwatch/turbo-geth/common/prque"
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
	"github.com/ledgerwatch/turbo-geth/rlp"
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

	errInsertionInterrupted = errors.New("insertion is interrupted")

	// ErrNotFound is returned when sought data isn't found.
	ErrNotFound = errors.New("data not found")
)

const (
	receiptsCacheLimit = 32
	maxFutureBlocks    = 256
	TriesInMemory      = 128

	// BlockChainVersion ensures that an incompatible database forces a resync from scratch.
	//
	// Changelog:
	//
	// - Version 4
	//   The following incompatible database changes were added:
	//   * the `BlockNumber`, `TxHash`, `TxIndex`, `BlockHash` and `Index` fields of log are deleted
	//   * the `Bloom` field of receipt is deleted
	//   * the `BlockIndex` and `TxIndex` fields of txlookup are deleted
	// - Version 5
	//  The following incompatible database changes were added:
	//    * the `TxHash`, `GasCost`, and `ContractAddress` fields are no longer stored for a receipt
	//    * the `TxHash`, `GasCost`, and `ContractAddress` fields are computed by looking up the
	//      receipts' corresponding block
	// - Version 6
	//  The following incompatible database changes were added:
	//    * Transaction lookup information stores the corresponding block number instead of block hash
	// - Version 7
	//  The following incompatible database changes were added:
	//    * Use freezer as the ancient database to maintain all ancient data
	// - Version 8
	//  The following incompatible database changes were added:
	//    * New scheme for contract code in order to separate the codes and trie nodes
	BlockChainVersion uint64 = 8
)

// CacheConfig contains the configuration values for the trie caching/pruning
// that's resident in a blockchain.
type CacheConfig struct {
	Pruning bool

	BlocksBeforePruning uint64
	BlocksToPrune       uint64
	PruneTimeout        time.Duration
	ArchiveSyncInterval uint64
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

	db            ethdb.Database // Low level persistent database to store final content in
	triegc        *prque.Prque   // Priority queue mapping block numbers to tries to gc
	txLookupLimit uint64

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

	currentBlock     atomic.Value // Current head of the block chain
	currentFastBlock atomic.Value // Current head of the fast-sync chain (may be above the block chain!)

	receiptsCache *lru.Cache // Cache for the most recent receipts per block
	futureBlocks  *lru.Cache // future blocks are blocks added for later processing

	quit          chan struct{}  // blockchain quit channel
	wg            sync.WaitGroup // chain processing wait group for shutting down
	running       int32          // 0 if chain is running, 1 when stopped (must be called atomically)
	procInterrupt int32          // interrupt signaler for block processing
	quitMu        sync.RWMutex

	engine     consensus.Engine
	validator  Validator  // Block and state validator interface
	prefetcher Prefetcher // Block state prefetcher interface
	processor  Processor  // Block transaction processor interface
	vmConfig   vm.Config

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
	if cacheConfig.ArchiveSyncInterval == 0 {
		cacheConfig.ArchiveSyncInterval = 1024
	}

	receiptsCache, _ := lru.New(receiptsCacheLimit)
	futureBlocks, _ := lru.New(maxFutureBlocks)

	bc := &BlockChain{
		chainConfig:         chainConfig,
		cacheConfig:         cacheConfig,
		db:                  db,
		triegc:              prque.New(nil),
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
	bc.validator = NewBlockValidator(chainConfig, bc, engine)
	bc.prefetcher = newStatePrefetcher(chainConfig, bc, engine)
	bc.processor = NewStateProcessor(chainConfig, bc, engine)

	var err error
	bc.hc, err = NewHeaderChain(db, chainConfig, engine, bc.insertStopped)
	if err != nil {
		return nil, err
	}
	bc.genesisBlock = bc.GetBlockByNumber(0)
	if bc.genesisBlock == nil {
		return nil, ErrNoGenesis
	}

	var nilBlock *types.Block
	bc.currentBlock.Store(nilBlock)
	bc.currentFastBlock.Store(nilBlock)

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
	currentBlock := bc.GetBlockByHash(head)
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
		if header := bc.GetHeaderByHash(head); header != nil {
			currentHeader = header
		}
	}
	bc.hc.SetCurrentHeader(bc.db, currentHeader)

	// Restore the last known head fast block
	bc.currentFastBlock.Store(currentBlock)
	//headFastBlockGauge.Update(int64(currentBlock.NumberU64()))

	if head := rawdb.ReadHeadFastBlockHash(bc.db); head != (common.Hash{}) {
		if block := bc.GetBlockByHash(head); block != nil {
			bc.currentFastBlock.Store(block)
			//headFastBlockGauge.Update(int64(block.NumberU64()))
		}
	}
	// Issue a status log for the user

	headerTd := bc.GetTd(currentHeader.Hash(), currentHeader.Number.Uint64())
	blockTd := bc.GetTd(currentBlock.Hash(), currentBlock.NumberU64())

	log.Info("Most recent local header", "number", currentHeader.Number, "hash", currentHeader.Hash(), "td", headerTd, "age", common.PrettyAge(time.Unix(int64(currentHeader.Time), 0)))
	log.Info("Most recent local block", "number", currentBlock.Number(), "hash", currentBlock.Hash(), "td", blockTd, "age", common.PrettyAge(time.Unix(int64(currentBlock.Time()), 0)))

	return nil
}

// SetHead rewinds the local chain to a new head. Depending on whether the node
// was fast synced or full synced and in which state, the method will try to
// delete minimal data from disk whilst retaining chain consistency.
func (bc *BlockChain) SetHead(head uint64) error {
	log.Warn("Rewinding blockchain", "target", head)

	bc.Chainmu.Lock()
	defer bc.Chainmu.Unlock()

	updateFn := func(db ethdb.Database, header *types.Header) (uint64, bool) {
		// Rewind the block chain, ensuring we don't end up with a stateless head block
		if currentBlock := bc.CurrentBlock(); currentBlock != nil && header.Number.Uint64() < currentBlock.NumberU64() {
			if newHeadBlock := bc.GetBlock(header.Hash(), header.Number.Uint64()); newHeadBlock == nil {
				log.Error("Gap in the chain, rewinding to genesis", "number", header.Number, "hash", header.Hash())
			} else {
				rawdb.WriteHeadBlockHash(db, newHeadBlock.Hash())

				// Degrade the chain markers if they are explicitly reverted.
				// In theory we should update all in-memory markers in the
				// last step, however the direction of SetHead is from high
				// to low, so it's safe the update in-memory markers directly.
				bc.currentBlock.Store(newHeadBlock)
				//headBlockGauge.Update(int64(newHeadBlock.NumberU64()))
			}
		}

		// Rewind the fast block in a simpleton way to the target head
		if currentFastBlock := bc.CurrentFastBlock(); currentFastBlock != nil && header.Number.Uint64() < currentFastBlock.NumberU64() {
			newHeadFastBlock := bc.GetBlock(header.Hash(), header.Number.Uint64())
			// If either blocks reached nil, reset to the genesis state
			if newHeadFastBlock == nil {
				newHeadFastBlock = bc.genesisBlock
			}
			rawdb.WriteHeadFastBlockHash(db, newHeadFastBlock.Hash())

			// Degrade the chain markers if they are explicitly reverted.
			// In theory we should update all in-memory markers in the
			// last step, however the direction of SetHead is from high
			// to low, so it's safe the update in-memory markers directly.
			bc.currentFastBlock.Store(newHeadFastBlock)
			//headFastBlockGauge.Update(int64(newHeadFastBlock.NumberU64()))
		}

		return bc.CurrentBlock().NumberU64(), false /* we have nothing to wipe in turbo-geth */
	}

	// Rewind the header chain, deleting all block bodies until then
	delFn := func(db ethdb.Database, hash common.Hash, num uint64) {
		// Remove relative body and receipts from the active store.
		// The header, total difficulty and canonical hash will be
		// removed in the hc.SetHead function.
		rawdb.DeleteBody(db, hash, num)
		if err := rawdb.DeleteReceipts(db, num); err != nil {
			panic(err)
		}
	}

	// If SetHead was only called as a chain reparation method, try to skip
	// touching the header chain altogether, unless the freezer is broken
	if block := bc.CurrentBlock(); block.NumberU64() == head {
		if target, force := updateFn(bc.db, block.Header()); force {
			bc.hc.SetHead(target, updateFn, delFn)
		}
	} else {
		// Rewind the chain to the requested head and keep going backwards until a
		// block with a state is found or fast sync pivot is passed
		log.Warn("Rewinding blockchain", "target", head)
		bc.hc.SetHead(head, updateFn, delFn)
	}

	// Clear out any stale content from the caches
	bc.receiptsCache.Purge()
	bc.futureBlocks.Purge()

	return bc.loadLastState()
}

// FastSyncCommitHead sets the current head block to the one defined by the hash
// irrelevant what the chain contents were prior.
func (bc *BlockChain) FastSyncCommitHead(hash common.Hash) error {
	// Make sure that both the block as well at its state trie exists
	block := bc.GetBlockByHash(hash)
	if block == nil {
		return fmt.Errorf("non existent block [%x…]", hash[:4])
	}
	// If all checks out, manually set the head block
	bc.Chainmu.Lock()
	bc.currentBlock.Store(block)
	//headBlockGauge.Update(int64(block.NumberU64()))
	bc.Chainmu.Unlock()

	log.Info("Committed new head block", "number", block.Number(), "hash", hash)
	return nil
}

// GasLimit returns the gas limit of the current HEAD block.
func (bc *BlockChain) GasLimit() uint64 {
	return bc.CurrentBlock().GasLimit()
}

// CurrentBlock retrieves the current head block of the canonical chain. The
// block is retrieved from the blockchain's internal cache.
func (bc *BlockChain) CurrentBlock() *types.Block {
	headHash := rawdb.ReadHeadBlockHash(bc.db)
	headNumber := rawdb.ReadHeaderNumber(bc.db, headHash)
	return rawdb.ReadBlock(bc.db, headHash, *headNumber)
}

// CurrentFastBlock retrieves the current fast-sync head block of the canonical
// chain. The block is retrieved from the blockchain's internal cache.
func (bc *BlockChain) CurrentFastBlock() *types.Block {
	return bc.currentFastBlock.Load().(*types.Block)
}

// Validator returns the current validator.
func (bc *BlockChain) Validator() Validator {
	return bc.validator
}

// Processor returns the current processor.
func (bc *BlockChain) Processor() Processor {
	return bc.processor
}

// Reset purges the entire blockchain, restoring it to its genesis state.
func (bc *BlockChain) Reset() error {
	return bc.ResetWithGenesisBlock(bc.genesisBlock)
}

// ResetWithGenesisBlock purges the entire blockchain, restoring it to the
// specified genesis state.
func (bc *BlockChain) ResetWithGenesisBlock(genesis *types.Block) error {
	// Dump the entire block chain and purge the caches
	if err := bc.SetHead(0); err != nil {
		return err
	}
	bc.Chainmu.Lock()
	defer bc.Chainmu.Unlock()

	if err := rawdb.WriteTd(bc.db, genesis.Hash(), genesis.NumberU64(), genesis.Difficulty()); err != nil {
		return err
	}
	if err := rawdb.WriteBlock(context.Background(), bc.db, genesis); err != nil {
		return err
	}
	if err := bc.writeHeadBlock(genesis); err != nil {
		return err
	}

	// Last update all in-memory chain markers
	bc.genesisBlock = genesis
	bc.currentBlock.Store(bc.genesisBlock)
	//headBlockGauge.Update(int64(bc.genesisBlock.NumberU64()))
	bc.hc.SetGenesis(bc.genesisBlock.Header())
	bc.hc.SetCurrentHeader(bc.db, bc.genesisBlock.Header())
	bc.currentFastBlock.Store(bc.genesisBlock)
	//headFastBlockGauge.Update(int64(bc.genesisBlock.NumberU64()))
	return nil
}

// Export writes the active chain to the given writer.
func (bc *BlockChain) Export(w io.Writer) error {
	return bc.ExportN(w, uint64(0), bc.CurrentBlock().NumberU64())
}

// ExportN writes a subset of the active chain to the given writer.
func (bc *BlockChain) ExportN(w io.Writer, first uint64, last uint64) error {
	bc.Chainmu.RLock()
	defer bc.Chainmu.RUnlock()

	if first > last {
		return fmt.Errorf("export failed: first (%d) is greater than last (%d)", first, last)
	}
	log.Info("Exporting batch of blocks", "count", last-first+1)

	start, reported := time.Now(), time.Now()
	for nr := first; nr <= last; nr++ {
		block := bc.GetBlockByNumber(nr)
		if block == nil {
			return fmt.Errorf("export failed on #%d: not found", nr)
		}
		if err := block.EncodeRLP(w); err != nil {
			return err
		}
		if time.Since(reported) >= statsReportLimit {
			log.Info("Exporting blocks", "exported", block.NumberU64()-first, "elapsed", common.PrettyDuration(time.Since(start)))
			reported = time.Now()
		}
	}
	return nil
}

// writeHeadBlock injects a new head block into the current block chain. This method
// assumes that the block is indeed a true head. It will also reset the head
// header and the head fast sync block to this very same block if they are older
// or if they are on a different side chain.
//
// Note, this function assumes that the `mu` mutex is held!
func (bc *BlockChain) writeHeadBlock(block *types.Block) error {
	// If the block is on a side chain or an unknown one, force other heads onto it too
	h, err := rawdb.ReadCanonicalHash(bc.db, block.NumberU64())
	if err != nil {
		return err
	}

	updateHeads := h != block.Hash()

	// Add the block to the canonical chain number scheme and mark as the head
	if err := rawdb.WriteCanonicalHash(bc.db, block.Hash(), block.NumberU64()); err != nil {
		log.Error("failed WriteCanonicalHash", "err", err)
	}
	if bc.enableTxLookupIndex && !bc.cacheConfig.DownloadOnly {
		rawdb.WriteTxLookupEntries(bc.db, block)
	}
	rawdb.WriteHeadBlockHash(bc.db, block.Hash())

	if updateHeads {
		bc.hc.SetCurrentHeader(bc.db, block.Header())
		rawdb.WriteHeadFastBlockHash(bc.db, block.Hash())
	}

	// If the block is better than our head or is on a different chain, force update heads
	if updateHeads {
		bc.currentFastBlock.Store(block)
		//headFastBlockGauge.Update(int64(block.NumberU64()))
	}
	bc.currentBlock.Store(block)
	//headBlockGauge.Update(int64(block.NumberU64()))
	return nil
}

// Genesis retrieves the chain's genesis block.
func (bc *BlockChain) Genesis() *types.Block {
	return bc.genesisBlock
}

// GetBody retrieves a block body (transactions and uncles) from the database by
// hash, caching it if found.
func (bc *BlockChain) GetBody(hash common.Hash) *types.Body {
	number := bc.hc.GetBlockNumber(bc.db, hash)
	if number == nil {
		return nil
	}
	body := rawdb.ReadBody(bc.db, hash, *number)
	if body == nil {
		return nil
	}
	return body
}

// GetBodyRLP retrieves a block body in RLP encoding from the database by hash,
// caching it if found.
func (bc *BlockChain) GetBodyRLP(hash common.Hash) rlp.RawValue {
	number := bc.hc.GetBlockNumber(bc.db, hash)
	if number == nil {
		return nil
	}
	body := rawdb.ReadBodyRLP(bc.db, hash, *number)
	if len(body) == 0 {
		return nil
	}
	return body
}

// HasBlock checks if a block is fully present in the database or not.
func (bc *BlockChain) HasBlock(hash common.Hash, number uint64) bool {
	return rawdb.HasBody(bc.db, hash, number)
}

// HasBlockAndState checks if a block and associated state trie is fully present
// in the database or not, caching it if present.
func (bc *BlockChain) HasBlockAndState(hash common.Hash, number uint64) bool {
	// Check first that the block itself is known
	return bc.GetBlock(hash, number) != nil
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

// GetBlocksFromHash returns the block corresponding to hash and up to n-1 ancestors.
// [deprecated by eth/62]
func (bc *BlockChain) GetBlocksFromHash(hash common.Hash, n int) (blocks []*types.Block) {
	number := bc.hc.GetBlockNumber(bc.db, hash)
	if number == nil {
		return nil
	}
	for i := 0; i < n; i++ {
		block := bc.GetBlock(hash, *number)
		if block == nil {
			break
		}
		blocks = append(blocks, block)
		hash = block.ParentHash()
		*number--
	}
	return
}

// GetUnclesInChain retrieves all the uncles from a given block backwards until
// a specific distance is reached.
func (bc *BlockChain) GetUnclesInChain(block *types.Block, length int) []*types.Header {
	uncles := []*types.Header{}
	for i := 0; block != nil && i < length; i++ {
		uncles = append(uncles, block.Uncles()...)
		block = bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
	}
	return uncles
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
	bc.StopInsert()
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

// StopInsert interrupts all insertion methods, causing them to return
// errInsertionInterrupted as soon as possible. Insertion is permanently disabled after
// calling this method.
func (bc *BlockChain) StopInsert() {
	atomic.StoreInt32(&bc.procInterrupt, 1)
}

// insertStopped returns true after StopInsert has been called.
func (bc *BlockChain) insertStopped() bool {
	return atomic.LoadInt32(&bc.procInterrupt) == 1
}

// WriteStatus status of write
type WriteStatus byte

const (
	NonStatTy WriteStatus = iota
	CanonStatTy
	SideStatTy
)

// numberHash is just a container for a number and a hash, to represent a block
type numberHash struct {
	number uint64
	hash   common.Hash
}

// InsertReceiptChain attempts to complete an already existing header chain with
// transaction and receipt data.
func (bc *BlockChain) InsertReceiptChain(blockChain types.Blocks, receiptChain []types.Receipts, ancientLimit uint64) (int, error) {
	// We don't require the chainMu here since we want to maximize the
	// concurrency of header insertion and receipt insertion.
	if err := bc.addJob(); err != nil {
		return 0, err
	}
	defer bc.doneJob()
	commitEvery := time.NewTicker(30 * time.Second)
	defer commitEvery.Stop()
	liveBlocks := types.Blocks{}
	liveReceipts := []types.Receipts{}

	// Do a sanity check that the provided chain is actually ordered and linked
	for i := 0; i < len(blockChain); i++ {
		if i != 0 {
			if blockChain[i].NumberU64() != blockChain[i-1].NumberU64()+1 || blockChain[i].ParentHash() != blockChain[i-1].Hash() {
				log.Error("Non contiguous receipt insert", "number", blockChain[i].Number(), "hash", blockChain[i].Hash(), "parent", blockChain[i].ParentHash(),
					"prevnumber", blockChain[i-1].Number(), "prevhash", blockChain[i-1].Hash())
				return 0, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, blockChain[i-1].NumberU64(),
					blockChain[i-1].Hash().Bytes()[:4], i, blockChain[i].NumberU64(), blockChain[i].Hash().Bytes()[:4], blockChain[i].ParentHash().Bytes()[:4])
			}
		}
		if blockChain[i].NumberU64() > ancientLimit {
			liveBlocks, liveReceipts = append(liveBlocks, blockChain[i]), append(liveReceipts, receiptChain[i])
		}
	}

	var (
		stats = struct{ processed, ignored int32 }{}
		start = time.Now()
		size  = 0
	)
	// updateHead updates the head fast sync block if the inserted blocks are better
	// and returns an indicator whether the inserted blocks are canonical.
	updateHead := func(head *types.Block) bool {
		bc.Chainmu.Lock()

		// Rewind may have occurred, skip in that case.
		if bc.CurrentHeader().Number.Cmp(head.Number()) >= 0 {
			currentFastBlock, td := bc.CurrentFastBlock(), bc.GetTd(head.Hash(), head.NumberU64())
			if bc.GetTd(currentFastBlock.Hash(), currentFastBlock.NumberU64()).Cmp(td) < 0 {
				rawdb.WriteHeadFastBlockHash(bc.db, head.Hash())
				bc.currentFastBlock.Store(head)
				//headFastBlockGauge.Update(int64(head.NumberU64()))
				bc.Chainmu.Unlock()
				return true
			}
		}
		bc.Chainmu.Unlock()
		return false
	}
	// writeLive writes blockchain and corresponding receipt chain into active store.
	writeLive := func(blockChain types.Blocks, receiptChain []types.Receipts) (int, error) {
		skipPresenceCheck := false
		batch := bc.db.NewBatch()
		defer batch.Rollback()
		for i, block := range blockChain {
			// Short circuit insertion if shutting down or processing failed
			if bc.insertStopped() {
				return 0, errInsertionInterrupted
			}
			// Short circuit if the owner header is unknown
			if !bc.HasHeader(block.Hash(), block.NumberU64()) {
				return i, fmt.Errorf("containing header #%d [%x…] unknown", block.Number(), block.Hash().Bytes()[:4])
			}
			if !skipPresenceCheck {
				// Ignore if the entire data is already known
				if bc.HasBlock(block.Hash(), block.NumberU64()) {
					stats.ignored++
					continue
				} else {
					// If block N is not present, neither are the later blocks.
					// This should be true, but if we are mistaken, the shortcut
					// here will only cause overwriting of some existing data
					skipPresenceCheck = true
				}
			}
			// Write all the data out into the database
			if err := rawdb.WriteBody(batch, block.Hash(), block.NumberU64(), block.Body()); err != nil {
				return 0, err
			}

			if err := rawdb.WriteReceipts(batch, block.NumberU64(), receiptChain[i]); err != nil {
				return 0, err
			}
			if bc.enableTxLookupIndex {
				rawdb.WriteTxLookupEntries(batch, block)
			}

			stats.processed++
			select {
			case <-commitEvery.C:
				size += batch.BatchSize()
				if err := batch.CommitAndBegin(context.Background()); err != nil {
					return 0, err
				}
			default:
			}
		}
		if batch.BatchSize() > 0 {
			size += batch.BatchSize()
			if err := batch.Commit(); err != nil {
				return 0, err
			}
		}
		updateHead(blockChain[len(blockChain)-1])
		return 0, nil
	}

	if len(liveBlocks) > 0 {
		if n, err := writeLive(liveBlocks, liveReceipts); err != nil {
			if err == errInsertionInterrupted {
				return 0, nil
			}
			return n, err
		}
	}

	head := blockChain[len(blockChain)-1]
	context := []interface{}{
		"count", stats.processed, "elapsed", common.PrettyDuration(time.Since(start)),
		"number", head.Number(), "hash", head.Hash(), "age", common.PrettyAge(time.Unix(int64(head.Time()), 0)),
		"size", common.StorageSize(size),
	}
	if stats.ignored > 0 {
		context = append(context, []interface{}{"ignored", stats.ignored}...)
	}
	log.Info("Imported new block receipts", context...)

	return 0, nil
}

// SetTxLookupLimit is responsible for updating the txlookup limit to the
// original one stored in db if the new mismatches with the old one.
func (bc *BlockChain) SetTxLookupLimit(limit uint64) {
	bc.txLookupLimit = limit
}

// TxLookupLimit retrieves the txlookup limit used by blockchain to prune
// stale transaction indices.
func (bc *BlockChain) TxLookupLimit() uint64 {
	return bc.txLookupLimit
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

func (bc *BlockChain) HeaderChain() *HeaderChain {
	return bc.hc
}

// InsertHeaderChain attempts to insert the given header chain in to the local
// chain, possibly creating a reorg. If an error is returned, it will return the
// index number of the failing header as well an error describing what went wrong.
//
// The verify parameter can be used to fine tune whether nonce verification
// should be done or not. The reason behind the optional check is because some
// of the header retrieval mechanisms already need to verify nonces, as well as
// because nonces can be verified sparsely, not needing to check each.
func (bc *BlockChain) InsertHeaderChain(chain []*types.Header, checkFreq int) (int, error) {
	if i, err := bc.hc.ValidateHeaderChain(chain, checkFreq); err != nil {
		return i, err
	}

	// Make sure only one thread manipulates the chain at once
	bc.Chainmu.Lock()
	defer bc.Chainmu.Unlock()

	if err := bc.addJob(); err != nil {
		return 0, err
	}
	defer bc.doneJob()

	return 0, nil
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

// addJob should be called only for public methods
func (bc *BlockChain) addJob() error {
	bc.quitMu.RLock()
	defer bc.quitMu.RUnlock()
	if bc.insertStopped() {
		return errors.New("blockchain is stopped")
	}
	bc.wg.Add(1)

	return nil
}

func (bc *BlockChain) doneJob() {
	bc.wg.Done()
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
	gp := new(GasPool).AddGas(block.GasLimit())

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
			return nil, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		//fmt.Printf("Tx Hash: %x, gas used: %d\n", tx.Hash(), receipt.GasUsed)
		if !vmConfig.NoReceipts {
			receipts = append(receipts, receipt)
		}
	}

	if chainConfig.IsByzantium(header.Number) && !vmConfig.NoReceipts {
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

	ctx := cc.WithEIPsFlags(context.Background(), header.Number)
	if err := ibs.CommitBlock(ctx, stateWriter); err != nil {
		return fmt.Errorf("committing block %d failed: %v", header.Number.Uint64(), err)
	}
	if err := stateWriter.WriteChangeSets(); err != nil {
		return fmt.Errorf("writing changesets for block %d failed: %v", header.Number.Uint64(), err)
	}
	return nil
}
