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
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
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
	headBlockGauge     = metrics.NewRegisteredGauge("chain/head/block", nil)
	headHeaderGauge    = metrics.NewRegisteredGauge("chain/head/header", nil)
	headFastBlockGauge = metrics.NewRegisteredGauge("chain/head/receipt", nil)

	accountReadTimer   = metrics.NewRegisteredTimer("chain/account/reads", nil)
	accountHashTimer   = metrics.NewRegisteredTimer("chain/account/hashes", nil)
	accountUpdateTimer = metrics.NewRegisteredTimer("chain/account/updates", nil)
	accountCommitTimer = metrics.NewRegisteredTimer("chain/account/commits", nil)

	storageReadTimer   = metrics.NewRegisteredTimer("chain/storage/reads", nil)
	storageHashTimer   = metrics.NewRegisteredTimer("chain/storage/hashes", nil)
	storageUpdateTimer = metrics.NewRegisteredTimer("chain/storage/updates", nil)
	storageCommitTimer = metrics.NewRegisteredTimer("chain/storage/commits", nil)

	blockInsertTimer     = metrics.NewRegisteredTimer("chain/inserts", nil)
	blockValidationTimer = metrics.NewRegisteredTimer("chain/validation", nil)
	blockExecutionTimer  = metrics.NewRegisteredTimer("chain/execution", nil)
	blockWriteTimer      = metrics.NewRegisteredTimer("chain/write", nil)

	blockReorgMeter         = metrics.NewRegisteredMeter("chain/reorg/executes", nil)
	blockReorgAddMeter      = metrics.NewRegisteredMeter("chain/reorg/add", nil)
	blockReorgDropMeter     = metrics.NewRegisteredMeter("chain/reorg/drop", nil)
	blockReorgInvalidatedTx = metrics.NewRegisteredMeter("chain/reorg/invalidTx", nil)

	blockPrefetchExecuteTimer   = metrics.NewRegisteredTimer("chain/prefetch/executes", nil)
	blockPrefetchInterruptMeter = metrics.NewRegisteredMeter("chain/prefetch/interrupts", nil)

	errInsertionInterrupted = errors.New("insertion is interrupted")

	// ErrNotFound is returned when sought data isn't found.
	ErrNotFound = errors.New("data not found")
)

const (
	receiptsCacheLimit  = 32
	maxFutureBlocks     = 256
	maxTimeFutureBlocks = 30
	badBlockLimit       = 10
	triesInMemory       = 128

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
	Pruning             bool
	TrieCleanLimit      int           // Memory allowance (MB) to use for caching trie nodes in memory
	TrieCleanJournal    string        // Disk journal for saving clean cache entries.
	TrieCleanRejournal  time.Duration // Time interval to dump clean cache to disk periodically
	TrieCleanNoPrefetch bool          // Whether to disable heuristic state prefetching for followup blocks
	TrieDirtyLimit      int           // Memory limit (MB) at which to start flushing dirty trie nodes to disk
	TrieTimeLimit       time.Duration // Time limit after which to flush the current in-memory trie to disk

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
	TrieCleanLimit:      256,
	TrieDirtyLimit:      256,
	TrieTimeLimit:       5 * time.Minute,
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
	gcproc        time.Duration  // Accumulates canonical block processing for trie dumping
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

	chainmu sync.RWMutex // blockchain insertion lock

	currentBlock     atomic.Value // Current head of the block chain
	committedBlock   atomic.Value // Committed head of the block chain
	currentFastBlock atomic.Value // Current head of the fast-sync chain (may be above the block chain!)

	trieDbState   *state.TrieDbState
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

	badBlocks           *lru.Cache                     // Bad block cache
	shouldPreserve      func(*types.Block) bool        // Function used to determine whether should preserve the given block.
	terminateInsert     func(common.Hash, uint64) bool // Testing hook used to terminate ancient receipt chain insertion.
	highestKnownBlock   uint64
	highestKnownBlockMu sync.Mutex
	enableReceipts      bool // Whether receipts need to be written to the database
	enableTxLookupIndex bool // Whether we store tx lookup index into the database
	enablePreimages     bool // Whether we store preimages into the database
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
	badBlocks, _ := lru.New(badBlockLimit)

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
		badBlocks:           badBlocks,
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

func (bc *BlockChain) GetTrieDbState() (*state.TrieDbState, error) {
	if bc.trieDbState == nil && !bc.cacheConfig.DownloadOnly {
		currentBlockNr := bc.CurrentBlock().NumberU64()
		trieDbState, err := bc.GetTrieDbStateByBlock(bc.CurrentBlock().Header().Root, currentBlockNr)
		if err != nil {
			return nil, err
		}
		bc.setTrieDbState(trieDbState)
		log.Info("Creation complete.")
	}
	return bc.trieDbState, nil
}

func (bc *BlockChain) setTrieDbState(trieDbState *state.TrieDbState) {
	log.Warn("trieDbState has been changed", "isNil", trieDbState == nil, "callers", debug.Callers(20))
	bc.trieDbState = trieDbState
}

func (bc *BlockChain) GetTrieDbStateByBlock(root common.Hash, blockNr uint64) (*state.TrieDbState, error) {
	if bc.trieDbState == nil || bc.trieDbState.LastRoot() != root || bc.trieDbState.GetBlockNr() != blockNr {
		log.Info("Creating IntraBlockState from latest state", "block", blockNr, "isNIl", bc.trieDbState == nil, "callers", debug.Callers(20))
		tds := state.NewTrieDbState(root, bc.db, blockNr)
		tds.SetNoHistory(bc.NoHistory())
		tds.SetResolveReads(bc.resolveReads)
		tds.EnablePreimages(bc.enablePreimages)

		log.Info("Creation complete.")
		return tds, nil
	}
	return bc.trieDbState, nil
}

// GetVMConfig returns the block chain VM config.
func (bc *BlockChain) GetVMConfig() *vm.Config {
	return &bc.vmConfig
}

// empty returns an indicator whether the blockchain is empty.
// Note, it's a special case that we connect a non-empty ancient
// database with an empty node, so that we can plugin the ancient
// into node seamlessly.
func (bc *BlockChain) empty() bool {
	genesis := bc.genesisBlock.Hash()
	for _, hash := range []common.Hash{rawdb.ReadHeadBlockHash(bc.db), rawdb.ReadHeadHeaderHash(bc.db), rawdb.ReadHeadFastBlockHash(bc.db)} {
		if hash != genesis {
			return false
		}
	}
	return true
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
	headBlockGauge.Update(int64(currentBlock.NumberU64()))

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
	headFastBlockGauge.Update(int64(currentBlock.NumberU64()))

	if head := rawdb.ReadHeadFastBlockHash(bc.db); head != (common.Hash{}) {
		if block := bc.GetBlockByHash(head); block != nil {
			bc.currentFastBlock.Store(block)
			headFastBlockGauge.Update(int64(block.NumberU64()))
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

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	updateFn := func(db ethdb.Database, header *types.Header) (uint64, bool) {
		// Rewind the block chain, ensuring we don't end up with a stateless head block
		if currentBlock := bc.CurrentBlock(); currentBlock != nil && header.Number.Uint64() < currentBlock.NumberU64() {
			newHeadBlock := bc.GetBlock(header.Hash(), header.Number.Uint64())
			if newHeadBlock == nil {
				log.Error("Gap in the chain, rewinding to genesis", "number", header.Number, "hash", header.Hash())
				newHeadBlock = bc.genesisBlock
			} else {
				rawdb.WriteHeadBlockHash(db, newHeadBlock.Hash())

				// Degrade the chain markers if they are explicitly reverted.
				// In theory we should update all in-memory markers in the
				// last step, however the direction of SetHead is from high
				// to low, so it's safe the update in-memory markers directly.
				bc.currentBlock.Store(newHeadBlock)
				headBlockGauge.Update(int64(newHeadBlock.NumberU64()))
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
			headFastBlockGauge.Update(int64(newHeadFastBlock.NumberU64()))
		}

		return bc.CurrentBlock().NumberU64(), false /* we have nothing to wipe in turbo-geth */
	}

	// Rewind the header chain, deleting all block bodies until then
	delFn := func(db ethdb.Database, hash common.Hash, num uint64) {
		// Ignore the error here since light client won't hit this path
		frozen, _ := bc.db.Ancients()
		if num+1 <= frozen {
			// Truncate all relative data(header, total difficulty, body, receipt
			// and canonical hash) from ancient store.
			if err := bc.db.TruncateAncients(num); err != nil {
				log.Crit("Failed to truncate ancient data", "number", num, "err", err)
			}

			// Remove the hash <-> number mapping from the active store.
			rawdb.DeleteHeaderNumber(db, hash)
		} else {
			// Remove relative body and receipts from the active store.
			// The header, total difficulty and canonical hash will be
			// removed in the hc.SetHead function.
			rawdb.DeleteBody(db, hash, num)
			if err := rawdb.DeleteReceipts(db, num); err != nil {
				panic(err)
			}
		}
		// Todo(rjl493456442) txlookup, bloombits, etc
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
	bc.chainmu.Lock()
	bc.currentBlock.Store(block)
	headBlockGauge.Update(int64(block.NumberU64()))
	bc.chainmu.Unlock()

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
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

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
	headBlockGauge.Update(int64(bc.genesisBlock.NumberU64()))
	bc.hc.SetGenesis(bc.genesisBlock.Header())
	bc.hc.SetCurrentHeader(bc.db, bc.genesisBlock.Header())
	bc.currentFastBlock.Store(bc.genesisBlock)
	headFastBlockGauge.Update(int64(bc.genesisBlock.NumberU64()))
	return nil
}

// Export writes the active chain to the given writer.
func (bc *BlockChain) Export(w io.Writer) error {
	return bc.ExportN(w, uint64(0), bc.CurrentBlock().NumberU64())
}

// ExportN writes a subset of the active chain to the given writer.
func (bc *BlockChain) ExportN(w io.Writer, first uint64, last uint64) error {
	bc.chainmu.RLock()
	defer bc.chainmu.RUnlock()

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
	if err := rawdb.WriteCanonicalHeader(bc.db, block.Header()); err != nil {
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
		headFastBlockGauge.Update(int64(block.NumberU64()))
	}
	bc.currentBlock.Store(block)
	headBlockGauge.Update(int64(block.NumberU64()))
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

// HasFastBlock checks if a fast block is fully present in the database or not.
func (bc *BlockChain) HasFastBlock(hash common.Hash, number uint64) bool {
	if !bc.HasBlock(hash, number) {
		return false
	}
	if bc.receiptsCache.Contains(hash) {
		return true
	}
	return rawdb.HasReceipts(bc.db, hash, number)
}

// HasBlockAndState checks if a block and associated state trie is fully present
// in the database or not, caching it if present.
func (bc *BlockChain) HasBlockAndState(hash common.Hash, number uint64) bool {
	// Check first that the block itself is known
	block := bc.GetBlock(hash, number)
	if block == nil {
		return false
	}
	return true
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

// truncateAncient rewinds the blockchain to the specified header and deletes all
// data in the ancient store that exceeds the specified header.
func (bc *BlockChain) truncateAncient(head uint64) error {
	frozen, err := bc.db.Ancients()
	if err != nil {
		return err
	}
	// Short circuit if there is no data to truncate in ancient store.
	if frozen <= head+1 {
		return nil
	}
	// Truncate all the data in the freezer beyond the specified head
	if err := bc.db.TruncateAncients(head + 1); err != nil {
		return err
	}

	// Clear out any stale content from the caches
	bc.receiptsCache.Purge()
	bc.futureBlocks.Purge()

	log.Info("Rewind ancient data", "number", head)
	return nil
}

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

	var (
		ancientBlocks, liveBlocks     types.Blocks
		ancientReceipts, liveReceipts []types.Receipts
	)
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
		if blockChain[i].NumberU64() <= ancientLimit {
			ancientBlocks, ancientReceipts = append(ancientBlocks, blockChain[i]), append(ancientReceipts, receiptChain[i])
		} else {
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
		bc.chainmu.Lock()

		// Rewind may have occurred, skip in that case.
		if bc.CurrentHeader().Number.Cmp(head.Number()) >= 0 {
			currentFastBlock, td := bc.CurrentFastBlock(), bc.GetTd(head.Hash(), head.NumberU64())
			if bc.GetTd(currentFastBlock.Hash(), currentFastBlock.NumberU64()).Cmp(td) < 0 {
				rawdb.WriteHeadFastBlockHash(bc.db, head.Hash())
				bc.currentFastBlock.Store(head)
				headFastBlockGauge.Update(int64(head.NumberU64()))
				bc.chainmu.Unlock()
				return true
			}
		}
		bc.chainmu.Unlock()
		return false
	}
	// writeAncient writes blockchain and corresponding receipt chain into ancient store.
	//
	// this function only accepts canonical chain data. All side chain will be reverted
	// eventually.
	/*
				writeAncient := func(blockChain types.Blocks, receiptChain []types.Receipts) (int, error) {
					var (
						previous = bc.CurrentFastBlock()
					)
					// If any error occurs before updating the head or we are inserting a side chain,
					// all the data written this time wll be rolled back.
					defer func() {
						if previous != nil {
							if err := bc.truncateAncient(previous.NumberU64()); err != nil {
								log.Crit("Truncate ancient store failed", "err", err)
							}
						}
					}()
					var deleted []*numberHash
					for i, block := range blockChain {
						// Short circuit insertion if shutting down or processing failed
						if bc.getProcInterrupt() {
							return 0, errInsertionInterrupted
						}
						// Short circuit insertion if it is required(used in testing only)
						if bc.terminateInsert != nil && bc.terminateInsert(block.Hash(), block.NumberU64()) {
							return i, errors.New("insertion is terminated for testing purpose")
						}
						// Short circuit if the owner header is unknown
						if !bc.HasHeader(block.Hash(), block.NumberU64()) {
							return i, fmt.Errorf("containing header #%d [%x…] unknown", block.Number(), block.Hash().Bytes()[:4])
						}

						// Turbo-Geth doesn't have fast sync support
						// Flush data into ancient database.
						size += rawdb.WriteAncientBlock(bc.db, block, receiptChain[i], bc.GetTd(block.Hash(), block.NumberU64()))
						if bc.enableTxLookupIndex {
							rawdb.WriteTxLookupEntries(bc.db, block)
						}

						// Write tx indices if any condition is satisfied:
		 				// * If user requires to reserve all tx indices(txlookuplimit=0)
		 				// * If all ancient tx indices are required to be reserved(txlookuplimit is even higher than ancientlimit)
		 				// * If block number is large enough to be regarded as a recent block
		 				// It means blocks below the ancientLimit-txlookupLimit won't be indexed.
		 				//
		 				// But if the `TxIndexTail` is not nil, e.g. Geth is initialized with
		 				// an external ancient database, during the setup, blockchain will start
		 				// a background routine to re-indexed all indices in [ancients - txlookupLimit, ancients)
		 				// range. In this case, all tx indices of newly imported blocks should be
		 				// generated.
		 				if bc.txLookupLimit == 0 || ancientLimit <= bc.txLookupLimit || block.NumberU64() >= ancientLimit-bc.txLookupLimit {
		 					rawdb.WriteTxLookupEntries(batch, block)
		 				} else if rawdb.ReadTxIndexTail(bc.db) != nil {
		 					rawdb.WriteTxLookupEntries(batch, block)
		 				}
						stats.processed++
					}

					if !updateHead(blockChain[len(blockChain)-1]) {
						return 0, errors.New("side blocks can't be accepted as the ancient chain data")
					}
					previous = nil // disable rollback explicitly

					// Wipe out canonical block data.
					for _, nh := range deleted {
						rawdb.DeleteBlockWithoutNumber(bc.db, nh.hash, nh.number)
						rawdb.DeleteCanonicalHash(bc.db, nh.number)
					}
					for _, block := range blockChain {
						// Always keep genesis block in active database.
						if block.NumberU64() != 0 {
							rawdb.DeleteBlockWithoutNumber(bc.db, block.Hash(), block.NumberU64())
							rawdb.DeleteCanonicalHash(bc.db, block.NumberU64())
						}
					}

					// Wipe out side chain too.
					for _, nh := range deleted {
						for _, hash := range rawdb.ReadAllHashes(bc.db, nh.number) {
							rawdb.DeleteBlock(bc.db, hash, nh.number)
						}
					}
					for _, block := range blockChain {
						// Always keep genesis block in active database.
						if block.NumberU64() != 0 {
							for _, hash := range rawdb.ReadAllHashes(bc.db, block.NumberU64()) {
								rawdb.DeleteBlock(bc.db, hash, block.NumberU64())
							}
						}
					}
					return 0, nil
				}
	*/
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
			rawdb.WriteBody(context.Background(), batch, block.Hash(), block.NumberU64(), block.Body())
			if err := rawdb.WriteReceipts(batch, block.NumberU64(), receiptChain[i]); err != nil {
				return 0, err
			}
			if bc.enableTxLookupIndex {
				rawdb.WriteTxLookupEntries(batch, block)
			}

			stats.processed++
			if batch.BatchSize() >= batch.IdealBatchSize() {
				size += batch.BatchSize()
				if err := batch.CommitAndBegin(context.Background()); err != nil {
					return 0, err
				}
			}
			stats.processed++
		}
		if batch.BatchSize() > 0 {
			size += batch.BatchSize()
			if _, err := batch.Commit(); err != nil {
				return 0, err
			}
		}
		updateHead(blockChain[len(blockChain)-1])
		return 0, nil
	}

	/*
		// Write downloaded chain data and corresponding receipt chain data
		if len(ancientBlocks) > 0 {
			if n, err := writeAncient(ancientBlocks, ancientReceipts); err != nil {
				if err == errInsertionInterrupted {
					return 0, nil
				}
				return n, err

		}
		// Write the tx index tail (block number from where we index) before write any live blocks
		if len(liveBlocks) > 0 && liveBlocks[0].NumberU64() == ancientLimit+1 {
			// The tx index tail can only be one of the following two options:
			// * 0: all ancient blocks have been indexed
			// * ancient-limit: the indices of blocks before ancient-limit are ignored
			if tail := rawdb.ReadTxIndexTail(bc.db); tail == nil {
				if bc.txLookupLimit == 0 || ancientLimit <= bc.txLookupLimit {
					rawdb.WriteTxIndexTail(bc.db, 0)
				} else {
					rawdb.WriteTxIndexTail(bc.db, ancientLimit-bc.txLookupLimit)
				}
			}
		}
	*/
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

// WriteBlockWithState writes the block and all associated state to the database.
func (bc *BlockChain) WriteBlockWithState(ctx context.Context, block *types.Block, receipts []*types.Receipt, logs []*types.Log, state *state.IntraBlockState, tds *state.TrieDbState, emitHeadEvent bool) (status WriteStatus, err error) {
	if err = bc.addJob(); err != nil {
		return NonStatTy, err
	}
	defer bc.doneJob()

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	return bc.writeBlockWithState(ctx, block, receipts, logs, state, tds, emitHeadEvent)
}

// writeBlockWithState writes the block and all associated state to the database,
// but is expects the chain mutex to be held.
func (bc *BlockChain) writeBlockWithState(ctx context.Context, block *types.Block, receipts []*types.Receipt, logs []*types.Log, stateDb *state.IntraBlockState, tds *state.TrieDbState, emitHeadEvent bool) (status WriteStatus, err error) {
	// Make sure no inconsistent state is leaked during insertion
	currentBlock := bc.CurrentBlock()

	// Calculate the total difficulty of the block
	ptd := bc.GetTd(block.ParentHash(), block.NumberU64()-1)
	if ptd == nil {
		return NonStatTy, consensus.ErrUnknownAncestor
	}
	externTd := new(big.Int).Add(block.Difficulty(), ptd)

	// Irrelevant of the canonical status, write the block itself to the database
	if common.IsCanceled(ctx) {
		return NonStatTy, ctx.Err()
	}
	if err := rawdb.WriteTd(bc.db, block.Hash(), block.NumberU64(), externTd); err != nil {
		return NonStatTy, err
	}
	rawdb.WriteBody(ctx, bc.db, block.Hash(), block.NumberU64(), block.Body())
	sendersData := make([]byte, len(block.Transactions())*common.AddressLength)
	senders := block.Body().SendersFromTxs()
	for i, sender := range senders {
		copy(sendersData[i*common.AddressLength:], sender[:])
	}
	if err := bc.db.Put(dbutils.Senders, dbutils.BlockBodyKey(block.NumberU64(), block.Hash()), sendersData); err != nil {
		return NonStatTy, err
	}
	rawdb.WriteHeader(ctx, bc.db, block.Header())

	if tds != nil {
		tds.SetBlockNr(block.NumberU64())
	}

	ctx = bc.WithContext(ctx, block.Number())
	if stateDb != nil {
		blockWriter := tds.DbStateWriter()
		if err := stateDb.CommitBlock(ctx, blockWriter); err != nil {
			return NonStatTy, err
		}
		plainBlockWriter := state.NewPlainStateWriter(bc.db, bc.db, block.NumberU64())
		if err := stateDb.CommitBlock(ctx, plainBlockWriter); err != nil {
			return NonStatTy, err
		}
		// Always write changesets
		if err := blockWriter.WriteChangeSets(); err != nil {
			return NonStatTy, err
		}
		// Optionally write history
		if !bc.NoHistory() {
			if err := blockWriter.WriteHistory(); err != nil {
				return NonStatTy, err
			}
		}
	}
	if bc.enableReceipts && !bc.cacheConfig.DownloadOnly {
		if err := rawdb.WriteReceipts(bc.db, block.NumberU64(), receipts); err != nil {
			return NonStatTy, err
		}
	}

	// If the total difficulty is higher than our known, add it to the canonical chain
	// Second clause in the if statement reduces the vulnerability to selfish mining.
	// Please refer to http://www.cs.cornell.edu/~ie53/publications/btcProcFC.pdf
	//reorg := externTd.Cmp(localTd) > 0
	//currentBlock = bc.CurrentBlock()
	//if !reorg && externTd.Cmp(localTd) == 0 {
	//	// Split same-difficulty blocks by number, then preferentially select
	//	// the block generated by the local miner as the canonical block.
	//	if block.NumberU64() < currentBlock.NumberU64() {
	//		reorg = true
	//	} else if block.NumberU64() == currentBlock.NumberU64() {
	//		var currentPreserve, blockPreserve bool
	//		if bc.shouldPreserve != nil {
	//			currentPreserve, blockPreserve = bc.shouldPreserve(currentBlock), bc.shouldPreserve(block)
	//		}
	//		reorg = !currentPreserve && (blockPreserve || mrand.Float64() < 0.5)
	//	}
	//}
	//if reorg {
	// Reorganise the chain if the parent is not the head block

	if block.ParentHash() != currentBlock.Hash() {
		if err := bc.reorg(currentBlock, block); err != nil {
			return NonStatTy, err
		}
	}
	// Write the positional metadata for transaction/receipt lookups and preimages

	if stateDb != nil && bc.enablePreimages && !bc.cacheConfig.DownloadOnly {
		rawdb.WritePreimages(bc.db, stateDb.Preimages())
	}

	status = CanonStatTy

	// Set new head.
	if err := bc.writeHeadBlock(block); err != nil {
		return NonStatTy, err
	}
	bc.futureBlocks.Remove(block.Hash())

	bc.chainFeed.Send(ChainEvent{Block: block, Hash: block.Hash(), Logs: logs})
	if len(logs) > 0 {
		bc.logsFeed.Send(logs)
	}
	// In theory we should fire a ChainHeadEvent when we inject
	// a canonical block, but sometimes we can insert a batch of
	// canonicial blocks. Avoid firing too much ChainHeadEvents,
	// we will fire an accumulated ChainHeadEvent and disable fire
	// event here.
	if emitHeadEvent {
		bc.chainHeadFeed.Send(ChainHeadEvent{Block: block})
	}
	return status, nil
}

// addFutureBlock checks if the block is within the max allowed window to get
// accepted for future processing, and returns an error if the block is too far
// ahead and was not added.
func (bc *BlockChain) addFutureBlock(block *types.Block) error {
	max := uint64(time.Now().Unix() + maxTimeFutureBlocks)
	if block.Time() > max {
		return fmt.Errorf("future block timestamp %v > allowed %v", block.Time(), max)
	}
	bc.futureBlocks.Add(block.Hash(), block)
	return nil
}

// InsertBodyChain attempts to insert the given batch of block into the
// canonical chain, without executing those blocks
func (bc *BlockChain) InsertBodyChain(logPrefix string, ctx context.Context, chain types.Blocks) (bool, error) {
	// Sanity check that we have something meaningful to import
	if len(chain) == 0 {
		return true, nil
	}
	// Remove already known canon-blocks
	var (
		block, prev *types.Block
	)
	// Do a sanity check that the provided chain is actually ordered and linked
	for i := 1; i < len(chain); i++ {
		block = chain[i]
		prev = chain[i-1]
		if block.NumberU64() != prev.NumberU64()+1 || block.ParentHash() != prev.Hash() {
			// Chain broke ancestry, log a message (programming error) and skip insertion
			log.Error("Non contiguous block insert", "number", block.Number(), "hash", block.Hash(),
				"parent", block.ParentHash(), "prevnumber", prev.Number(), "prevhash", prev.Hash())

			return true, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, prev.NumberU64(),
				prev.Hash().Bytes()[:4], i, block.NumberU64(), block.Hash().Bytes()[:4], block.ParentHash().Bytes()[:4])
		}
	}
	// Only insert if the difficulty of the inserted chain is bigger than existing chain
	// Pre-checks passed, start the full block imports
	if err := bc.addJob(); err != nil {
		return true, err
	}
	ctx = bc.WithContext(ctx, chain[0].Number())
	bc.chainmu.Lock()
	defer func() {
		bc.chainmu.Unlock()
		bc.doneJob()
	}()

	return InsertBodies(
		logPrefix,
		ctx,
		&bc.procInterrupt,
		chain,
		bc.db,
		bc.Config(),
		bc.NoHistory(),
		bc.IsNoHistory,
		&bc.currentBlock,
		&bc.committedBlock,
		bc.badBlocks,
	)
}

// InsertChain attempts to insert the given batch of blocks in to the canonical
// chain or, otherwise, create a fork. If an error is returned it will return
// the index number of the failing block as well an error describing what went
// wrong.
//
// After insertion is done, all accumulated events will be fired.
func (bc *BlockChain) InsertChain(ctx context.Context, chain types.Blocks) (int, error) {
	// Sanity check that we have something meaningful to import
	if len(chain) == 0 {
		return 0, nil
	}

	bc.blockProcFeed.Send(true)
	defer bc.blockProcFeed.Send(false)

	// Remove already known canon-blocks
	var (
		block, prev *types.Block
	)
	// Do a sanity check that the provided chain is actually ordered and linked
	for i := 1; i < len(chain); i++ {
		block = chain[i]
		prev = chain[i-1]
		if block.NumberU64() != prev.NumberU64()+1 || block.ParentHash() != prev.Hash() {
			// Chain broke ancestry, log a message (programming error) and skip insertion
			log.Error("Non contiguous block insert", "number", block.Number(), "hash", block.Hash(),
				"parent", block.ParentHash(), "prevnumber", prev.Number(), "prevhash", prev.Hash())

			return 0, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, prev.NumberU64(),
				prev.Hash().Bytes()[:4], i, block.NumberU64(), block.Hash().Bytes()[:4], block.ParentHash().Bytes()[:4])
		}
	}
	// Only insert if the difficulty of the inserted chain is bigger than existing chain
	// Pre-checks passed, start the full block imports
	if err := bc.addJob(); err != nil {
		return 0, err
	}
	ctx = bc.WithContext(ctx, chain[0].Number())
	bc.chainmu.Lock()
	defer func() {
		bc.chainmu.Unlock()
		bc.doneJob()
	}()
	n, err := bc.insertChain(ctx, chain, true)

	return n, err
}

// insertChain is the internal implementation of InsertChain, which assumes that
// 1) chains are contiguous, and 2) The chain mutex is held.
//
// This method is split out so that import batches that require re-injecting
// historical blocks can do so without releasing the lock, which could lead to
// racey behaviour. If a sidechain import is in progress, and the historic state
// is imported, but then new canon-head is added before the actual sidechain
// completes, then the historic state could be pruned again
func (bc *BlockChain) insertChain(ctx context.Context, chain types.Blocks, verifySeals bool) (int, error) {
	log.Info("Inserting chain",
		"start", chain[0].NumberU64(), "end", chain[len(chain)-1].NumberU64(),
		"current", bc.CurrentBlock().Number().Uint64(), "currentHeader", bc.CurrentHeader().Number.Uint64())

	// If the chain is terminating, don't even bother starting u
	if bc.insertStopped() {
		return 0, nil
	}

	// A queued approach to delivering events. This is generally
	// faster than direct delivery and requires much less mutex
	// acquiring.
	var (
		stats     = InsertStats{StartTime: mclock.Now()}
		lastCanon *types.Block
	)
	// Fire a single chain head event if we've progressed the chain
	defer func() {
		if lastCanon != nil && bc.CurrentBlock().Hash() == lastCanon.Hash() {
			bc.chainHeadFeed.Send(ChainHeadEvent{lastCanon})
		}
	}()

	for i, block := range chain {
		err := bc.engine.VerifyHeader(bc, block.Header(), verifySeals)
		switch {
		case errors.Is(err, ErrKnownBlock):
			// Block and state both already known. However if the current block is below
			// this number we did a rollback and we should reimport it nonetheless.
			if bc.CurrentBlock().NumberU64() >= block.NumberU64() {
				//fmt.Printf("Skipped known block %d\n", block.NumberU64())
				stats.ignored++

				// Special case. Commit the empty receipt slice if we meet the known
				// block in the middle. It can only happen in the clique chain. Whenever
				// we insert blocks via `insertSideChain`, we only commit `td`, `header`
				// and `body` if it's non-existent. Since we don't have receipts without
				// reexecution, so nothing to commit. But if the sidechain will be adpoted
				// as the canonical chain eventually, it needs to be reexecuted for missing
				// state, but if it's this special case here(skip reexecution) we will lose
				// the empty receipt entry.
				if len(block.Transactions()) == 0 {
					if err1 := rawdb.WriteReceipts(bc.db, block.NumberU64(), nil); err1 != nil {
						return i, err1
					}
				} else {
					log.Error("Please file an issue, skip known block execution without receipt",
						"hash", block.Hash(), "number", block.NumberU64())
				}
				continue
			}

		case errors.Is(err, consensus.ErrFutureBlock):
			// Allow up to MaxFuture second in the future blocks. If this limit is exceeded
			// the chain is discarded and processed at a later time if given.
			max := big.NewInt(time.Now().Unix() + maxTimeFutureBlocks)
			if block.Time() > max.Uint64() {
				return i, fmt.Errorf("future block: %v > %v", block.Time(), max)
			}
			bc.futureBlocks.Add(block.Hash(), block)
			stats.queued++
			continue

		case errors.Is(err, consensus.ErrUnknownAncestor) && bc.futureBlocks.Contains(block.ParentHash()):
			bc.futureBlocks.Add(block.Hash(), block)
			stats.queued++
			continue

		case errors.Is(err, consensus.ErrPrunedAncestor):
			// Block competing with the canonical chain, store in the db, but don't process
			// until the competitor TD goes above the canonical TD
			panic(err)

		case err != nil:
			bc.reportBlock(block, nil, err)
			return i, err
		}
		rawdb.WriteHeader(context.Background(), bc.db, block.Header())
	}

	externTd := big.NewInt(0)
	if len(chain) > 0 && chain[0].NumberU64() > 0 {
		d := bc.GetTd(chain[0].ParentHash(), chain[0].NumberU64()-1)
		if d != nil {
			externTd = externTd.Set(d)
		}
	}

	localTd := bc.GetTd(bc.CurrentBlock().Hash(), bc.CurrentBlock().NumberU64())

	var verifyFrom int
	for verifyFrom = 0; verifyFrom < len(chain) && localTd.Cmp(externTd) >= 0; verifyFrom++ {
		header := chain[verifyFrom].Header()
		externTd = externTd.Add(externTd, header.Difficulty)
		_ = rawdb.WriteTd(bc.db, header.Hash(), header.Number.Uint64(), externTd)
	}

	if localTd.Cmp(externTd) >= 0 {
		log.Warn("Ignoring the chain segment because of insufficient difficulty",
			"external", externTd,
			"local", localTd,
			"insertingNumber", chain[0].NumberU64(),
			"currentNumber", bc.CurrentBlock().Number().Uint64(),
		)

		// But we still write the blocks to the database because others might build on top of them
		td := bc.GetTd(chain[0].ParentHash(), chain[0].NumberU64()-1)
		for _, block := range chain {
			log.Warn("Saving", "block", block.NumberU64(), "hash", block.Hash())
			td = new(big.Int).Add(block.Difficulty(), td)
			if err := rawdb.WriteBlock(ctx, bc.db, block); err != nil {
				return 0, err
			}
			_ = rawdb.WriteTd(bc.db, block.Hash(), block.NumberU64(), td)
		}
		return 0, nil
	}
	var offset int
	var parent *types.Block
	var parentNumber = chain[0].NumberU64() - 1
	// Find correct insertion point for this chain

	preBlocks := []*types.Block{}
	parentHash := chain[0].ParentHash()
	parent = bc.GetBlock(parentHash, parentNumber)
	if parent == nil {
		log.Error("chain segment could not be inserted, missing parent", "hash", parentHash)
		return 0, fmt.Errorf("chain segment could not be inserted, missing parent %x", parentHash)
	}

	canonicalHash, err := rawdb.ReadCanonicalHash(bc.db, parentNumber)
	if err != nil {
		return 0, err
	}
	for canonicalHash != parentHash {
		log.Warn("Chain segment's parent not on canonical hash, adding to pre-blocks", "block", parentNumber, "hash", parentHash)
		preBlocks = append(preBlocks, parent)
		parentNumber--
		parentHash = parent.ParentHash()
		parent = bc.GetBlock(parentHash, parentNumber)
		if parent == nil {
			fmt.Printf("missing parent %x\n", parentHash)
			log.Error("chain segment could not be inserted, missing parent", "hash", parentHash)
			return 0, fmt.Errorf("chain segment could not be inserted, missing parent %x", parentHash)
		}
		canonicalHash, err = rawdb.ReadCanonicalHash(bc.db, parentNumber)
		if err != nil {
			return 0, err
		}
	}

	for left, right := 0, len(preBlocks)-1; left < right; left, right = left+1, right-1 {
		preBlocks[left], preBlocks[right] = preBlocks[right], preBlocks[left]
	}

	offset = len(preBlocks)
	if offset > 0 {
		chain = append(preBlocks, chain...)
	}

	var k int
	var committedK int
	// Iterate over the blocks and insert when the verifier permits
	for i, block := range chain {
		start := time.Now()
		if i >= offset {
			k = i - offset
		} else {
			k = 0
		}

		// If the chain is terminating, stop processing blocks
		if bc.insertStopped() {
			log.Debug("Abort during block processing")
			break
		}

		// If the header is a banned one, straight out abort
		if BadHashes[block.Hash()] {
			bc.reportBlock(block, nil, ErrBlacklistedHash)
			return k, ErrBlacklistedHash
		}

		// Wait for the block's verification to complete
		var err error
		ctx, _ = params.GetNoHistoryByBlock(ctx, block.Number())
		if err = bc.Validator().ValidateBody(ctx, block); err != nil {
			return k, err
		}

		// Create a new statedb using the parent block and report an
		// error if it fails.
		if i > 0 {
			parent = chain[i-1]
		}
		readBlockNr := parentNumber
		var root common.Hash
		if bc.trieDbState == nil && !bc.cacheConfig.DownloadOnly {
			if _, err = bc.GetTrieDbState(); err != nil {
				return k, err
			}
		}

		if !bc.cacheConfig.DownloadOnly {
			root = bc.trieDbState.LastRoot()
		}

		var parentRoot common.Hash
		if parent != nil {
			parentRoot = parent.Root()
		}

		if parent != nil && root != parentRoot && !bc.cacheConfig.DownloadOnly {
			log.Info("Rewinding from", "block", bc.CurrentBlock().NumberU64(), "to block", readBlockNr,
				"root", root.String(), "parentRoot", parentRoot.String())

			bc.committedBlock.Store(bc.currentBlock.Load())

			if err = bc.trieDbState.UnwindTo(readBlockNr); err != nil {
				log.Error("Could not rewind", "error", err)
				bc.setTrieDbState(nil)
				return k, err
			}

			root := bc.trieDbState.LastRoot()
			if root != parentRoot {
				log.Error("Incorrect rewinding", "root", fmt.Sprintf("%x", root), "expected", fmt.Sprintf("%x", parentRoot))
				bc.setTrieDbState(nil)
				return k, fmt.Errorf("incorrect rewinding: wrong root %x, expected %x", root, parentRoot)
			}
			currentBlock := bc.CurrentBlock()
			if err = bc.reorg(currentBlock, parent); err != nil {
				bc.setTrieDbState(nil)
				return k, err
			}

			committedK = k
			bc.committedBlock.Store(bc.currentBlock.Load())
		}

		var stateDB *state.IntraBlockState
		var receipts types.Receipts
		var usedGas uint64
		var logs []*types.Log
		if !bc.cacheConfig.DownloadOnly {
			stateDB = state.New(bc.trieDbState)
			// Process block using the parent state as reference point.
			receipts, logs, usedGas, root, err = bc.processor.PreProcess(block, stateDB, bc.trieDbState, bc.vmConfig)
			reuseTrieDbState := true
			if err != nil {
				bc.rollbackBadBlock(block, receipts, err, reuseTrieDbState)
				return k, err
			}

			err = bc.Validator().ValidateGasAndRoot(block, root, usedGas, bc.trieDbState)
			if err != nil {
				bc.rollbackBadBlock(block, receipts, err, reuseTrieDbState)
				return k, err
			}

			reuseTrieDbState = false
			err = bc.processor.PostProcess(block, bc.trieDbState, receipts)
			if err != nil {
				bc.rollbackBadBlock(block, receipts, err, reuseTrieDbState)
				return k, err
			}
			err = bc.Validator().ValidateReceipts(block, receipts)
			if err != nil {
				bc.rollbackBadBlock(block, receipts, err, reuseTrieDbState)
				return k, err
			}
		}
		proctime := time.Since(start)

		// Write the block to the chain and get the status.
		status, err := bc.writeBlockWithState(ctx, block, receipts, logs, stateDB, bc.trieDbState, false)
		if err != nil {
			bc.setTrieDbState(nil)
			if bc.committedBlock.Load() != nil {
				bc.currentBlock.Store(bc.committedBlock.Load())
			}
			return k, err
		}

		switch status {
		case CanonStatTy:
			log.Debug("Inserted new block", "number", block.Number(), "hash", block.Hash(),
				"uncles", len(block.Uncles()), "txs", len(block.Transactions()), "gas", block.GasUsed(),
				"elapsed", common.PrettyDuration(time.Since(start)),
				"root", block.Root())

			lastCanon = block

			// Only count canonical blocks for GC processing time
			bc.gcproc += proctime

		case SideStatTy:
			log.Debug("Inserted forked block", "number", block.Number(), "hash", block.Hash(),
				"diff", block.Difficulty(), "elapsed", common.PrettyDuration(time.Since(start)),
				"txs", len(block.Transactions()), "gas", block.GasUsed(), "uncles", len(block.Uncles()),
				"root", block.Root())

		default:
			// This in theory is impossible, but lets be nice to our future selves and leave
			// a log, instead of trying to track down blocks imports that don't emit logs.
			log.Warn("Inserted block with unknown status", "number", block.Number(), "hash", block.Hash(),
				"diff", block.Difficulty(), "elapsed", common.PrettyDuration(time.Since(start)),
				"txs", len(block.Transactions()), "gas", block.GasUsed(), "uncles", len(block.Uncles()),
				"root", block.Root())
		}
		stats.Processed++
		stats.UsedGas += usedGas
		toCommit := stats.NeedToCommit(chain, i)
		stats.Report("logPrefix", chain, i, toCommit)
		if toCommit {
			bc.committedBlock.Store(bc.currentBlock.Load())
			committedK = k
			if bc.trieDbState != nil {
				bc.trieDbState.EvictTries(false)
			}
		}
	}
	return committedK + 1, nil
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

// reorg takes two blocks, an old chain and a new chain and will reconstruct the
// blocks and inserts them to be part of the new canonical chain and accumulates
// potential missing transactions and post an event about them.
func (bc *BlockChain) reorg(oldBlock, newBlock *types.Block) error {
	var (
		newChain    types.Blocks
		oldChain    types.Blocks
		commonBlock *types.Block

		deletedTxs types.Transactions
		addedTxs   types.Transactions

		deletedLogs [][]*types.Log
		rebirthLogs [][]*types.Log

		// collectLogs collects the logs that were generated or removed during
		// the processing of the block that corresponds with the given hash.
		// These logs are later announced as deleted or reborn
		collectLogs = func(hash common.Hash, removed bool) {
			// Coalesce logs and set 'Removed'.
			number := bc.hc.GetBlockNumber(bc.db, hash)
			if number == nil {
				return
			}
			receipts := rawdb.ReadReceipts(bc.db, hash, *number)

			var logs []*types.Log
			for _, receipt := range receipts {
				for _, log := range receipt.Logs {
					l := *log
					if removed {
						l.Removed = true
					} else {
					}
					logs = append(logs, &l)
				}
			}
			if len(logs) > 0 {
				if removed {
					deletedLogs = append(deletedLogs, logs)
				} else {
					rebirthLogs = append(rebirthLogs, logs)
				}
			}
		}
		// mergeLogs returns a merged log slice with specified sort order.
		mergeLogs = func(logs [][]*types.Log, reverse bool) []*types.Log {
			var ret []*types.Log
			if reverse {
				for i := len(logs) - 1; i >= 0; i-- {
					ret = append(ret, logs[i]...)
				}
			} else {
				for i := 0; i < len(logs); i++ {
					ret = append(ret, logs[i]...)
				}
			}
			return ret
		}
	)

	// Reduce the longer chain to the same number as the shorter one
	// first reduce whoever is higher bound
	if oldBlock.NumberU64() > newBlock.NumberU64() {
		// Old chain is longer, gather all transactions and logs as deleted ones
		for ; oldBlock != nil && oldBlock.NumberU64() != newBlock.NumberU64(); oldBlock = bc.GetBlock(oldBlock.ParentHash(), oldBlock.NumberU64()-1) {
			oldChain = append(oldChain, oldBlock)
			deletedTxs = append(deletedTxs, oldBlock.Transactions()...)
			collectLogs(oldBlock.Hash(), true)
		}
	} else {
		// New chain is longer, stash all blocks away for subsequent insertion
		for ; newBlock != nil && newBlock.NumberU64() != oldBlock.NumberU64(); newBlock = bc.GetBlock(newBlock.ParentHash(), newBlock.NumberU64()-1) {
			newChain = append(newChain, newBlock)
		}
	}
	if oldBlock == nil {
		return fmt.Errorf("invalid old chain")
	}
	if newBlock == nil {
		return fmt.Errorf("invalid new chain")
	}
	// Both sides of the reorg are at the same number, reduce both until the common
	// ancestor is found
	for {
		// If the common ancestor was found, bail out
		if oldBlock.Hash() == newBlock.Hash() {
			commonBlock = oldBlock
			break
		}
		// Remove an old block as well as stash away a new block
		oldChain = append(oldChain, oldBlock)
		deletedTxs = append(deletedTxs, oldBlock.Transactions()...)
		collectLogs(oldBlock.Hash(), true)

		newChain = append(newChain, newBlock)

		// Step back with both chains
		oldBlock = bc.GetBlock(oldBlock.ParentHash(), oldBlock.NumberU64()-1)
		if oldBlock == nil {
			return fmt.Errorf("invalid old chain")
		}
		newBlock = bc.GetBlock(newBlock.ParentHash(), newBlock.NumberU64()-1)
		if newBlock == nil {
			return fmt.Errorf("invalid new chain")
		}
	}
	// Ensure the user sees large reorgs
	if len(oldChain) > 0 && len(newChain) > 0 {
		logFn := log.Info
		msg := "Chain reorg detected"
		if len(oldChain) > 63 {
			msg = "Large chain reorg detected"
			logFn = log.Warn
		}
		logFn(msg, "number", commonBlock.Number(), "hash", commonBlock.Hash(),
			"drop", len(oldChain), "dropfrom", oldChain[0].Hash(), "add", len(newChain), "addfrom", newChain[0].Hash())
		blockReorgAddMeter.Mark(int64(len(newChain)))
		blockReorgDropMeter.Mark(int64(len(oldChain)))
		blockReorgMeter.Mark(1)
	} else {
		log.Error("Impossible reorg, please file an issue", "oldnum", oldBlock.Number(), "oldhash", oldBlock.Hash(), "newnum", newBlock.Number(), "newhash", newBlock.Hash())
	}
	// Delete the old chain
	for _, oldBlock := range oldChain {
		_ = rawdb.DeleteCanonicalHeader(bc.db, oldBlock.NumberU64())
	}
	_ = bc.writeHeadBlock(commonBlock)
	// Insert the new chain, taking care of the proper incremental order
	for i := len(newChain) - 1; i >= 0; i-- {
		// insert the block in the canonical way, re-writing history
		_ = bc.writeHeadBlock(newChain[i])

		// Collect reborn logs due to chain reorg
		collectLogs(newChain[i].Hash(), false)

		// Write lookup entries for hash based transaction/receipt searches
		if bc.enableTxLookupIndex {
			rawdb.WriteTxLookupEntries(bc.db, newChain[i])
		}
		addedTxs = append(addedTxs, newChain[i].Transactions()...)
	}
	// When transactions get deleted from the database, the receipts that were
	// created in the fork must also be deleted
	for _, tx := range types.TxDifference(deletedTxs, addedTxs) {
		_ = rawdb.DeleteTxLookupEntry(bc.db, tx.Hash())
	}
	// Delete any canonical number assignments above the new head
	number := bc.CurrentBlock().NumberU64()
	for i := number + 1; ; i++ {
		hash, err := rawdb.ReadCanonicalHash(bc.db, i)
		if err != nil {
			return err
		}
		if hash == (common.Hash{}) {
			break
		}
		_ = rawdb.DeleteCanonicalHeader(bc.db, i)
	}

	// If any logs need to be fired, do it now. In theory we could avoid creating
	// this goroutine if there are no events to fire, but realistcally that only
	// ever happens if we're reorging empty blocks, which will only happen on idle
	// networks where performance is not an issue either way.
	if len(deletedLogs) > 0 {
		bc.rmLogsFeed.Send(RemovedLogsEvent{mergeLogs(deletedLogs, true)})
	}
	if len(rebirthLogs) > 0 {
		bc.logsFeed.Send(mergeLogs(rebirthLogs, false))
	}
	if len(oldChain) > 0 {
		for i := len(oldChain) - 1; i >= 0; i-- {
			bc.chainSideFeed.Send(ChainSideEvent{Block: oldChain[i]})
		}
	}
	return nil
}

// BadBlocks returns a list of the last 'bad blocks' that the client has seen on the network
func (bc *BlockChain) BadBlocks() []*types.Block {
	blocks := make([]*types.Block, 0, bc.badBlocks.Len())
	for _, hash := range bc.badBlocks.Keys() {
		if blk, exist := bc.badBlocks.Peek(hash); exist {
			block := blk.(*types.Block)
			blocks = append(blocks, block)
		}
	}
	return blocks
}

// addBadBlock adds a bad block to the bad-block LRU cache
func (bc *BlockChain) addBadBlock(block *types.Block) {
	bc.badBlocks.Add(block.Hash(), block)
}

// reportBlock logs a bad block error.
func (bc *BlockChain) reportBlock(block *types.Block, receipts types.Receipts, err error) {
	bc.addBadBlock(block)

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

func (bc *BlockChain) rollbackBadBlock(block *types.Block, receipts types.Receipts, err error, reuseTrieDbState bool) {
	if reuseTrieDbState {
		bc.setTrieDbState(bc.trieDbState.WithNewBuffer())
	} else {
		bc.setTrieDbState(nil)
	}
	bc.reportBlock(block, receipts, err)
	if bc.committedBlock.Load() != nil {
		bc.currentBlock.Store(bc.committedBlock.Load())
	}
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
	start := time.Now()
	if i, err := bc.hc.ValidateHeaderChain(chain, checkFreq); err != nil {
		return i, err
	}

	// Make sure only one thread manipulates the chain at once
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	if err := bc.addJob(); err != nil {
		return 0, err
	}
	defer bc.doneJob()

	whFunc := func(header *types.Header) error {
		_, err := bc.hc.WriteHeader(context.Background(), header)
		return err
	}
	n, err := bc.hc.InsertHeaderChain(chain, whFunc, start)
	return n, err
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

// GetAncestor retrieves the Nth ancestor of a given block. It assumes that either the given block or
// a close ancestor of it is canonical. maxNonCanonical points to a downwards counter limiting the
// number of blocks to be individually checked before we reach the canonical chain.
//
// Note: ancestor == 0 returns the same block, 1 returns its parent and so on.
func (bc *BlockChain) GetAncestor(hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	return bc.hc.GetAncestor(hash, number, ancestor, maxNonCanonical)
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

func (bc *BlockChain) IsNoHistory(currentBlock *big.Int) bool {
	if currentBlock == nil {
		return bc.cacheConfig.NoHistory
	}

	if !bc.cacheConfig.NoHistory {
		return false
	}

	var isArchiveInterval bool
	currentBlockNumber := bc.CurrentBlock().Number().Uint64()
	highestKnownBlock := bc.GetHeightKnownBlock()
	if highestKnownBlock > currentBlockNumber {
		isArchiveInterval = (currentBlock.Uint64() - highestKnownBlock) <= bc.cacheConfig.ArchiveSyncInterval
	} else {
		isArchiveInterval = (currentBlock.Uint64() - currentBlockNumber) <= bc.cacheConfig.ArchiveSyncInterval
	}

	return bc.cacheConfig.NoHistory || isArchiveInterval
}

func (bc *BlockChain) NotifyHeightKnownBlock(h uint64) {
	bc.highestKnownBlockMu.Lock()
	if bc.highestKnownBlock < h {
		bc.highestKnownBlock = h
	}
	bc.highestKnownBlockMu.Unlock()
}

func (bc *BlockChain) GetHeightKnownBlock() uint64 {
	bc.highestKnownBlockMu.Lock()
	defer bc.highestKnownBlockMu.Unlock()
	return bc.highestKnownBlock
}

func (bc *BlockChain) WithContext(ctx context.Context, blockNum *big.Int) context.Context {
	ctx = bc.Config().WithEIPsFlags(ctx, blockNum)
	ctx = params.WithNoHistory(ctx, bc.NoHistory(), bc.IsNoHistory)
	return ctx
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
	chainContext ChainContext,
	engine consensus.Engine,
	block *types.Block,
	stateReader state.StateReader,
	stateWriter state.WriterWithChangeSets,
) (types.Receipts, error) {
	defer blockExecutionTimer.UpdateSince(time.Now())

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
		receipt, err := ApplyTransaction(chainConfig, chainContext, nil, gp, ibs, noop, header, tx, usedGas, *vmConfig)
		if err != nil {
			return nil, fmt.Errorf("tx %x failed: %v", tx.Hash(), err)
		}
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
		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		engine.Finalize(chainConfig, header, ibs, block.Transactions(), block.Uncles())

		ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
		if err := ibs.CommitBlock(ctx, stateWriter); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %v", block.NumberU64(), err)
		}

		if err := stateWriter.WriteChangeSets(); err != nil {
			return nil, fmt.Errorf("writing changesets for block %d failed: %v", block.NumberU64(), err)
		}
	}

	return receipts, nil
}

// InsertBodies is insertChain with execute=false and ommission of blockchain object
func InsertBodies(
	logPrefix string,
	ctx context.Context,
	procInterrupt *int32,
	chain types.Blocks,
	db ethdb.Database,
	config *params.ChainConfig,
	noHistory bool,
	isNoHistory func(currentBlock *big.Int) bool,
	currentBlock *atomic.Value,
	committedBlock *atomic.Value,
	badBlocks *lru.Cache,
) (bool, error) {
	// If the chain is terminating, don't even bother starting u
	if atomic.LoadInt32(procInterrupt) == 1 {
		return true, nil
	}

	batch := db.NewBatch()
	stats := InsertStats{StartTime: mclock.Now()}

	var parent *types.Block
	var parentNumber = chain[0].NumberU64() - 1
	parentHash := chain[0].ParentHash()
	parent = rawdb.ReadBlock(batch, parentHash, parentNumber)
	if parent == nil {
		log.Error("chain segment could not be inserted, missing parent", "hash", parentHash)
		return true, fmt.Errorf("chain segment could not be inserted, missing parent %x", parentHash)
	}

	// Iterate over the blocks and insert when the verifier permits
	for _, block := range chain {
		start := time.Now()

		// If the chain is terminating, stop processing blocks
		if atomic.LoadInt32(procInterrupt) == 1 {
			return true, nil
		}

		// Calculate the total difficulty of the block
		ptd, err := rawdb.ReadTd(batch, block.ParentHash(), block.NumberU64()-1)
		if err != nil {
			return true, err
		}

		if ptd == nil {
			return true, consensus.ErrUnknownAncestor
		}

		// Irrelevant of the canonical status, write the block itself to the database
		if common.IsCanceled(ctx) {
			return true, ctx.Err()
		}

		rawdb.WriteBody(ctx, batch, block.Hash(), block.NumberU64(), block.Body())

		ctx = config.WithEIPsFlags(ctx, block.Number())
		ctx = params.WithNoHistory(ctx, noHistory, isNoHistory)

		log.Debug("Inserted new block", "number", block.Number(), "hash", block.Hash(),
			"uncles", len(block.Uncles()), "txs", len(block.Transactions()), "gas", block.GasUsed(),
			"elapsed", common.PrettyDuration(time.Since(start)),
			"root", block.Root())
	}
	stats.Processed = len(chain)
	stats.Report(logPrefix, chain, len(chain)-1, true)
	rawdb.WriteHeadBlockHash(db, chain[len(chain)-1].Hash())
	if _, err := batch.Commit(); err != nil {
		return true, fmt.Errorf("commit inserting bodies: %w", err)
	}
	return false, nil
}
