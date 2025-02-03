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

package core

import (
	"context"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/arb/arbitrum_types"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/event"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/services"
)

// Installs an Arbitrum TxProcessor, enabling ArbOS for this state transition (see vm/evm_arbitrum.go)
var ReadyEVMForL2 func(evm *vm.EVM, msg *types.Message)

// Allows ArbOS to swap out or return early from an RPC message to support the NodeInterface virtual contract
var InterceptRPCMessage = func(
	msg *types.Message,
	ctx context.Context,
	statedb *state.IntraBlockState,
	header *types.Header,
	backend NodeInterfaceBackendAPI,
	blockCtx *evmtypes.BlockContext,
) (*types.Message, *evmtypes.ExecutionResult, error) {
	return msg, nil, nil
}

// Gets ArbOS's maximum intended gas per second
var GetArbOSSpeedLimitPerSecond func(statedb state.IntraBlockStateArbitrum) (uint64, error)

// Allows ArbOS to update the gas cap so that it ignores the message's specific L1 poster costs.
var InterceptRPCGasCap = func(gascap *uint64, msg *types.Message, header *types.Header, statedb *state.IntraBlockState) {}

// Renders a solidity error in human-readable form
var RenderRPCError func(data []byte) error

type NodeInterfaceBackendAPI interface {
	ChainConfig() *chain.Config
	// CurrentHeader() *types.Header
	CurrentBlock() *types.Block
	BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error)
	HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
	GetLogs(ctx context.Context, blockHash common.Hash, number uint64) ([][]*types.Log, error)
	GetEVM(ctx context.Context, msg *types.Message, state *state.IntraBlockState, header *types.Header, vmConfig *vm.Config, blockCtx *evmtypes.BlockContext) *vm.EVM
}

// Arbitrum widely uses BlockChain structure so better to wrap interface here
type BlockChain interface {
	services.FullBlockReader
	ChainReader() consensus.ChainHeaderReader // may be useful more than embedding of FullBlockReader itself
	Engine() consensus.Engine

	// Config retrieves the chain's fork configuration.
	Config() *chain.Config

	Genesis() *types.Block
	// Stop stops the blockchain service. If any imports are currently in progress
	// it will abort them using the procInterrupt.
	Stop()

	// State returns a new mutable state based on the current HEAD block.
	State() (state.IntraBlockStateArbitrum, error)

	// StateAt returns a new mutable state based on a particular point in time.
	StateAt(root common.Hash) (state.IntraBlockStateArbitrum, error)

	ClipToPostNitroGenesis(blockNum rpc.BlockNumber) (rpc.BlockNumber, rpc.BlockNumber)

	RecoverState(block *types.Block) error

	ReorgToOldBlock(newHead *types.Block) error

	// WriteBlockAndSetHeadWithTime also counts processTime, which will cause intermittent TrieDirty cache writes
	WriteBlockAndSetHeadWithTime(block *types.Block, receipts []*types.Receipt, logs []*types.Log, state state.IntraBlockStateArbitrum, emitHeadEvent bool, processTime time.Duration) (status WriteStatus, err error)

	GetReceiptsByHash(hash common.Hash) types.Receipts
	// StateCache returns the caching database underpinning the blockchain instance.
	StateCache() state.WasmIface //kv.RwDB // TODO could return wasm storage only

	SharedDomains() *state2.SharedDomains

	ResetWithGenesisBlock(gb *types.Block)
	SubscribeNewTxsEvent(ch chan<- NewTxsEvent) event.Subscription
	EnqueueL2Message(ctx context.Context, tx types.Transaction, options *arbitrum_types.ConditionalOptions) error

	// GetVMConfig returns the block chain VM config.
	GetVMConfig() *vm.Config

	GetTd(common.Hash, uint64) *big.Int
	// Processor returns the current processor.
	Processor() Processor
}

// Processor is an interface for processing blocks using a given initial state.
type Processor interface {
	// Process processes the state changes according to the Ethereum rules by running
	// the transaction messages using the statedb and applying any rewards to both
	// the processor (coinbase) and any included uncles.
	Process(block *types.Block, statedb state.IntraBlockStateArbitrum, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error)
}

// func (b *Backend) ResetWithGenesisBlock(gb *types.Block) {
// 	b.arb.BlockChain().ResetWithGenesisBlock(gb)
// }

// func (b *Backend) EnqueueL2Message(ctx context.Context, tx *types.Transaction, options *arbitrum_types.ConditionalOptions) error {
// 	return b.arb.PublishTransaction(ctx, tx, options)
// }

// func (b *Backend) SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription {
// 	return b.scope.Track(b.txFeed.Subscribe(ch))
// }

// Processor returns the current processor.
// func (bc *BlockChain) Processor() Processor {
// 	return bc.processor
// }

// // State returns a new mutable state based on the current HEAD block.
// func (bc *BlockChain) State() (*state.StateDB, error) {
// 	return bc.StateAt(bc.CurrentBlock().Root)
// }

// // StateAt returns a new mutable state based on a particular point in time.
// func (bc *BlockChain) StateAt(root common.Hash) (*state.StateDB, error) {
// 	return state.New(root, bc.stateCache, bc.snaps)
// }

func NewBlockChain(db kv.RwTx /*cacheConfig *CacheConfig, */, chainConfig *chain.Config, genesis *types.Genesis /* overrides *types.ChainOverrides, */, engine consensus.Engine, vmConfig vm.Config, shouldPreserve func(header *types.Header) bool, txLookupLimit *uint64) (BlockChain, error) {
	return nil, nil
}

// NewBlockChain returns a fully initialised block chain using information
// available in the database. It initialises the default Ethereum Validator
// and Processor.
// func NewBlockChainOG(db ethdb.Database, cacheConfig *CacheConfig, chainConfig *params.ChainConfig, genesis *Genesis, overrides *ChainOverrides, engine consensus.Engine, vmConfig vm.Config, shouldPreserve func(header *types.Header) bool, txLookupLimit *uint64) (*BlockChain, error) {
// 	if cacheConfig == nil {
// 		cacheConfig = defaultCacheConfig
// 	}
// 	// Open trie database with provided config
// 	triedb := triedb.NewDatabase(db, cacheConfig.triedbConfig())

// 	var genesisHash common.Hash
// 	var genesisErr error

// 	if chainConfig != nil && chainConfig.IsArbitrum() {
// 		genesisHash = rawdb.ReadCanonicalHash(db, chainConfig.ArbitrumChainParams.GenesisBlockNum)
// 		if genesisHash == (common.Hash{}) {
// 			return nil, ErrNoGenesis
// 		}
// 	} else {
// 		// Setup the genesis block, commit the provided genesis specification
// 		// to database if the genesis block is not present yet, or load the
// 		// stored one from database.
// 		chainConfig, genesisHash, genesisErr = SetupGenesisBlockWithOverride(db, triedb, genesis, overrides)
// 		if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
// 			return nil, genesisErr
// 		}
// 	}
// 	log.Info("")
// 	log.Info(strings.Repeat("-", 153))
// 	for _, line := range strings.Split(chainConfig.Description(), "\n") {
// 		log.Info(line)
// 	}
// 	log.Info(strings.Repeat("-", 153))
// 	log.Info("")

// 	bc := &BlockChain{
// 		chainConfig:   chainConfig,
// 		cacheConfig:   cacheConfig,
// 		db:            db,
// 		triedb:        triedb,
// 		triegc:        prque.New[int64, trieGcEntry](nil),
// 		quit:          make(chan struct{}),
// 		chainmu:       syncx.NewClosableMutex(),
// 		bodyCache:     lru.NewCache[common.Hash, *types.Body](bodyCacheLimit),
// 		bodyRLPCache:  lru.NewCache[common.Hash, rlp.RawValue](bodyCacheLimit),
// 		receiptsCache: lru.NewCache[common.Hash, []*types.Receipt](receiptsCacheLimit),
// 		blockCache:    lru.NewCache[common.Hash, *types.Block](blockCacheLimit),
// 		txLookupCache: lru.NewCache[common.Hash, txLookup](txLookupCacheLimit),
// 		futureBlocks:  lru.NewCache[common.Hash, *types.Block](maxFutureBlocks),
// 		engine:        engine,
// 		vmConfig:      vmConfig,
// 	}
// 	bc.flushInterval.Store(int64(cacheConfig.TrieTimeLimit))
// 	bc.forker = NewForkChoice(bc, shouldPreserve)
// 	bc.stateCache = state.NewDatabaseWithNodeDB(bc.db, bc.triedb)
// 	bc.validator = NewBlockValidator(chainConfig, bc, engine)
// 	bc.prefetcher = newStatePrefetcher(chainConfig, bc, engine)
// 	bc.processor = NewStateProcessor(chainConfig, bc, engine)

// 	var err error
// 	bc.hc, err = NewHeaderChain(db, chainConfig, engine, bc.insertStopped)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if chainConfig.IsArbitrum() {
// 		bc.genesisBlock = bc.GetBlockByNumber(chainConfig.ArbitrumChainParams.GenesisBlockNum)
// 	} else {
// 		bc.genesisBlock = bc.GetBlockByNumber(0)
// 	}
// 	if bc.genesisBlock == nil {
// 		return nil, ErrNoGenesis
// 	}

// 	bc.currentBlock.Store(nil)
// 	bc.currentSnapBlock.Store(nil)
// 	bc.currentFinalBlock.Store(nil)
// 	bc.currentSafeBlock.Store(nil)

// 	// Update chain info data metrics
// 	chainInfoGauge.Update(metrics.GaugeInfoValue{"chain_id": bc.chainConfig.ChainID.String()})

// 	// If Geth is initialized with an external ancient store, re-initialize the
// 	// missing chain indexes and chain flags. This procedure can survive crash
// 	// and can be resumed in next restart since chain flags are updated in last step.
// 	if bc.empty() {
// 		rawdb.InitDatabaseFromFreezer(bc.db)
// 	}
// 	// Load blockchain states from disk
// 	if err := bc.loadLastState(); err != nil {
// 		return nil, err
// 	}
// 	// Make sure the state associated with the block is available, or log out
// 	// if there is no available state, waiting for state sync.
// 	head := bc.CurrentBlock()
// 	if !bc.HasState(head.Root) {
// 		if head.Number.Uint64() <= bc.genesisBlock.NumberU64() {
// 			// The genesis state is missing, which is only possible in the path-based
// 			// scheme. This situation occurs when the initial state sync is not finished
// 			// yet, or the chain head is rewound below the pivot point. In both scenarios,
// 			// there is no possible recovery approach except for rerunning a snap sync.
// 			// Do nothing here until the state syncer picks it up.
// 			log.Info("Genesis state is missing, wait state sync")
// 		} else {
// 			// Head state is missing, before the state recovery, find out the
// 			// disk layer point of snapshot(if it's enabled). Make sure the
// 			// rewound point is lower than disk layer.
// 			var diskRoot common.Hash
// 			if bc.cacheConfig.SnapshotLimit > 0 {
// 				diskRoot = rawdb.ReadSnapshotRoot(bc.db)
// 			}
// 			if diskRoot != (common.Hash{}) {
// 				log.Warn("Head state missing, repairing", "number", head.Number, "hash", head.Hash(), "snaproot", diskRoot)

// 				snapDisk, diskRootFound, err := bc.setHeadBeyondRoot(head.Number.Uint64(), 0, diskRoot, true, bc.cacheConfig.SnapshotRestoreMaxGas)
// 				if err != nil {
// 					return nil, err
// 				}
// 				// Chain rewound, persist old snapshot number to indicate recovery procedure
// 				if diskRootFound {
// 					rawdb.WriteSnapshotRecoveryNumber(bc.db, snapDisk)
// 				} else {
// 					log.Warn("Snapshot root not found or too far back. Recreating snapshot from scratch.")
// 					rawdb.DeleteSnapshotRecoveryNumber(bc.db)
// 				}
// 			} else {
// 				log.Warn("Head state missing, repairing", "number", head.Number, "hash", head.Hash())
// 				if _, _, err := bc.setHeadBeyondRoot(head.Number.Uint64(), 0, common.Hash{}, true, 0); err != nil {
// 					return nil, err
// 				}
// 			}
// 		}
// 	}
// 	// Ensure that a previous crash in SetHead doesn't leave extra ancients
// 	if frozen, err := bc.db.Ancients(); err == nil && frozen > 0 {
// 		var (
// 			needRewind bool
// 			low        uint64
// 		)
// 		// The head full block may be rolled back to a very low height due to
// 		// blockchain repair. If the head full block is even lower than the ancient
// 		// chain, truncate the ancient store.
// 		fullBlock := bc.CurrentBlock()
// 		if fullBlock != nil && fullBlock.Hash() != bc.genesisBlock.Hash() && fullBlock.Number.Uint64() < frozen-1 {
// 			needRewind = true
// 			low = fullBlock.Number.Uint64()
// 		}
// 		// In snap sync, it may happen that ancient data has been written to the
// 		// ancient store, but the LastFastBlock has not been updated, truncate the
// 		// extra data here.
// 		snapBlock := bc.CurrentSnapBlock()
// 		if snapBlock != nil && snapBlock.Number.Uint64() < frozen-1 {
// 			needRewind = true
// 			if snapBlock.Number.Uint64() < low || low == 0 {
// 				low = snapBlock.Number.Uint64()
// 			}
// 		}
// 		if needRewind {
// 			log.Error("Truncating ancient chain", "from", bc.CurrentHeader().Number.Uint64(), "to", low)
// 			if err := bc.SetHead(low); err != nil {
// 				return nil, err
// 			}
// 		}
// 	}
// 	// The first thing the node will do is reconstruct the verification data for
// 	// the head block (ethash cache or clique voting snapshot). Might as well do
// 	// it in advance.
// 	bc.engine.VerifyHeader(bc, bc.CurrentHeader())

// 	// Check the current state of the block hashes and make sure that we do not have any of the bad blocks in our chain
// 	for hash := range BadHashes {
// 		if header := bc.GetHeaderByHash(hash); header != nil {
// 			// get the canonical block corresponding to the offending header's number
// 			headerByNumber := bc.GetHeaderByNumber(header.Number.Uint64())
// 			// make sure the headerByNumber (if present) is in our current canonical chain
// 			if headerByNumber != nil && headerByNumber.Hash() == header.Hash() {
// 				log.Error("Found bad hash, rewinding chain", "number", header.Number, "hash", header.ParentHash)
// 				if err := bc.SetHead(header.Number.Uint64() - 1); err != nil {
// 					return nil, err
// 				}
// 				log.Error("Chain rewind was successful, resuming normal operation")
// 			}
// 		}
// 	}

// 	// Load any existing snapshot, regenerating it if loading failed
// 	if bc.cacheConfig.SnapshotLimit > 0 {
// 		// If the chain was rewound past the snapshot persistent layer (causing
// 		// a recovery block number to be persisted to disk), check if we're still
// 		// in recovery mode and in that case, don't invalidate the snapshot on a
// 		// head mismatch.
// 		var recover bool

// 		head := bc.CurrentBlock()
// 		if layer := rawdb.ReadSnapshotRecoveryNumber(bc.db); layer != nil && *layer >= head.Number.Uint64() {
// 			log.Warn("Enabling snapshot recovery", "chainhead", head.Number, "diskbase", *layer)
// 			recover = true
// 		}
// 		snapconfig := snapshot.Config{
// 			CacheSize:  bc.cacheConfig.SnapshotLimit,
// 			Recovery:   recover,
// 			NoBuild:    bc.cacheConfig.SnapshotNoBuild,
// 			AsyncBuild: !bc.cacheConfig.SnapshotWait,
// 		}
// 		bc.snaps, _ = snapshot.New(snapconfig, bc.db, bc.triedb, head.Root)
// 	}

// 	// Start future block processor.
// 	bc.wg.Add(1)
// 	go bc.updateFutureBlocks()

// 	// Rewind the chain in case of an incompatible config upgrade.
// 	if compat, ok := genesisErr.(*params.ConfigCompatError); ok {
// 		log.Warn("Rewinding chain to upgrade configuration", "err", compat)
// 		if compat.RewindToTime > 0 {
// 			bc.SetHeadWithTimestamp(compat.RewindToTime)
// 		} else {
// 			bc.SetHead(compat.RewindToBlock)
// 		}
// 		rawdb.WriteChainConfig(db, genesisHash, chainConfig)
// 	}
// 	// Start tx indexer if it's enabled.
// 	if txLookupLimit != nil {
// 		bc.txIndexer = newTxIndexer(*txLookupLimit, bc)
// 	}
// 	return bc, nil
// }
