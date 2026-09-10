// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/concurrent"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	ethapi2 "github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	// Block related (proposed file: ./eth_blocks.go)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]any, error)
	GetBlockByHash(ctx context.Context, hash rpc.BlockNumberOrHash, fullTx bool) (map[string]any, error)
	GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error)
	GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error)

	// Transaction related (see ./eth_txs.go)
	GetTransactionByHash(ctx context.Context, hash common.Hash) (*ethapi.RPCTransaction, error)
	GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (*ethapi.RPCTransaction, error)
	GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*ethapi.RPCTransaction, error)
	GetRawTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (hexutil.Bytes, error)
	GetRawTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index hexutil.Uint) (hexutil.Bytes, error)
	GetRawTransactionByHash(ctx context.Context, hash common.Hash) (hexutil.Bytes, error)

	// Receipt related (see ./eth_receipts.go)
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]any, error)
	GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.RPCLogs, error)
	GetBlockReceipts(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) ([]map[string]any, error)

	// Block access list related (see ./eth_block_access_list.go)
	GetBlockAccessList(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) ([]*ethapi.RPCAccountAccess, error)

	// Uncle related (see ./eth_uncles.go)
	GetUncleByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (map[string]any, error)
	GetUncleByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint) (map[string]any, error)
	GetUncleCountByBlockNumber(ctx context.Context, number rpc.BlockNumber) (*hexutil.Uint, error)
	GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (*hexutil.Uint, error)

	// Filter related (see ./eth_filters.go)
	NewPendingTransactionFilter(_ context.Context) (string, error)
	NewBlockFilter(_ context.Context) (string, error)
	NewFilter(_ context.Context, crit filters.FilterCriteria) (string, error)
	UninstallFilter(_ context.Context, index string) (bool, error)
	GetFilterChanges(_ context.Context, index string) ([]any, error)
	GetFilterLogs(ctx context.Context, index string) (types.RPCLogs, error)
	Logs(ctx context.Context, crit filters.FilterCriteria) (*rpc.Subscription, error)

	// Account related (see ./eth_accounts.go)
	Accounts(ctx context.Context) ([]common.Address, error)
	GetBalance(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (*hexutil.U256, error)
	GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (*hexutil.Uint64, error)
	GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash *rpc.BlockNumberOrHash) (string, error)
	GetStorageValues(ctx context.Context, requests map[common.Address][]common.Hash, blockNrOrHash *rpc.BlockNumberOrHash) (map[common.Address][]hexutil.Bytes, error)
	GetCode(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (hexutil.Bytes, error)

	// System related (see ./eth_system.go)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	Syncing(ctx context.Context) (any, error)
	ChainId(ctx context.Context) (hexutil.Uint64, error) /* called eth_protocolVersion elsewhere */
	ProtocolVersion(_ context.Context) (hexutil.Uint, error)
	GasPrice(_ context.Context) (*hexutil.U256, error)
	BaseFee(ctx context.Context) (*hexutil.U256, error)
	BlobBaseFee(ctx context.Context) (*hexutil.U256, error)
	Config(ctx context.Context, timeArg *hexutil.Uint64) (*EthConfigResp, error)
	Capabilities(ctx context.Context) (*CapabilitiesResult, error)

	// Sending related (see ./eth_call.go)
	Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi.StateOverrides, blockOverrides *ethapi.BlockOverrides) (hexutil.Bytes, error)
	EstimateGas(ctx context.Context, argsOrNil *ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi.StateOverrides, blockOverrides *ethapi.BlockOverrides) (hexutil.Uint64, error)

	// Simulation related (see ./eth_simulation.go)
	SimulateV1(ctx context.Context, req SimulationRequest, blockParameter rpc.BlockNumberOrHash) (SimulationResult, error)
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
	SendRawTransactionSync(ctx context.Context, encodedTx hexutil.Bytes, timeoutMs *uint64) (map[string]any, error)
	SendTransaction(_ context.Context, txObject any) (common.Hash, error)
	Sign(ctx context.Context, _ common.Address, _ hexutil.Bytes) (hexutil.Bytes, error)
	SignTransaction(_ context.Context, txObject any) (common.Hash, error)
	FillTransaction(ctx context.Context, args ethapi.CallArgs) (*ethapi.SignTransactionResult, error)
	GetProof(ctx context.Context, address common.Address, storageKeys []hexutil.Bytes, blockNr *rpc.BlockNumberOrHash) (*accounts.AccProofResult, error)
	CreateAccessList(ctx context.Context, args ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides, optimizeGas *bool) (*accessListResult, error)

	// Mining related (see ./eth_mining.go)
	Coinbase(ctx context.Context) (common.Address, error)
	Hashrate(ctx context.Context) (uint64, error)
	Mining(ctx context.Context) (bool, error)
	GetWork(ctx context.Context) ([4]string, error)
	SubmitWork(ctx context.Context, nonce types.BlockNonce, powHash, digest common.Hash) (bool, error)
	SubmitHashrate(ctx context.Context, hashRate hexutil.Uint64, id common.Hash) (bool, error)
}

type BaseAPI struct {
	// all caches are thread-safe
	stateCache kvcache.Cache
	blocksLRU  *lru.Cache[common.Hash, *types.Block]

	filters                   *rpchelper.Filters
	_chainConfig              atomic.Pointer[chain.Config]
	_genesis                  atomic.Pointer[types.Block]
	_pruneMode                atomic.Pointer[prune.Mode]
	_commitmentHistoryEnabled atomic.Pointer[bool]
	// _preMergeData is kept for a TTL rather than settled once: it reads live snapshot
	// availability, which widens as segments arrive.
	_preMergeData concurrent.CachedValue[bool]

	_blockReader dbservices.FullBlockReader
	_txNumReader rawdbv3.TxNumsReader
	_txnReader   dbservices.TxnReader
	_engine      rules.EngineReader

	evmCallTimeout    time.Duration
	blockRangeLimit   int
	getLogsMaxResults int
	logQueryLimit     int
	dirs              datadir.Dirs
	receiptsGenerator *receipts.Generator
	balRegenerator    *bal.Regenerator

	// witnessCache serves recent legacy-mode debug_executionWitness results from
	// memory, keyed by block hash; nil disables it (only the embedded node wires one).
	// It is the single source of truth for head-capture/cache-only serving mode, read
	// by both the debug and eth_getWitness serve paths.
	witnessCache *witnessResultCache
}

func NewBaseApi(f *rpchelper.Filters, stateCache kvcache.Cache, blockReader dbservices.FullBlockReader, engine rules.Engine, conf *rpccfg.BaseApiConfig) *BaseAPI {
	if conf == nil {
		conf = &rpccfg.BaseApiConfig{}
	}
	blocksLRUSize := 128 // ~32Mb
	// if RPCDaemon deployed as independent process: increase cache sizes
	if !conf.SingleNodeMode {
		blocksLRUSize *= 5
	}
	blocksLRU, err := lru.New[common.Hash, *types.Block](blocksLRUSize)
	if err != nil {
		panic(err)
	}

	evmCallTimeout := conf.EvmCallTimeout
	if evmCallTimeout == 0 {
		evmCallTimeout = rpccfg.DefaultEvmCallTimeout
	}

	api := &BaseAPI{
		filters:           f,
		stateCache:        stateCache,
		blocksLRU:         blocksLRU,
		_blockReader:      blockReader,
		_txnReader:        blockReader,
		_txNumReader:      blockReader.TxnumReader(),
		evmCallTimeout:    evmCallTimeout,
		_engine:           engine,
		receiptsGenerator: receipts.NewGenerator(conf.Dirs, blockReader, engine, stateCache, evmCallTimeout, f),
		balRegenerator:    bal.NewRegenerator(blockReader, engine, log.Root()),
		dirs:              conf.Dirs,
		blockRangeLimit:   conf.BlockRangeLimit,
		getLogsMaxResults: conf.GetLogsMaxResults,
		logQueryLimit:     conf.LogQueryLimit,
	}
	api._preMergeData.SetTTL(defaultPreMergeDataTTL)
	return api
}

func (api *BaseAPI) chainConfig(ctx context.Context, tx kv.Tx) (*chain.Config, error) {
	cfg, _, err := api.chainConfigWithGenesis(ctx, tx)
	return cfg, err
}

func (api *BaseAPI) chainConfigWithGenesis(ctx context.Context, tx kv.Tx) (*chain.Config, *types.Block, error) {
	cc, genesisBlock := api._chainConfig.Load(), api._genesis.Load()
	if cc != nil && genesisBlock != nil {
		return cc, genesisBlock, nil
	}

	genesisBlock, err := api.blockByNumberWithSenders(ctx, api.filters.WithOverlay(tx), 0)
	if err != nil {
		return nil, nil, err
	}
	if genesisBlock == nil {
		return nil, nil, errors.New("genesis block not found in database")
	}
	cc, err = rawdb.ReadChainConfig(tx, genesisBlock.Hash())
	if err != nil {
		return nil, nil, err
	}
	if cc != nil {
		api._genesis.Store(genesisBlock)
		api._chainConfig.Store(cc)
	}
	return cc, genesisBlock, nil
}

func (api *BaseAPI) pendingBlock() *types.Block {
	if api.filters == nil {
		return nil
	}
	return api.filters.LastPendingBlock()
}

// resolveCommittedBlockNumber resolves a selector only when its canonical block
// is available in tx. If tx cannot resolve it, the overlay probe distinguishes
// an unknown selector from a known block that is unavailable in the committed
// view. The probe never changes the selected transaction.
func (api *BaseAPI) resolveCommittedBlockNumber(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash) (uint64, error) {
	blockNumber, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, nil)
	if _, ok := errors.AsType[rpc.BlockNotFoundErr](err); !ok {
		return blockNumber, err
	}

	overlayBlockNumber, _, _, overlayErr := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, api.filters.WithOverlay(tx), api._blockReader, nil)
	if overlayErr != nil {
		return 0, overlayErr
	}
	if err := rpchelper.CheckBlockExecuted(tx, overlayBlockNumber); err != nil {
		return 0, err
	}

	// Execution progress alone is insufficient after an overlay reorg because tx
	// still exposes state for the previously committed canonical block.
	return 0, fmt.Errorf("block %s is not available in the committed view", blockNrOrHash.String())
}

func (api *BaseAPI) engine() rules.EngineReader {
	return api._engine
}

func (api *BaseAPI) txnLookup(ctx context.Context, tx kv.Tx, txnHash common.Hash) (blockNum uint64, txNum uint64, ok bool, err error) {
	return api._txnReader.TxnLookup(ctx, tx, txnHash)
}

// txnIndexInBlock derives the in-block txn index from a global txNum.
func (api *BaseAPI) txnIndexInBlock(ctx context.Context, tx kv.Tx, blockNum, txNum uint64) (int, error) {
	txNumMin, err := api._txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return 0, err
	}
	if txNumMin+1 > txNum {
		return 0, fmt.Errorf("uint underflow txnums error txNum: %d, txNumMin: %d, blockNum: %d", txNum, txNumMin, blockNum)
	}
	return int(txNum - txNumMin - 1), nil
}

func (api *BaseAPI) blockByNumberWithSenders(ctx context.Context, tx kv.Tx, number uint64) (*types.Block, error) {
	hash, ok, err := api._blockReader.CanonicalHash(ctx, tx, number)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return api.blockWithSenders(ctx, tx, hash, number)
}

func (api *BaseAPI) blockByHashWithSenders(ctx context.Context, tx kv.Tx, hash common.Hash) (*types.Block, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it, nil
		}
	}
	number, err := api._blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if number == nil {
		return nil, nil
	}

	return api.blockWithSenders(ctx, tx, hash, *number)
}

func (api *BaseAPI) blockWithSenders(ctx context.Context, tx kv.Tx, hash common.Hash, number uint64) (*types.Block, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it, nil
		}
	}
	block, _, err := api._blockReader.BlockWithSenders(ctx, tx, hash, number)
	if err != nil {
		return nil, err
	}
	if block == nil { // don't save nil's to cache
		return nil, nil
	}
	// don't save empty blocks to cache, because in Erigon
	// if block become non-canonical - we remove it's transactions, but block can become canonical in future
	if block.Transactions().Len() == 0 {
		return block, nil
	}
	if api.blocksLRU != nil {
		// calc fields before put to cache
		for _, txn := range block.Transactions() {
			txn.Hash()
		}
		block.Hash()
		api.blocksLRU.Add(hash, block)
	}
	return block, nil
}

func (api *BaseAPI) headerByHashAndNumber(ctx context.Context, tx kv.Getter, hash common.Hash, number uint64) (*types.Header, error) {
	if api.blocksLRU != nil {
		if block, ok := api.blocksLRU.Get(hash); ok && block != nil {
			return block.HeaderNoCopy(), nil
		}
	}
	return api._blockReader.Header(ctx, tx, hash, number)
}

func (api *BaseAPI) canonicalHeaderByNumber(ctx context.Context, tx kv.Getter, number uint64) (*types.Header, error) {
	hash, ok, err := api._blockReader.CanonicalHash(ctx, tx, number)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return api.headerByHashAndNumber(ctx, tx, hash, number)
}

func (api *BaseAPI) headerNumberByHash(ctx context.Context, tx kv.Tx, hash common.Hash) (uint64, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it.NumberU64(), nil
		}
	}
	number, err := api._blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return 0, err
	}

	if number == nil {
		return 0, errors.New("header number not found")
	}
	return *number, nil

}

// headerByNumberOrHash - intent to read recent headers only, tries from the lru cache before reading from the db
func (api *BaseAPI) headerByNumberOrHash(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash) (*types.Header, bool, error) {
	blockNum, hash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, false, err
	}
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it.HeaderNoCopy(), isLatest, nil
		}
	}

	overlayTx := api.filters.WithOverlay(tx)
	header, err := api._blockReader.HeaderByNumber(ctx, overlayTx, blockNum)
	if err != nil {
		return nil, false, err
	}
	return header, isLatest, nil
}

// canonicalHeaderByNumberOrHash resolves the selector and header through tx.
// It never selects an overlay, so callers can keep dependent reads on one view.
func (api *BaseAPI) canonicalHeaderByNumberOrHash(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash) (*types.Header, bool, error) {
	if number, ok := blockNrOrHash.Number(); ok && number == rpc.PendingBlockNumber {
		return nil, false, nil
	}
	blockNum, hash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, nil)
	if err != nil {
		return nil, false, err
	}
	header, err := api.headerByHashAndNumber(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, false, err
	}
	return header, isLatest, nil
}

func (api *BaseAPI) headerByNumber(ctx context.Context, number rpc.BlockNumber, tx kv.Tx) (*types.Header, error) {
	// Pending headers are not stored in the block tables; do not substitute latest.
	if number == rpc.PendingBlockNumber {
		return nil, nil
	}
	overlayTx := api.filters.WithOverlay(tx)
	n, h, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(number), overlayTx, api._blockReader, nil)
	if err != nil {
		return nil, err
	}
	return api.headerByHashAndNumber(ctx, overlayTx, h, n)
}

func (api *BaseAPI) headerByHash(ctx context.Context, hash common.Hash, tx kv.Tx) (*types.Header, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it.HeaderNoCopy(), nil
		}
	}

	overlayTx := api.filters.WithOverlay(tx)
	number, err := api._blockReader.HeaderNumber(ctx, overlayTx, hash)
	if err != nil {
		return nil, err
	}

	if number == nil {
		return nil, nil
	}
	return api._blockReader.Header(ctx, overlayTx, hash, *number)
}

const defaultPreMergeDataTTL = 30 * time.Second

// systemTxsPerBlock is the pair of system entries every block carries in the txnum
// sequence, which a stored TxCount includes.
const systemTxsPerBlock = 2

// checks the pruning state to see if we would hold information about this
// block in state history or not.  Some strange issues arise getting account
// history for blocks that have been pruned away giving nonce too low errors
// etc. as red herrings
func (api *BaseAPI) checkPruneHistory(ctx context.Context, tx kv.Tx, block uint64) error {
	return api.checkPruneField(tx, block, func(p *prune.Mode) prune.BlockAmount { return p.History }, "history is available")
}

// checkPruneBlocks gates on block-body availability rather than state history — use for RPCs
// that read block headers/bodies but do not require state (e.g. GetBlockByNumber, GetTransactionByHash).
func (api *BaseAPI) checkPruneBlocks(ctx context.Context, tx kv.Tx, block uint64) error {
	expiry, mergeHeight, err := api.blocksFollowChainHistoryExpiry(ctx, tx)
	if err != nil {
		return err
	}
	if expiry {
		if mergeHeight == nil || block >= *mergeHeight {
			return nil
		}
		return fmt.Errorf("%w: requested block %d, blocks are available from block %d", state.PrunedError, block, *mergeHeight)
	}
	return api.checkPruneField(tx, block, func(p *prune.Mode) prune.BlockAmount { return p.Blocks }, "blocks are available")
}

// blocksFollowChainHistoryExpiry reports whether block retention is the chain's
// history-expiry policy rather than a window, which Distance.Enabled reads as "not
// pruning" although pre-merge transactions are never downloaded.
func (api *BaseAPI) blocksFollowChainHistoryExpiry(ctx context.Context, tx kv.Tx) (bool, *uint64, error) {
	p, err := api.pruneMode(tx)
	if err != nil || p == nil {
		return false, nil, err
	}
	if p.Blocks != prune.KeepPostMergeBlocksPruneMode {
		return false, nil, nil
	}
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return false, nil, err
	}
	if chainConfig.MergeHeight != nil {
		holds, err := api.holdsPreMergeBlockData(ctx, tx, *chainConfig.MergeHeight)
		if err != nil || holds {
			return false, nil, err
		}
	}
	return true, chainConfig.MergeHeight, nil
}

// holdsPreMergeBlockData reports whether the datadir holds full blocks below the merge
// point, which tells a legacy archive from chain-history expiry where the stored prune
// mode carries the same sentinel for both. Only a readable transaction of an early block
// settles it: expiry keeps pre-merge headers and bodies, and the transaction segment
// spanning the merge point reaches below it.
func (api *BaseAPI) holdsPreMergeBlockData(ctx context.Context, tx kv.Tx, mergeHeight uint64) (bool, error) {
	for {
		if holds, observed, fresh := api._preMergeData.Load(); observed && fresh {
			return holds, nil
		}
		holds, ran, err := api._preMergeData.Produce(ctx, func() (bool, bool, error) {
			return api.probePreMergeBlockData(ctx, tx, mergeHeight)
		})
		switch {
		case err == nil:
			return holds, nil
		case ran || ctx.Err() != nil:
			return false, err
		}
		// The probe reads through the transaction of the caller that ran it, so a failure
		// is about that caller rather than about the datadir: one that only waited asks
		// again on its own.
	}
}

// probePreMergeBlockData answers holdsPreMergeBlockData from what is on disk. It reports
// decided=false where the block data it reads is itself missing: a verdict inferred from
// absent data is not one to remember.
func (api *BaseAPI) probePreMergeBlockData(ctx context.Context, tx kv.Tx, mergeHeight uint64) (holds, decided bool, err error) {
	if mergeHeight == 0 {
		return false, true, nil
	}
	oldest, err := api._blockReader.MinimumBlockAvailable(ctx, tx)
	if err != nil {
		return false, false, err
	}
	// Zero is a snapshot set starting at genesis, one a database holding every block
	// after it; anything higher starts mid-chain, however far below the merge point.
	if oldest > 1 {
		return false, true, nil
	}
	return api.hasEarlyTransaction(ctx, tx, mergeHeight)
}

// hasEarlyTransaction reports whether the datadir is read as holding user transactions
// below limit, and whether the block data it takes to answer was there at all. The last
// pre-merge body carries the cumulative txnum position: no more than the system entries
// below limit means there is no user transaction to be missing. Sampling by halving keeps
// the candidates clear of the transaction segment spanning limit.
func (api *BaseAPI) hasEarlyTransaction(ctx context.Context, tx kv.Tx, limit uint64) (holds, decided bool, err error) {
	last, err := api._blockReader.CanonicalBodyForStorage(ctx, tx, limit-1)
	if err != nil {
		return false, false, err
	}
	if last != nil && earlyUserTxns(last, limit-1) <= 0 {
		return true, true, nil
	}
	for candidate := limit / 2; candidate >= 1; candidate /= 2 {
		body, err := api._blockReader.CanonicalBodyForStorage(ctx, tx, candidate)
		if err != nil {
			return false, false, err
		}
		if body == nil || body.TxCount <= systemTxsPerBlock {
			continue
		}
		return api.readsUserTransaction(ctx, tx, candidate)
	}
	if last == nil {
		// Nothing sampled could show a transaction, and no count proved there are none,
		// so the datadir has not answered: a verdict inferred from what is missing is
		// not one.
		return false, false, nil
	}
	// The count proves a transaction is there and no sampled block held one: a chain
	// sparse enough to pay for a search.
	candidate, outcome, err := api.searchUserTxnBlock(ctx, tx, limit-1, last)
	if err != nil {
		return false, false, err
	}
	switch outcome {
	case earlyTxnFound:
		return api.readsUserTransaction(ctx, tx, candidate)
	case earlyTxnNone:
		// Every block the count leaves room for one in was read and none records a
		// transaction, so the count is inflation alone: there is none to be missing.
		return true, true, nil
	default:
		return false, false, nil
	}
}

// earlyUserTxns reports how many user transactions the chain records up to blockNum. It
// is an upper bound: the database numbers non-canonical bodies from the same sequence, so
// a reorg inflates the total, as does a genesis position past zero. Only the body of a
// block confirms that it holds one of the transactions the count records.
func earlyUserTxns(body *types.BodyForStorage, blockNum uint64) int64 {
	return int64(body.BaseTxnID.U64()) + int64(body.TxCount) - int64(systemTxsPerBlock*(blockNum+1))
}

// earlyTxnSearch is what a search for a pre-merge user transaction observed. Block data
// it could not read leaves the question open, which is neither evidence of an archive
// datadir nor of chain history expiry.
type earlyTxnSearch uint8

const (
	earlyTxnUnread earlyTxnSearch = iota
	earlyTxnNone
	earlyTxnFound
)

// earlyTxnSearchBudget bounds the bodies one search reads; past it the question is left
// open rather than settled on what the search has not seen.
const earlyTxnSearchBudget = 256

// searchUserTxnBlock locates a block up to last whose body records a user transaction,
// which the count the caller read says is there. That count is an upper bound, so the
// block it lands on can record none: what it carried was inflation, and excluding it
// moves the bound past that block so the search resumes above it.
func (api *BaseAPI) searchUserTxnBlock(ctx context.Context, tx kv.Tx, last uint64, lastBody *types.BodyForStorage) (uint64, earlyTxnSearch, error) {
	budget, low, excludedTxns := earlyTxnSearchBudget, uint64(0), int64(0)
	totalTxns := earlyUserTxns(lastBody, last)
	for excludedTxns < totalTxns {
		high, highBody := last, lastBody
		for low < high {
			if budget <= 0 {
				return 0, earlyTxnUnread, nil
			}
			budget--
			middle := low + (high-low)/2
			body, err := api._blockReader.CanonicalBodyForStorage(ctx, tx, middle)
			if err != nil {
				return 0, earlyTxnUnread, err
			}
			if body == nil {
				return 0, earlyTxnUnread, nil
			}
			if earlyUserTxns(body, middle) > excludedTxns {
				high, highBody = middle, body
			} else {
				low = middle + 1
			}
		}
		if highBody.TxCount > systemTxsPerBlock {
			return low, earlyTxnFound, nil
		}
		excludedTxns = earlyUserTxns(highBody, low)
		low++
	}
	return 0, earlyTxnNone, nil
}

func (api *BaseAPI) readsUserTransaction(ctx context.Context, tx kv.Tx, blockNum uint64) (holds, decided bool, err error) {
	txn, ok, err := api._blockReader.TxnByIdxInBlock(ctx, tx, blockNum, 0)
	if err != nil {
		return false, false, err
	}
	return ok && txn != nil, true, nil
}

func (api *BaseAPI) checkPruneField(tx kv.Tx, block uint64, field func(*prune.Mode) prune.BlockAmount, available string) error {
	p, err := api.pruneMode(tx)
	if err != nil {
		return err
	}
	if p == nil {
		return nil
	}
	amount := field(p)
	if !amount.Enabled() {
		return nil
	}
	latest, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return err
	}
	if block < amount.PruneTo(latest) {
		return fmt.Errorf("%w: requested block %d, %s from block %d", state.PrunedError, block, available, amount.PruneTo(latest))
	}
	return nil
}

// checkReceiptsAvailable gates endpoints serving the full receipts of a block. Below
// Byzantium those carry a post state the cache does not store, so the block has to be
// re-executed and reaches only as far back as state history.
func (api *BaseAPI) checkReceiptsAvailable(ctx context.Context, tx kv.Tx, block uint64) error {
	computed, err := api.postStateCalculated(ctx, tx, block)
	if err != nil {
		return err
	}
	if computed {
		return api.checkPruneHistory(ctx, tx, block)
	}
	return api.checkReceiptSourceAvailable(ctx, tx, block)
}

// checkReceiptSourceAvailable gates on where the receipts come from, whatever fields
// the caller reads off them: the receipt cache where it still covers the block, and
// otherwise a re-execution reaching only as far back as state history. Enabling the
// cache says it exists on disk, not how much of it is kept: RCacheDomain is retired on
// its own --prune.receipts.distance window when one is set, and alongside history
// otherwise.
func (api *BaseAPI) checkReceiptSourceAvailable(ctx context.Context, tx kv.Tx, block uint64) error {
	persisted, err := kvcfg.PersistReceipts.Enabled(tx)
	if err != nil {
		return err
	}
	if !persisted || !receipts.PersistedReceiptsServed() {
		return api.checkPruneHistory(ctx, tx, block)
	}
	p, err := api.pruneMode(tx)
	if err != nil || p == nil {
		return err
	}
	switch amount := p.ReceiptsAmount(); {
	case amount == prune.KeepAllReceiptsPruneMode:
		return nil
	case !amount.Enabled():
		return api.checkPruneHistory(ctx, tx, block)
	default:
		err := api.checkPruneField(tx, block, func(*prune.Mode) prune.BlockAmount { return amount }, "receipts are available")
		if err == nil || !errors.Is(err, state.PrunedError) {
			return err
		}
		return api.checkPruneHistory(ctx, tx, block)
	}
}

// postStateCalculated reports whether the receipts of this block carry a post state
// that has to be computed, which is the case below Byzantium. The persistent cache
// does not store that field, so those receipts are always re-executed and reach only
// as far back as state history.
func (api *BaseAPI) postStateCalculated(ctx context.Context, tx kv.Tx, block uint64) (bool, error) {
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return false, err
	}
	commitmentHistory, err := api.commitmentHistoryEnabled(tx)
	if err != nil {
		return false, err
	}
	return receipts.PostStateCalculated(chainConfig, block, commitmentHistory, api._blockReader), nil
}

// checkBlockReceiptsAvailable gates endpoints serving the receipts of one block.
// Reading them needs the block body too: the stored receipt carries no TxHash, so it
// is derived from the block's transaction, and the result is sized by the transaction
// count. The blocks boundary therefore applies on top of receipt availability.
func (api *BaseAPI) checkBlockReceiptsAvailable(ctx context.Context, tx kv.Tx, block uint64) error {
	if err := api.checkPruneBlocks(ctx, tx, block); err != nil {
		return err
	}
	return api.checkReceiptsAvailable(ctx, tx, block)
}

// checkLogsAvailable gates a log query on the data it reads: the receipts of the
// range, which are derived from the block's transactions, plus the log indices when
// the filter searches them. The indices are retired at the history cutoff whatever
// the receipt retention is. Logs are read off a receipt without its post state, so
// this takes the receipt source rather than the full-receipt gate. Every leg is a
// lower bound, so checking the first block of the range covers all of it.
func (api *BaseAPI) checkLogsAvailable(ctx context.Context, tx kv.Tx, block uint64, crit filters.FilterCriteria) error {
	if err := api.checkPruneBlocks(ctx, tx, block); err != nil {
		return err
	}
	if err := api.checkReceiptSourceAvailable(ctx, tx, block); err != nil {
		return err
	}
	if !usesLogIndex(crit) {
		return nil
	}
	return api.checkPruneHistory(ctx, tx, block)
}

// checkBlockHistoryAvailable gates endpoints that re-execute a block: they read its
// transactions from the body and start from the state history preceding it.
func (api *BaseAPI) checkBlockHistoryAvailable(ctx context.Context, tx kv.Tx, block uint64) error {
	if err := api.checkPruneBlocks(ctx, tx, block); err != nil {
		return err
	}
	return api.checkPruneHistory(ctx, tx, block)
}

func (api *BaseAPI) pruneMode(tx kv.Tx) (*prune.Mode, error) {
	p := api._pruneMode.Load()
	if p != nil {
		return p, nil
	}

	mode, err := prune.Get(tx)
	if err != nil {
		return nil, err
	}

	api._pruneMode.Store(&mode)

	return &mode, nil
}

// commitmentHistoryEnabled returns whether --prune.include-commitment-history was set at node
// startup. The flag is written once by checkAndSetCommitmentHistoryFlag and never changed, so
// the result is cached after the first successful read.
// Unlike pruneMode, false is not cached when the DB key is absent: during the brief boot window
// before checkAndSetCommitmentHistoryFlag runs the key may not exist yet, and caching false
// would shadow a subsequent true write. Each request during that window pays one DB lookup.
func (api *BaseAPI) commitmentHistoryEnabled(tx kv.Tx) (bool, error) {
	if p := api._commitmentHistoryEnabled.Load(); p != nil {
		return *p, nil
	}
	enabled, ok, err := rawdb.ReadDBCommitmentHistoryEnabled(tx)
	if err != nil {
		return false, err
	}
	if ok {
		api._commitmentHistoryEnabled.Store(&enabled)
	}
	return enabled, nil
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	*BaseAPI
	ethBackend                  rpchelper.ApiBackend
	txPool                      txpoolproto.TxpoolClient
	mining                      txpoolproto.MiningClient
	gasCache                    gasprice.Cache
	feeHistoryCache             *gasprice.FeeHistoryCache
	db                          kv.TemporalRoDB
	GasCap                      uint64
	FeeCap                      float64
	ReturnDataLimit             int
	AllowUnprotectedTxs         bool
	MaxGetProofRewindBlockCount int
	SubscribeLogsChannelSize    int
	RpcTxSyncDefaultTimeout     time.Duration
	RpcTxSyncMaxTimeout         time.Duration
	logger                      log.Logger
}

// NewEthAPI returns APIImpl instance
func NewEthAPI(base *BaseAPI, db kv.TemporalRoDB, eth rpchelper.ApiBackend, txPool txpoolproto.TxpoolClient, mining txpoolproto.MiningClient, cfg *rpccfg.EthApiConfig, logger log.Logger) *APIImpl {
	gascap := cfg.GasCap
	if gascap == 0 {
		gascap = uint64(math.MaxUint64 / 2)
	}

	return &APIImpl{
		BaseAPI:                     base,
		db:                          db,
		ethBackend:                  eth,
		txPool:                      txPool,
		mining:                      mining,
		gasCache:                    NewGasPriceCache(),
		feeHistoryCache:             gasprice.NewFeeHistoryCache(),
		GasCap:                      gascap,
		FeeCap:                      cfg.FeeCap,
		AllowUnprotectedTxs:         cfg.AllowUnprotectedTxs,
		ReturnDataLimit:             cfg.ReturnDataLimit,
		MaxGetProofRewindBlockCount: cfg.MaxGetProofRewindBlockCount,
		SubscribeLogsChannelSize:    cfg.SubscribeLogsChannelSize,
		RpcTxSyncDefaultTimeout:     cfg.RpcTxSyncDefaultTimeout,
		RpcTxSyncMaxTimeout:         cfg.RpcTxSyncMaxTimeout,
		logger:                      logger,
	}
}

// newRPCPendingTransaction returns a pending transaction that will serialize to the RPC representation
func newRPCPendingTransaction(txn types.Transaction, current *types.Header, config *chain.Config) *ethapi.RPCTransaction {
	var (
		baseFee   *uint256.Int
		blockTime = uint64(0)
	)
	if current != nil {
		baseFee = misc.CalcBaseFee(config, current)
		blockTime = current.Time
	}
	return ethapi.NewRPCTransaction(txn, common.Hash{}, blockTime, 0, 0, baseFee)
}

// newRPCRawTransactionFromBlockIndex returns the bytes of a transaction given a block and a transaction index.
func newRPCRawTransactionFromBlockIndex(b *types.Block, index uint64) (hexutil.Bytes, error) {
	txs := b.Transactions()
	if index >= uint64(len(txs)) {
		return nil, nil
	}
	var buf bytes.Buffer
	err := txs[index].MarshalBinary(&buf)
	return buf.Bytes(), err
}

type GasPriceCache struct {
	latestPrice *uint256.Int
	latestHash  common.Hash
	mtx         sync.Mutex
}

func NewGasPriceCache() *GasPriceCache {
	return &GasPriceCache{
		latestPrice: uint256.NewInt(common.GWei / 1000),
	}
}

func (c *GasPriceCache) GetLatest() (common.Hash, *uint256.Int) {
	var hash common.Hash
	var price *uint256.Int
	c.mtx.Lock()
	hash = c.latestHash
	price = c.latestPrice
	c.mtx.Unlock()
	return hash, price
}

func (c *GasPriceCache) SetLatest(hash common.Hash, price *uint256.Int) {
	c.mtx.Lock()
	c.latestPrice = price
	c.latestHash = hash
	c.mtx.Unlock()
}
