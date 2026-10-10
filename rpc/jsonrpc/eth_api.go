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
	"unsafe"

	"github.com/erigontech/erigon/common/dbg"

	"github.com/c2h5oh/datasize"
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
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	// Block related (proposed file: ./eth_blocks.go)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (*ethapi.RPCBlock, error)
	GetBlockByHash(ctx context.Context, hash rpc.BlockNumberOrHash, fullTx bool) (*ethapi.RPCBlock, error)
	GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*ethapi.RPCHeader, error)
	GetHeaderByHash(ctx context.Context, hash common.Hash) (*ethapi.RPCHeader, error)
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
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (*ethutils.RPCReceipt, error)
	GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.Logs, error)
	GetBlockReceipts(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) (ethutils.RPCReceipts, error)

	// Block access list related (see ./eth_block_access_list.go)
	GetBlockAccessList(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) ([]*ethapi.RPCAccountAccess, error)

	// Uncle related (see ./eth_uncles.go)
	GetUncleByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (*ethapi.RPCBlock, error)
	GetUncleByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint) (*ethapi.RPCBlock, error)
	GetUncleCountByBlockNumber(ctx context.Context, number rpc.BlockNumber) (*hexutil.Uint, error)
	GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (*hexutil.Uint, error)

	// Filter related (see ./eth_filters.go)
	NewPendingTransactionFilter(_ context.Context) (string, error)
	NewBlockFilter(_ context.Context) (string, error)
	NewFilter(_ context.Context, crit filters.FilterCriteria) (string, error)
	UninstallFilter(_ context.Context, index string) (bool, error)
	GetFilterChanges(_ context.Context, index string) ([]any, error)
	GetFilterLogs(ctx context.Context, index string) (types.Logs, error)
	Logs(ctx context.Context, crit filters.FilterCriteria) (*rpc.Subscription, error)

	// Account related (see ./eth_accounts.go)
	Accounts(ctx context.Context) ([]common.Address, error)
	GetBalance(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (*hexutil.U256, error)
	GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (*hexutil.Uint64, error)
	GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash *rpc.BlockNumberOrHash) (common.Hash, error)
	GetStorageValues(ctx context.Context, requests map[common.Address][]common.Hash, blockNrOrHash *rpc.BlockNumberOrHash) (StorageValues, error)
	GetCode(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (hexutil.Bytes, error)

	// System related (see ./eth_system.go)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	Syncing(ctx context.Context) (any, error)
	ChainId(ctx context.Context) (hexutil.Uint64, error) /* called eth_protocolVersion elsewhere */
	ProtocolVersion(_ context.Context) (hexutil.Uint, error)
	GasPrice(_ context.Context) (*hexutil.U256, error)
	MaxPriorityFeePerGas(ctx context.Context) (*hexutil.U256, error)
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
	SendRawTransactionSync(ctx context.Context, encodedTx hexutil.Bytes, timeoutMs *uint64) (*ethutils.RPCReceipt, error)
	SendTransaction(_ context.Context, txObject any) (common.Hash, error)
	Sign(ctx context.Context, _ common.Address, _ hexutil.Bytes) (hexutil.Bytes, error)
	SignTransaction(_ context.Context, txObject any) (common.Hash, error)
	FillTransaction(ctx context.Context, args ethapi.CallArgs) (*ethapi.SignTransactionResult, error)
	GetProof(ctx context.Context, address common.Address, storageKeys []hexutil.Bytes, blockNr *rpc.BlockNumberOrHash) (*accounts.AccProofResult, error)
	CreateAccessList(ctx context.Context, args ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi.StateOverrides, optimizeGas *bool) (*accessListResult, error)

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
	blocksLRU  *cache.HashByteLRU[*types.Block]
	// headersLRU holds headers decoded for header-only lookups.
	headersLRU *cache.HashByteLRU[*types.Header]

	filters                   *rpchelper.Filters
	_chainConfig              atomic.Pointer[chain.Config]
	_genesis                  atomic.Pointer[types.Block]
	_pruneMode                atomic.Pointer[prune.Mode]
	_commitmentHistoryEnabled atomic.Pointer[bool]
	// _preMergeData is kept for a TTL rather than settled once: it reads live snapshot
	// availability, which widens as segments arrive.
	_preMergeData concurrent.CachedValue[preMergeBlockData]
	// _preMergeUnsettled is what a probe answered without settling the question. It
	// stands in for another walk over the same absent block data, for a TTL of its own:
	// shorter than the verdict's, since the data it waits for can arrive at any time.
	_preMergeUnsettled    atomic.Pointer[unsettledProbe]
	_preMergeUnsettledTTL time.Duration
	_historyPruneFloor    pruneFloorCache[historyPruneFloors]
	_blocksPruneFloor     pruneFloorCache[uint64]

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

// BlockCacheBytes bounds the decoded blocks the RPC layer keeps. A mainnet block costs ~331KB of
// heap, so this holds ~1600 of them, about 5 hours of chain.
var BlockCacheBytes = dbg.EnvDataSize("RPC_BLOCK_CACHE", 512*datasize.MB)

// HeaderCacheBytes bounds the decoded headers the RPC layer keeps for header-only lookups.
var HeaderCacheBytes = dbg.EnvDataSize("RPC_HEADER_CACHE", 2*datasize.MB)

// blockHeapSize approximates a decoded block's heap: its encoding plus the header and one
// transaction struct per transaction, which hold inline integers and hash and sender caches.
func blockHeapSize(b *types.Block) int64 {
	return int64(b.EncodingSize()) + int64(unsafe.Sizeof(types.Header{})) + int64(len(b.Transactions()))*int64(unsafe.Sizeof(types.DynamicFeeTransaction{}))
}

// headerHeapSize approximates a decoded header's heap: its encoding plus the struct.
func headerHeapSize(h *types.Header) int64 {
	return int64(h.EncodingSize()) + int64(unsafe.Sizeof(types.Header{}))
}

func NewBaseApi(f *rpchelper.Filters, stateCache kvcache.Cache, blockReader dbservices.FullBlockReader, engine rules.Engine, conf *rpccfg.BaseApiConfig) *BaseAPI {
	if conf == nil {
		conf = &rpccfg.BaseApiConfig{}
	}
	blocksLRU := cache.NewHashByteLRU(BlockCacheBytes, blockHeapSize)
	headersLRU := cache.NewHashByteLRU(HeaderCacheBytes, headerHeapSize)

	evmCallTimeout := conf.EvmCallTimeout
	if evmCallTimeout == 0 {
		evmCallTimeout = rpccfg.DefaultEvmCallTimeout
	}

	api := &BaseAPI{
		filters:           f,
		stateCache:        stateCache,
		blocksLRU:         blocksLRU,
		headersLRU:        headersLRU,
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
	api._preMergeUnsettledTTL = defaultUnsettledPreMergeTTL
	return api
}

func (api *BaseAPI) tryChainConfig() (*chain.Config, bool) {
	cc := api._chainConfig.Load()
	return cc, cc != nil
}

func (api *BaseAPI) chainConfig(ctx context.Context, tx kv.Tx) (*chain.Config, error) {
	cfg, _, err := api.chainConfigWithGenesis(ctx, tx)
	return cfg, err
}

func (api *BaseAPI) tryChainConfigWithGenesis() (*chain.Config, *types.Block, bool) {
	cc, genesisBlock := api._chainConfig.Load(), api._genesis.Load()
	return cc, genesisBlock, cc != nil && genesisBlock != nil
}

func (api *BaseAPI) chainConfigWithGenesis(ctx context.Context, tx kv.Tx) (*chain.Config, *types.Block, error) {
	cc, genesisBlock, ok := api.tryChainConfigWithGenesis()
	if ok {
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
	blockNumber, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader)
	if _, ok := errors.AsType[rpc.BlockNotFoundErr](err); !ok {
		return blockNumber, err
	}

	overlayBlockNumber, _, _, overlayErr := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, api.filters.WithOverlay(tx), api._blockReader)
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
	if api.headersLRU != nil {
		if header, ok := api.headersLRU.Get(hash); ok && header != nil {
			return header, nil
		}
	}
	header, err := api._blockReader.Header(ctx, tx, hash, number)
	if err != nil {
		return nil, err
	}
	if header == nil { // don't save nil's to cache
		return nil, nil
	}
	if api.headersLRU != nil {
		api.headersLRU.Add(hash, header)
	}
	return header, nil
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

// canonicalHeaderByNumberOrHash resolves the selector and header through tx.
// It never selects an overlay, so callers can keep dependent reads on one view.
func (api *BaseAPI) canonicalHeaderByNumberOrHash(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash) (*types.Header, bool, error) {
	if number, ok := blockNrOrHash.Number(); ok && number == rpc.PendingBlockNumber {
		return nil, false, nil
	}
	blockNum, hash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader)
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
	n, h, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(number), overlayTx, api._blockReader)
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

const defaultUnsettledPreMergeTTL = time.Second

// systemTxsPerBlock is the pair of system entries every block carries in the txnum
// sequence, which a stored TxCount includes.
const systemTxsPerBlock = 2

// checkPruneHistory requires history from the block's first system transaction.
func (api *BaseAPI) checkPruneHistory(ctx context.Context, tx kv.Tx, block uint64) error {
	return api.checkHistoryFloor(ctx, tx, block, func(f historyPruneFloors) uint64 { return f.wholeBlock })
}

// checkPruneState requires the state after the block, read at Min(block+1).
func (api *BaseAPI) checkPruneState(ctx context.Context, tx kv.Tx, block uint64) error {
	return api.checkHistoryFloor(ctx, tx, block, func(f historyPruneFloors) uint64 { return f.postState })
}

// checkPruneStateAfterSystemTx matches state readers opened at Min(block+1)+1.
func (api *BaseAPI) checkPruneStateAfterSystemTx(ctx context.Context, tx kv.Tx, block uint64) error {
	return api.checkHistoryFloor(ctx, tx, block, func(f historyPruneFloors) uint64 { return f.stateAfterSystemTx })
}

func (api *BaseAPI) checkHistoryFloor(ctx context.Context, tx kv.Tx, block uint64, pick func(historyPruneFloors) uint64) error {
	return api.checkPruneField(tx, block, func(p *prune.Mode) prune.BlockAmount { return p.History }, "history is available", func(head uint64) (uint64, error) {
		floors, err := api.historyStartBlocks(ctx, tx, head)
		return pick(floors), err
	})
}

// checkPruneTransactionHistory requires the first user transaction's pre-state,
// at Min(block)+1, after the block's initial system transaction.
func (api *BaseAPI) checkPruneTransactionHistory(ctx context.Context, tx kv.Tx, block uint64) error {
	return api.checkPruneTransactionHistoryAtIndex(ctx, tx, block, 0)
}

func (api *BaseAPI) checkPruneTransactionHistoryAtIndex(ctx context.Context, tx kv.Tx, block, txIndex uint64) error {
	for {
		var indexedKey pruneFloorCacheKey
		var hasIndexedKey bool
		err := api.checkPruneField(tx, block, func(p *prune.Mode) prune.BlockAmount { return p.History }, "history is available", func(head uint64) (uint64, error) {
			floors, err := api.historyStartBlocks(ctx, tx, head)
			if err != nil || txIndex == 0 || block >= floors.replay {
				return floors.replay, err
			}
			indexedKey, hasIndexedKey = historyFloorCacheKey(tx.(kv.TemporalTx), head)
			// The block-level replay floor may reject an indexed read whose pre-state
			// survives. Check its exact txNum only when that floor would reject it.
			c, err := tx.Cursor(kv.MaxTxNum)
			if err != nil {
				return 0, err
			}
			defer c.Close()
			minTxNum, err := api._txNumReader.MinWithCursor(ctx, tx, c, block)
			if err != nil {
				return 0, err
			}
			maxTxNum, err := api._txNumReader.MaxWithCursor(ctx, tx, c, block)
			if err != nil {
				return 0, err
			}
			// Only a position in this block can bypass its replay floor. Max names the
			// final system transaction, whose pre-state follows all user transactions.
			if maxTxNum > minTxNum && txIndex < maxTxNum-minTxNum && minTxNum+txIndex+1 >= floors.startTxNum {
				return block, nil
			}
			return 0, fmt.Errorf("%w: requested block %d at transaction index %d, history is available from txNum %d", state.ErrPruned, block, txIndex, floors.startTxNum)
		})
		// Closing a remote cursor can also renew its view. Check after the deferred
		// Close completes, so the history floor and indexed bounds describe one view.
		if hasIndexedKey {
			if current, ok := historyFloorCacheKey(tx.(kv.TemporalTx), indexedKey.head); !ok || current != indexedKey {
				continue
			}
		}
		return err
	}
}

// checkPruneBlocks gates RPCs that need retained block transactions,
// independently of state history.
func (api *BaseAPI) checkPruneBlocks(ctx context.Context, tx kv.Tx, block uint64) error {
	return api.checkPruneBlocksRange(ctx, tx, block, block)
}

func (api *BaseAPI) checkPruneBlocksRange(ctx context.Context, tx kv.Tx, from, to uint64) error {
	expiry, oldest, err := api.blocksFollowChainHistoryExpiry(ctx, tx)
	if err != nil {
		return err
	}
	if expiry && oldest != nil && from < *oldest {
		return fmt.Errorf("%w: requested block %d, blocks are available from block %d", state.ErrPruned, from, *oldest)
	}
	return api.checkPruneField(tx, from, func(p *prune.Mode) prune.BlockAmount { return p.Blocks }, "blocks are available", func(head uint64) (uint64, error) {
		// Separately kept genesis cannot fill a gap in a range. Only a genesis-only
		// read may bypass the physical floor; retention and chain-history expiry still apply.
		if from == 0 && to == 0 {
			return 0, nil
		}
		return api.minimumBlockAvailable(ctx, tx, head)
	})
}

// blocksAvailableFrom combines the configured policy, any chain-history expiry,
// and the physical block-data floor into the effective block boundary.
func (api *BaseAPI) blocksAvailableFrom(ctx context.Context, tx kv.Tx, head uint64) (uint64, error) {
	p, err := api.pruneMode(tx)
	if err != nil {
		return 0, err
	}
	floor := uint64(0)
	if p != nil {
		floor = p.Blocks.PruneTo(head)
	}
	expiry, expiryFrom, err := api.blocksFollowChainHistoryExpiry(ctx, tx)
	if err != nil {
		return 0, err
	}
	if expiry && expiryFrom != nil {
		floor = max(floor, *expiryFrom)
	}
	onDiskFloor, err := api.minimumBlockAvailable(ctx, tx, head)
	if err != nil {
		return 0, err
	}
	floor = max(floor, onDiskFloor)
	return floor, nil
}

type historyPruneFloors struct {
	startTxNum         uint64
	postState          uint64
	stateAfterSystemTx uint64
	wholeBlock         uint64
	replay             uint64
}

var errHistoryViewChanged = errors.New("history view changed during availability lookup")

func (api *BaseAPI) historyStartBlocks(ctx context.Context, tx kv.Tx, head uint64) (historyPruneFloors, error) {
	ttx, ok := tx.(kv.TemporalTx)
	if !ok {
		return historyPruneFloors{}, fmt.Errorf("history availability requires a temporal transaction, got %T", tx)
	}
	for {
		key, ok := historyFloorCacheKey(ttx, head)
		if !ok {
			// Older remote servers and custom views may not identify their pinned files.
			return api.readHistoryStartBlocks(ctx, ttx, head)
		}
		floors, err := api._historyPruneFloor.getForKey(ctx, key, func() (historyPruneFloors, error) {
			floors, err := api.readHistoryStartBlocks(ctx, ttx, head)
			if err != nil {
				return historyPruneFloors{}, err
			}
			// Cursor reads can renew a remote transaction. Retry instead of caching
			// a floor read across two different views.
			if current, ok := historyFloorCacheKey(ttx, head); !ok || current != key {
				return historyPruneFloors{}, errHistoryViewChanged
			}
			return floors, nil
		})
		if !errors.Is(err, errHistoryViewChanged) {
			return floors, err
		}
	}
}

func historyFloorCacheKey(tx kv.TemporalTx, head uint64) (pruneFloorCacheKey, bool) {
	files, ok := tx.Debug().(interface{ HistoryFilesGeneration() uint64 })
	if !ok {
		return pruneFloorCacheKey{}, false
	}
	return pruneFloorCacheKey{
		head:                   head,
		dbViewID:               tx.ViewID(),
		historyFilesGeneration: files.HistoryFilesGeneration(),
		blockFilesGeneration:   blockFilesGeneration(tx),
	}, true
}

func (api *BaseAPI) readHistoryStartBlocks(ctx context.Context, tx kv.TemporalTx, head uint64) (historyPruneFloors, error) {
	startTxNum, err := state.StateHistoryStartTxNum(tx)
	if err != nil {
		return historyPruneFloors{}, err
	}
	return api.historyStartBlocksFromTxNum(ctx, tx, head, startTxNum)
}

func (api *BaseAPI) readCommitmentHistoryStartBlocks(ctx context.Context, tx kv.TemporalTx, head uint64) (historyPruneFloors, error) {
	for {
		key, hasKey := historyFloorCacheKey(tx, head)
		startTxNum, err := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
		if err != nil {
			return historyPruneFloors{}, err
		}
		floors, err := api.historyStartBlocksFromTxNum(ctx, tx, head, startTxNum)
		if err != nil || !hasKey {
			return floors, err
		}
		// Cursor reads in the conversion can renew a remote transaction.
		// Retry the whole lookup if it spans different views.
		if current, ok := historyFloorCacheKey(tx, head); ok && current == key {
			return floors, nil
		}
	}
}

func (api *BaseAPI) historyStartBlocksFromTxNum(ctx context.Context, tx kv.Tx, head, startTxNum uint64) (historyPruneFloors, error) {
	if startTxNum == 0 {
		return historyPruneFloors{}, nil
	}
	containingBlock, ok, err := api._txNumReader.FindBlockNum(ctx, tx, startTxNum)
	if err != nil {
		return historyPruneFloors{}, err
	}
	if !ok {
		// No historical block can be proven available; the current state remains readable.
		return historyPruneFloors{
			startTxNum:         startTxNum,
			postState:          head,
			stateAfterSystemTx: head,
			wholeBlock:         head,
			replay:             head,
		}, nil
	}
	blockStartTxNum, err := api._txNumReader.Min(ctx, tx, containingBlock)
	if err != nil {
		return historyPruneFloors{}, err
	}
	// State queries read at the next block's start, while whole-block history
	// reads need this block's start. Transaction replay skips the initial system tx.
	floors := historyPruneFloors{
		startTxNum:         startTxNum,
		postState:          containingBlock,
		stateAfterSystemTx: containingBlock,
		wholeBlock:         containingBlock,
		replay:             containingBlock,
	}
	if startTxNum > blockStartTxNum {
		floors.wholeBlock = min(containingBlock+1, head)
	} else if containingBlock > 0 {
		floors.postState = containingBlock - 1
	}
	if startTxNum > blockStartTxNum+1 {
		floors.replay = min(containingBlock+1, head)
	} else if containingBlock > 0 {
		floors.stateAfterSystemTx = containingBlock - 1
	}
	return floors, nil
}

func (api *BaseAPI) minimumBlockAvailable(ctx context.Context, tx kv.Tx, head uint64) (uint64, error) {
	key := pruneFloorCacheKey{head: head, dbViewID: tx.ViewID(), blockFilesGeneration: blockFilesGeneration(tx)}
	return api._blocksPruneFloor.getForKey(ctx, key, func() (uint64, error) {
		return api._blockReader.MinimumBlockAvailable(ctx, tx)
	})
}

// blockFilesGeneration returns zero when tx has no local pinned snapshot view;
// those callers rely on the cache TTL to refresh their physical floor.
func blockFilesGeneration(tx kv.Tx) uint64 {
	provider, ok := tx.(freezeblocks.HasBlockFilesRoTx)
	if !ok {
		return 0
	}
	view := provider.BlockFilesRoTx()
	if view == nil {
		return 0
	}
	return view.Generation()
}

// blocksFollowChainHistoryExpiry reports whether block retention is the chain's
// history-expiry policy rather than a window, which Distance.Enabled reads as "not
// pruning" although pre-merge transactions are never downloaded, and the block the
// datadir then serves from.
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
		data, err := api.holdsPreMergeBlockData(ctx, tx, *chainConfig.MergeHeight)
		if err != nil || data.holds {
			return false, nil, err
		}
		return true, &data.oldest, nil
	}
	return true, chainConfig.MergeHeight, nil
}

// preMergeBlockData is what the datadir answers about pre-merge blocks: whether it holds
// any, and the lowest block it serves in full when it does not.
type preMergeBlockData struct {
	holds  bool
	oldest uint64
}

// unsettledProbe is what a probe answered without settling the question, and when.
type unsettledProbe struct {
	data preMergeBlockData
	at   time.Time
}

// holdsPreMergeBlockData reports whether the datadir holds full blocks below the merge
// point, which tells a legacy archive from chain-history expiry where the stored prune
// mode carries the same sentinel for both. Only a readable transaction of an early block
// settles it: expiry keeps pre-merge headers and bodies, and the transaction segment
// spanning the merge point reaches below it.
func (api *BaseAPI) holdsPreMergeBlockData(ctx context.Context, tx kv.Tx, mergeHeight uint64) (preMergeBlockData, error) {
	for {
		if data, observed, fresh := api._preMergeData.Load(); observed && fresh {
			return data, nil
		}
		if unsettled := api._preMergeUnsettled.Load(); unsettled != nil && time.Since(unsettled.at) < api._preMergeUnsettledTTL {
			return unsettled.data, nil
		}
		data, ran, err := api._preMergeData.Produce(ctx, func() (preMergeBlockData, bool, error) {
			data, decided, err := api.probePreMergeBlockData(ctx, tx, mergeHeight)
			if err == nil && !decided {
				api._preMergeUnsettled.Store(&unsettledProbe{data: data, at: time.Now()})
			}
			return data, decided, err
		})
		switch {
		case err == nil:
			return data, nil
		case ran || ctx.Err() != nil:
			return preMergeBlockData{}, err
		}
		// The probe reads through the transaction of the caller that ran it, so a failure
		// is about that caller rather than about the datadir: one that only waited asks
		// again on its own.
	}
}

// probePreMergeBlockData answers holdsPreMergeBlockData from what is on disk. It reports
// decided=false where the block data it reads is itself missing: a verdict inferred from
// absent data is not one to remember.
func (api *BaseAPI) probePreMergeBlockData(ctx context.Context, tx kv.Tx, mergeHeight uint64) (data preMergeBlockData, decided bool, err error) {
	if mergeHeight == 0 {
		return preMergeBlockData{}, true, nil
	}
	oldest, err := api._blockReader.MinimumBlockAvailable(ctx, tx)
	if err != nil {
		return preMergeBlockData{}, false, err
	}
	// A floor above block 1 proves that the retained range starts mid-chain.
	// Otherwise, probe transactions: stored bodies do not prove their transactions are readable.
	if oldest > 1 {
		return preMergeBlockData{oldest: oldest}, true, nil
	}
	holds, decided, err := api.hasEarlyTransaction(ctx, tx, mergeHeight)
	return preMergeBlockData{holds: holds, oldest: mergeHeight}, decided, err
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
	var bounds []earlyTxnBound
	if last != nil {
		txns := earlyUserTxns(last, limit-1)
		if txns <= 0 {
			return true, true, nil
		}
		bounds = append(bounds, earlyTxnBound{num: limit - 1, body: last, txns: txns})
	}
	low := uint64(0)
	for candidate := limit / 2; candidate >= 1; candidate /= 2 {
		body, err := api._blockReader.CanonicalBodyForStorage(ctx, tx, candidate)
		if err != nil {
			return false, false, err
		}
		if body == nil {
			continue
		}
		if body.TxCount > systemTxsPerBlock {
			return api.readsUserTransaction(ctx, tx, candidate)
		}
		txns := earlyUserTxns(body, candidate)
		if txns <= 0 {
			low = candidate + 1
			break
		}
		bounds = append(bounds, earlyTxnBound{num: candidate, body: body, txns: txns})
	}
	if last == nil {
		// Nothing sampled could show a transaction, and no count proved there are none,
		// so the datadir has not answered: a verdict inferred from what is missing is
		// not one.
		return false, false, nil
	}
	// The count proves a transaction is there and no sampled block held one: a chain
	// sparse enough to pay for a search.
	candidate, outcome, err := api.searchUserTxnBlock(ctx, tx, low, bounds)
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
	case earlyTxnSpent:
		// What spent the budget is the chain shape the search read, so a second walk
		// reaches the same place: worth remembering, unlike a verdict the datadir was
		// too empty to give.
		return false, true, nil
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
	earlyTxnSpent
	earlyTxnNone
	earlyTxnFound
)

// earlyTxnSearchBudget bounds the bodies one search reads; past it the search stops where
// it is rather than walking the whole range.
const earlyTxnSearchBudget = 256

// earlyTxnBound is a block whose cumulative count ran ahead of what the search had
// excluded when it was read. Counts only grow with the block number, so it stays an
// upper bound until the search excludes as many transactions as it records.
type earlyTxnBound struct {
	num  uint64
	body *types.BodyForStorage
	txns int64
}

// searchUserTxnBlock locates a block at or above low whose body records a user
// transaction, which the count in the outermost of bounds says is there. That count is an
// upper bound, so the block it lands on can record none: what it carried was inflation,
// and excluding it moves the bound past that block so the search resumes above it.
func (api *BaseAPI) searchUserTxnBlock(ctx context.Context, tx kv.Tx, low uint64, bounds []earlyTxnBound) (uint64, earlyTxnSearch, error) {
	budget, excludedTxns := earlyTxnSearchBudget, int64(0)
	totalTxns := bounds[0].txns
	for excludedTxns < totalTxns {
		for len(bounds) > 1 && bounds[len(bounds)-1].txns <= excludedTxns {
			bounds = bounds[:len(bounds)-1]
		}
		high := bounds[len(bounds)-1]
		if high.txns <= excludedTxns {
			// The bound has to be one the search has not excluded yet, which pruning
			// above keeps true: an excluded one is walked again for as long as the
			// read transaction is held open.
			return 0, earlyTxnUnread, nil
		}
		for low < high.num {
			if budget <= 0 {
				return 0, earlyTxnSpent, nil
			}
			budget--
			middle := low + (high.num-low)/2
			body, err := api._blockReader.CanonicalBodyForStorage(ctx, tx, middle)
			if err != nil {
				return 0, earlyTxnUnread, err
			}
			if body == nil {
				return 0, earlyTxnUnread, nil
			}
			if txns := earlyUserTxns(body, middle); txns > excludedTxns {
				high = earlyTxnBound{num: middle, body: body, txns: txns}
				bounds = append(bounds, high)
			} else {
				low = middle + 1
			}
		}
		if high.body.TxCount > systemTxsPerBlock {
			return low, earlyTxnFound, nil
		}
		excludedTxns = high.txns
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

func (api *BaseAPI) checkPruneField(tx kv.Tx, block uint64, field func(*prune.Mode) prune.BlockAmount, available string, onDiskFloor func(uint64) (uint64, error)) error {
	p, err := api.pruneMode(tx)
	if err != nil {
		return err
	}
	if p == nil {
		return nil
	}
	amount := field(p)
	latest, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return err
	}
	floor := amount.PruneTo(latest)
	if block >= floor && block < latest && onDiskFloor != nil {
		actual, err := onDiskFloor(latest)
		if err != nil {
			return err
		}
		floor = max(floor, actual)
	}
	if block < floor {
		return fmt.Errorf("%w: requested block %d, %s from block %d", state.ErrPruned, block, available, floor)
	}
	return nil
}

// Pre-Byzantium post-state roots are not stored in the receipt domain. Rebuilding
// them needs state history and, when enabled, commitment history from the initial
// system transaction, not just the first user transaction.
func (api *BaseAPI) checkReceiptsAvailable(ctx context.Context, tx kv.Tx, block uint64) error {
	return api.checkReceiptAvailableAtIndex(ctx, tx, block, 0)
}

func (api *BaseAPI) checkReceiptAvailableAtIndex(ctx context.Context, tx kv.Tx, block, txIndex uint64) error {
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return err
	}
	commitmentHistory, err := api.commitmentHistoryEnabled(tx)
	if err != nil {
		return err
	}
	if !receipts.PostStateCalculated(chainConfig, block, commitmentHistory, api._blockReader) {
		return api.checkReceiptSourceAvailable(ctx, tx, block, txIndex)
	}
	return api.checkPruneField(tx, block, func(p *prune.Mode) prune.BlockAmount { return p.History }, "history is available", func(head uint64) (uint64, error) {
		for {
			floors, err := api.historyStartBlocks(ctx, tx, head)
			if err != nil || !commitmentHistory {
				return floors.wholeBlock, err
			}
			ttx := tx.(kv.TemporalTx)
			key, hasKey := historyFloorCacheKey(ttx, head)
			commitmentFloors, err := api.readCommitmentHistoryStartBlocks(ctx, ttx, head)
			if err != nil {
				return 0, err
			}
			// The commitment lookup can renew the remote view after the state
			// floor was read. Retry both floors together if that happens.
			if hasKey {
				if current, ok := historyFloorCacheKey(ttx, head); !ok || current != key {
					continue
				}
			}
			return max(floors.wholeBlock, commitmentFloors.wholeBlock), nil
		}
	})
}

// checkReceiptSourceAvailable gates on where the receipts come from, whatever fields
// the caller reads off them: the receipt cache where it still covers the block, and
// otherwise a re-execution reaching only as far back as state history. Enabling the
// cache says it exists on disk, not how much of it is kept: RCacheDomain is retired on
// its own --prune.receipts.distance window when one is set, and alongside history
// otherwise.
func (api *BaseAPI) checkReceiptSourceAvailable(ctx context.Context, tx kv.Tx, block, txIndex uint64) error {
	persisted, err := kvcfg.PersistReceipts.Enabled(tx)
	if err != nil {
		return err
	}
	if persisted && receipts.PersistedReceiptsServed() {
		p, err := api.pruneMode(tx)
		if err != nil || p == nil {
			return err
		}
		amount := p.ReceiptsAmount()
		if amount == prune.KeepAllReceiptsPruneMode {
			return nil
		}
		if amount.Enabled() {
			err := api.checkPruneField(tx, block, func(*prune.Mode) prune.BlockAmount { return amount }, "receipts are available", nil)
			if !errors.Is(err, state.ErrPruned) {
				return err
			}
		}
	}
	return api.checkPruneTransactionHistoryAtIndex(ctx, tx, block, txIndex)
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
// this takes the receipt source rather than the full-receipt gate.
func (api *BaseAPI) checkLogsAvailable(ctx context.Context, tx kv.Tx, from, to uint64, crit filters.FilterCriteria) error {
	if err := api.checkPruneBlocksRange(ctx, tx, from, to); err != nil {
		return err
	}
	if usesLogIndex(crit) {
		// Replay history also provides receipts when the receipt cache cannot.
		// Log queries skip the initial system entry and only return user-transaction logs.
		return api.checkPruneTransactionHistory(ctx, tx, from)
	}
	return api.checkReceiptSourceAvailable(ctx, tx, from, 0)
}

// checkBlockHistoryAvailable gates transaction replay, which needs block
// transactions and state history from the first user transaction.
func (api *BaseAPI) checkBlockHistoryAvailable(ctx context.Context, tx kv.Tx, block uint64) error {
	return api.checkBlockHistoryRangeAvailable(ctx, tx, block, block)
}

func (api *BaseAPI) checkBlockHistoryRangeAvailable(ctx context.Context, tx kv.Tx, from, to uint64) error {
	if err := api.checkPruneBlocksRange(ctx, tx, from, to); err != nil {
		return err
	}
	return api.checkPruneTransactionHistory(ctx, tx, from)
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
func newRPCPendingTransaction(txn types.Transaction) *ethapi.RPCTransaction {
	return ethapi.NewRPCTransaction(txn, common.Hash{}, 0, 0, 0, nil)
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
