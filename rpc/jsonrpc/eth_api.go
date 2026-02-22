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
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
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
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
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
	GetFilterLogs(_ context.Context, index string) ([]*types.Log, error)
	Logs(ctx context.Context, crit filters.FilterCriteria) (*rpc.Subscription, error)

	// Account related (see ./eth_accounts.go)
	Accounts(ctx context.Context) ([]common.Address, error)
	GetBalance(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error)
	GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error)
	GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error)
	GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)

	// System related (see ./eth_system.go)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	Syncing(ctx context.Context) (any, error)
	ChainId(ctx context.Context) (hexutil.Uint64, error) /* called eth_protocolVersion elsewhere */
	ProtocolVersion(_ context.Context) (hexutil.Uint, error)
	GasPrice(_ context.Context) (*hexutil.Big, error)
	Config(ctx context.Context, timeArg *hexutil.Uint64) (*EthConfigResp, error)

	// Sending related (see ./eth_call.go)
	Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi.StateOverrides, blockOverrides *ethapi.BlockOverrides) (hexutil.Bytes, error)
	EstimateGas(ctx context.Context, argsOrNil *ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi.StateOverrides, blockOverrides *ethapi.BlockOverrides) (hexutil.Uint64, error)
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
	SendRawTransactionSync(ctx context.Context, encodedTx hexutil.Bytes, timeoutMs *uint64) (map[string]any, error)
	SendTransaction(_ context.Context, txObject any) (common.Hash, error)
	Sign(ctx context.Context, _ common.Address, _ hexutil.Bytes) (hexutil.Bytes, error)
	SignTransaction(_ context.Context, txObject any) (common.Hash, error)
	GetProof(ctx context.Context, address common.Address, storageKeys []hexutil.Bytes, blockNr rpc.BlockNumberOrHash) (*accounts.AccProofResult, error)
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

	filters      *rpchelper.Filters
	_chainConfig atomic.Pointer[chain.Config]
	_genesis     atomic.Pointer[types.Block]
	_pruneMode   atomic.Pointer[prune.Mode]

	_blockReader services.FullBlockReader
	_txNumReader rawdbv3.TxNumsReader
	_txnReader   services.TxnReader
	_engine      rules.EngineReader

	bridgeReader bridgeReader

	evmCallTimeout      time.Duration
	rangeLimit          int
	dirs                datadir.Dirs
	receiptsGenerator   *receipts.Generator
	borReceiptGenerator *receipts.BorGenerator
}

func NewBaseApi(f *rpchelper.Filters, stateCache kvcache.Cache, blockReader services.FullBlockReader, singleNodeMode bool, evmCallTimeout time.Duration, engine rules.EngineReader, dirs datadir.Dirs, bridgeReader bridgeReader, rangeLimit int) *BaseAPI {
	var (
		blocksLRUSize = 128 // ~32Mb
	)
	// if RPCDaemon deployed as independent process: increase cache sizes
	if !singleNodeMode {
		blocksLRUSize *= 5
	}
	blocksLRU, err := lru.New[common.Hash, *types.Block](blocksLRUSize)
	if err != nil {
		panic(err)
	}

	return &BaseAPI{
		filters:             f,
		stateCache:          stateCache,
		blocksLRU:           blocksLRU,
		_blockReader:        blockReader,
		_txnReader:          blockReader,
		_txNumReader:        blockReader.TxnumReader(),
		evmCallTimeout:      evmCallTimeout,
		_engine:             engine,
		receiptsGenerator:   receipts.NewGenerator(dirs, blockReader, engine, stateCache, evmCallTimeout),
		borReceiptGenerator: receipts.NewBorGenerator(blockReader, engine, stateCache),
		dirs:                dirs,
		bridgeReader:        bridgeReader,
		rangeLimit:          rangeLimit,
	}
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

	genesisBlock, err := api.blockByNumberWithSenders(ctx, tx, 0)
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
	return api.filters.LastPendingBlock()
}
func (api *BaseAPI) engine() rules.EngineReader {
	return api._engine
}

func (api *BaseAPI) txnLookup(ctx context.Context, tx kv.Tx, txnHash common.Hash) (blockNum uint64, txNum uint64, ok bool, err error) {
	return api._txnReader.TxnLookup(ctx, tx, txnHash)
}

func (api *BaseAPI) blockByNumberWithSenders(ctx context.Context, tx kv.Tx, number uint64) (*types.Block, error) {
	blockNumber, hash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(number)), tx, api._blockReader, api.filters)
	if err != nil {
		if errors.As(err, &rpc.BlockNotFoundErr{}) {
			return nil, nil
		}
		return nil, err
	}
	return api.blockWithSenders(ctx, tx, hash, blockNumber)
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

func (api *BaseAPI) headerNumberByHash(ctx context.Context, tx kv.Tx, hash common.Hash) (uint64, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it.Header().Number.Uint64(), nil
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
			return it.Header(), isLatest, nil
		}
	}

	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, false, err
	}
	// header can be nil
	return header, isLatest, nil
}

func (api *BaseAPI) headerByNumber(ctx context.Context, number rpc.BlockNumber, tx kv.Tx) (*types.Header, error) {
	n, h, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(number), tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(h); ok && it != nil {
			return it.Header(), nil
		}
	}
	return api._blockReader.Header(ctx, tx, h, n)
}

func (api *BaseAPI) headerByHash(ctx context.Context, hash common.Hash, tx kv.Tx) (*types.Header, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it.Header(), nil
		}
	}

	number, err := api._blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return nil, err
	}

	if number == nil {
		return nil, nil
	}
	return api._blockReader.Header(ctx, tx, hash, *number)
}

// checks the pruning state to see if we would hold information about this
// block in state history or not.  Some strange issues arise getting account
// history for blocks that have been pruned away giving nonce too low errors
// etc. as red herrings
func (api *BaseAPI) checkPruneHistory(ctx context.Context, tx kv.Tx, block uint64) error {
	p, err := api.pruneMode(tx)
	if err != nil {
		return err
	}
	if p == nil {
		// no prune info found
		return nil
	}
	if p.History.Enabled() {
		latest, _, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), tx, api._blockReader, api.filters)
		if err != nil {
			return err
		}
		if latest <= 1 {
			return nil
		}
		prunedTo := p.History.PruneTo(latest)
		if block < prunedTo {
			return state.PrunedError
		}
	}

	return nil
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

type bridgeReader interface {
	Events(ctx context.Context, blockHash common.Hash, blockNum uint64) ([]*types.Message, error)
	EventTxnLookup(ctx context.Context, borTxHash common.Hash) (uint64, bool, error)
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	*BaseAPI
	ethBackend                  rpchelper.ApiBackend
	txPool                      txpoolproto.TxpoolClient
	mining                      txpoolproto.MiningClient
	gasCache                    *GasPriceCache
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

// EthApiConfig defines the configurable parameters for EthAPI
type EthApiConfig struct {
	GasCap                      uint64
	FeeCap                      float64
	ReturnDataLimit             int
	AllowUnprotectedTxs         bool
	MaxGetProofRewindBlockCount int
	SubscribeLogsChannelSize    int
	RpcTxSyncDefaultTimeout     time.Duration
	RpcTxSyncMaxTimeout         time.Duration
}

// NewEthAPI returns APIImpl instance
func NewEthAPI(base *BaseAPI, db kv.TemporalRoDB, eth rpchelper.ApiBackend, txPool txpoolproto.TxpoolClient, mining txpoolproto.MiningClient, cfg *EthApiConfig, logger log.Logger) *APIImpl {
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
		latestPrice: uint256.NewInt(0),
		latestHash:  common.Hash{},
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
