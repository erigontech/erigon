package commands

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	ethFilters "github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/rpc"
	ethapi2 "github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
)

// EthAPI is a collection of functions that are exposed in the
type EthAPI interface {
	// Block related (proposed file: ./eth_blocks.go)
	GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error)
	GetBlockByHash(ctx context.Context, hash rpc.BlockNumberOrHash, fullTx bool) (map[string]interface{}, error)
	GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error)
	GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Uint, error)

	// Transaction related (see ./eth_txs.go)
	GetTransactionByHash(ctx context.Context, hash common.Hash) (*RPCTransaction, error)
	GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (*RPCTransaction, error)
	GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*RPCTransaction, error)
	GetRawTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (hexutility.Bytes, error)
	GetRawTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index hexutil.Uint) (hexutility.Bytes, error)
	GetRawTransactionByHash(ctx context.Context, hash common.Hash) (hexutility.Bytes, error)

	// Receipt related (see ./eth_receipts.go)
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
	GetLogs(ctx context.Context, crit ethFilters.FilterCriteria) (types.Logs, error)
	GetBlockReceipts(ctx context.Context, number rpc.BlockNumber) ([]map[string]interface{}, error)

	// Uncle related (see ./eth_uncles.go)
	GetUncleByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (map[string]interface{}, error)
	GetUncleByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint) (map[string]interface{}, error)
	GetUncleCountByBlockNumber(ctx context.Context, number rpc.BlockNumber) (*hexutil.Uint, error)
	GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (*hexutil.Uint, error)

	// Filter related (see ./eth_filters.go)
	NewPendingTransactionFilter(_ context.Context) (string, error)
	NewBlockFilter(_ context.Context) (string, error)
	NewFilter(_ context.Context, crit ethFilters.FilterCriteria) (string, error)
	UninstallFilter(_ context.Context, index string) (bool, error)
	GetFilterChanges(_ context.Context, index string) ([]any, error)
	GetFilterLogs(_ context.Context, index string) ([]*types.Log, error)

	// Account related (see ./eth_accounts.go)
	Accounts(ctx context.Context) ([]common.Address, error)
	GetBalance(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error)
	GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error)
	GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error)
	GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutility.Bytes, error)

	// System related (see ./eth_system.go)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	Syncing(ctx context.Context) (interface{}, error)
	ChainId(ctx context.Context) (hexutil.Uint64, error) /* called eth_protocolVersion elsewhere */
	ProtocolVersion(_ context.Context) (hexutil.Uint, error)
	GasPrice(_ context.Context) (*hexutil.Big, error)

	// Sending related (see ./eth_call.go)
	Call(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutility.Bytes, error)
	EstimateGas(ctx context.Context, argsOrNil *ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash) (hexutil.Uint64, error)
	SendRawTransaction(ctx context.Context, encodedTx hexutility.Bytes) (common.Hash, error)
	SendTransaction(_ context.Context, txObject interface{}) (common.Hash, error)
	Sign(ctx context.Context, _ common.Address, _ hexutility.Bytes) (hexutility.Bytes, error)
	SignTransaction(_ context.Context, txObject interface{}) (common.Hash, error)
	GetProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNr rpc.BlockNumberOrHash) (*accounts.AccProofResult, error)
	CreateAccessList(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, optimizeGas *bool) (*accessListResult, error)

	// Mining related (see ./eth_mining.go)
	Coinbase(ctx context.Context) (common.Address, error)
	Hashrate(ctx context.Context) (uint64, error)
	Mining(ctx context.Context) (bool, error)
	GetWork(ctx context.Context) ([4]string, error)
	SubmitWork(ctx context.Context, nonce types.BlockNonce, powHash, digest common.Hash) (bool, error)
	SubmitHashrate(ctx context.Context, hashRate hexutil.Uint64, id common.Hash) (bool, error)
}

type BaseAPI struct {
	stateCache   kvcache.Cache                         // thread-safe
	blocksLRU    *lru.Cache[common.Hash, *types.Block] // thread-safe
	filters      *rpchelper.Filters
	_chainConfig atomic.Pointer[chain.Config]
	_genesis     atomic.Pointer[types.Block]
	_historyV3   atomic.Pointer[bool]
	_pruneMode   atomic.Pointer[prune.Mode]

	_blockReader services.FullBlockReader
	_txnReader   services.TxnReader
	_agg         *libstate.AggregatorV3
	_engine      consensus.EngineReader

	evmCallTimeout time.Duration
	dirs           datadir.Dirs
}

func NewBaseApi(f *rpchelper.Filters, stateCache kvcache.Cache, blockReader services.FullBlockReader, agg *libstate.AggregatorV3, singleNodeMode bool, evmCallTimeout time.Duration, engine consensus.EngineReader, dirs datadir.Dirs) *BaseAPI {
	blocksLRUSize := 128 // ~32Mb
	if !singleNodeMode {
		blocksLRUSize = 512
	}
	blocksLRU, err := lru.New[common.Hash, *types.Block](blocksLRUSize)
	if err != nil {
		panic(err)
	}

	return &BaseAPI{filters: f, stateCache: stateCache, blocksLRU: blocksLRU, _blockReader: blockReader, _txnReader: blockReader, _agg: agg, evmCallTimeout: evmCallTimeout, _engine: engine, dirs: dirs}
}

func (api *BaseAPI) chainConfig(tx kv.Tx) (*chain.Config, error) {
	cfg, _, err := api.chainConfigWithGenesis(tx)
	return cfg, err
}

func (api *BaseAPI) engine() consensus.EngineReader {
	return api._engine
}

// nolint:unused
func (api *BaseAPI) genesis(tx kv.Tx) (*types.Block, error) {
	_, genesis, err := api.chainConfigWithGenesis(tx)
	return genesis, err
}

func (api *BaseAPI) txnLookup(ctx context.Context, tx kv.Tx, txnHash common.Hash) (uint64, bool, error) {
	return api._txnReader.TxnLookup(ctx, tx, txnHash)
}

func (api *BaseAPI) blockByNumberWithSenders(ctx context.Context, tx kv.Tx, number uint64) (*types.Block, error) {
	hash, hashErr := api._blockReader.CanonicalHash(ctx, tx, number)
	if hashErr != nil {
		return nil, hashErr
	}
	return api.blockWithSenders(ctx, tx, hash, number)
}

func (api *BaseAPI) blockByHashWithSenders(ctx context.Context, tx kv.Tx, hash common.Hash) (*types.Block, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it, nil
		}
	}
	number := rawdb.ReadHeaderNumber(tx, hash)
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

func (api *BaseAPI) historyV3(tx kv.Tx) bool {
	historyV3 := api._historyV3.Load()
	if historyV3 != nil {
		return *historyV3
	}
	enabled, err := kvcfg.HistoryV3.Enabled(tx)
	if err != nil {
		log.Warn("HisoryV3Enabled: read", "err", err)
		return false
	}
	api._historyV3.Store(&enabled)
	return enabled
}

func (api *BaseAPI) chainConfigWithGenesis(tx kv.Tx) (*chain.Config, *types.Block, error) {
	cc, genesisBlock := api._chainConfig.Load(), api._genesis.Load()
	if cc != nil && genesisBlock != nil {
		return cc, genesisBlock, nil
	}

	hash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, err
	}
	cc, err = rawdb.ReadChainConfig(tx, hash)
	if err != nil {
		return nil, nil, err
	}
	if cc != nil && genesisBlock != nil {
		api._genesis.Store(genesisBlock)
		api._chainConfig.Store(cc)
	}
	return cc, genesisBlock, nil
}

func (api *BaseAPI) pendingBlock() *types.Block {
	return api.filters.LastPendingBlock()
}

func (api *BaseAPI) blockByRPCNumber(ctx context.Context, number rpc.BlockNumber, tx kv.Tx) (*types.Block, error) {
	n, h, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(number), tx, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, h, n)
	return block, err
}

func (api *BaseAPI) headerByRPCNumber(number rpc.BlockNumber, tx kv.Tx) (*types.Header, error) {
	n, h, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(number), tx, api.filters)
	if err != nil {
		return nil, err
	}
	return api._blockReader.Header(context.Background(), tx, h, n)
}

// checks the pruning state to see if we would hold information about this
// block in state history or not.  Some strange issues arise getting account
// history for blocks that have been pruned away giving nonce too low errors
// etc. as red herrings
func (api *BaseAPI) checkPruneHistory(tx kv.Tx, block uint64) error {
	p, err := api.pruneMode(tx)
	if err != nil {
		return err
	}
	if p == nil {
		// no prune info found
		return nil
	}
	if p.History.Enabled() {
		latest, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), tx, api.filters)
		if err != nil {
			return err
		}
		if latest <= 1 {
			return nil
		}
		prunedTo := p.History.PruneTo(latest)
		if block < prunedTo {
			return fmt.Errorf("history has been pruned for this block")
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

	return p, nil
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	*BaseAPI
	ethBackend      rpchelper.ApiBackend
	engineBackend   rpchelper.EngineBackend // TODO(Giulio2002): Remove
	txPool          txpool.TxpoolClient
	mining          txpool.MiningClient
	gasCache        *GasPriceCache
	db              kv.RoDB
	GasCap          uint64
	ReturnDataLimit int
	logger          log.Logger
}

// NewEthAPI returns APIImpl instance
func NewEthAPI(base *BaseAPI, db kv.RoDB, eth rpchelper.ApiBackend, engineBackend rpchelper.EngineBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, gascap uint64, returnDataLimit int, logger log.Logger) *APIImpl {
	if gascap == 0 {
		gascap = uint64(math.MaxUint64 / 2)
	}

	return &APIImpl{
		BaseAPI:         base,
		engineBackend:   engineBackend,
		db:              db,
		ethBackend:      eth,
		txPool:          txPool,
		mining:          mining,
		gasCache:        NewGasPriceCache(),
		GasCap:          gascap,
		ReturnDataLimit: returnDataLimit,
		logger:          logger,
	}
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash           *common.Hash       `json:"blockHash"`
	BlockNumber         *hexutil.Big       `json:"blockNumber"`
	From                common.Address     `json:"from"`
	Gas                 hexutil.Uint64     `json:"gas"`
	GasPrice            *hexutil.Big       `json:"gasPrice,omitempty"`
	Tip                 *hexutil.Big       `json:"maxPriorityFeePerGas,omitempty"`
	FeeCap              *hexutil.Big       `json:"maxFeePerGas,omitempty"`
	Hash                common.Hash        `json:"hash"`
	Input               hexutility.Bytes   `json:"input"`
	Nonce               hexutil.Uint64     `json:"nonce"`
	To                  *common.Address    `json:"to"`
	TransactionIndex    *hexutil.Uint64    `json:"transactionIndex"`
	Value               *hexutil.Big       `json:"value"`
	Type                hexutil.Uint64     `json:"type"`
	Accesses            *types2.AccessList `json:"accessList,omitempty"`
	ChainID             *hexutil.Big       `json:"chainId,omitempty"`
	MaxFeePerDataGas    *hexutil.Big       `json:"maxFeePerDataGas,omitempty"`
	BlobVersionedHashes []common.Hash      `json:"blobVersionedHashes,omitempty"`
	V                   *hexutil.Big       `json:"v"`
	R                   *hexutil.Big       `json:"r"`
	S                   *hexutil.Big       `json:"s"`
}

// newRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCTransaction(tx types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int) *RPCTransaction {
	// Determine the signer. For replay-protected transactions, use the most permissive
	// signer, because we assume that signers are backwards-compatible with old
	// transactions. For non-protected transactions, the homestead signer signer is used
	// because the return value of ChainId is zero for those transactions.
	chainId := uint256.NewInt(0)
	result := &RPCTransaction{
		Type:  hexutil.Uint64(tx.Type()),
		Gas:   hexutil.Uint64(tx.GetGas()),
		Hash:  tx.Hash(),
		Input: hexutility.Bytes(tx.GetData()),
		Nonce: hexutil.Uint64(tx.GetNonce()),
		To:    tx.GetTo(),
		Value: (*hexutil.Big)(tx.GetValue().ToBig()),
	}
	switch t := tx.(type) {
	case *types.LegacyTx:
		chainId = types.DeriveChainId(&t.V)
		// if a legacy transaction has an EIP-155 chain id, include it explicitly, otherwise chain id is not included
		if !chainId.IsZero() {
			result.ChainID = (*hexutil.Big)(chainId.ToBig())
		}
		result.GasPrice = (*hexutil.Big)(t.GasPrice.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
	case *types.AccessListTx:
		chainId.Set(t.ChainID)
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.GasPrice = (*hexutil.Big)(t.GasPrice.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
		result.Accesses = &t.AccessList
	case *types.DynamicFeeTransaction:
		chainId.Set(t.ChainID)
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.Tip = (*hexutil.Big)(t.Tip.ToBig())
		result.FeeCap = (*hexutil.Big)(t.FeeCap.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
		result.Accesses = &t.AccessList
		result.GasPrice = computeGasPrice(tx, blockHash, baseFee)
	case *types.BlobTx:
		chainId.Set(t.ChainID)
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.Tip = (*hexutil.Big)(t.Tip.ToBig())
		result.FeeCap = (*hexutil.Big)(t.FeeCap.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
		result.Accesses = &t.AccessList
		result.GasPrice = computeGasPrice(tx, blockHash, baseFee)
		result.MaxFeePerDataGas = (*hexutil.Big)(t.MaxFeePerDataGas.ToBig())
		result.BlobVersionedHashes = t.BlobVersionedHashes
	}
	signer := types.LatestSignerForChainID(chainId.ToBig())
	result.From, _ = tx.Sender(*signer)
	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

func computeGasPrice(tx types.Transaction, blockHash common.Hash, baseFee *big.Int) *hexutil.Big {
	fee, overflow := uint256.FromBig(baseFee)
	if fee != nil && !overflow && blockHash != (common.Hash{}) {
		// price = min(tip + baseFee, gasFeeCap)
		price := math.Min256(new(uint256.Int).Add(tx.GetTip(), fee), tx.GetFeeCap())
		return (*hexutil.Big)(price.ToBig())
	}
	return nil
}

// newRPCBorTransaction returns a Bor transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCBorTransaction(opaqueTx types.Transaction, txHash common.Hash, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int, chainId *big.Int) *RPCTransaction {
	tx := opaqueTx.(*types.LegacyTx)
	result := &RPCTransaction{
		Type:     hexutil.Uint64(tx.Type()),
		ChainID:  (*hexutil.Big)(new(big.Int)),
		GasPrice: (*hexutil.Big)(tx.GasPrice.ToBig()),
		Gas:      hexutil.Uint64(tx.GetGas()),
		Hash:     txHash,
		Input:    hexutility.Bytes(tx.GetData()),
		Nonce:    hexutil.Uint64(tx.GetNonce()),
		From:     common.Address{},
		To:       tx.GetTo(),
		Value:    (*hexutil.Big)(tx.GetValue().ToBig()),
		V:        (*hexutil.Big)(big.NewInt(0)),
		R:        (*hexutil.Big)(big.NewInt(0)),
		S:        (*hexutil.Big)(big.NewInt(0)),
	}
	if blockHash != (common.Hash{}) {
		result.ChainID = (*hexutil.Big)(new(big.Int).SetUint64(chainId.Uint64()))
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

// newRPCPendingTransaction returns a pending transaction that will serialize to the RPC representation
func newRPCPendingTransaction(tx types.Transaction, current *types.Header, config *chain.Config) *RPCTransaction {
	var baseFee *big.Int
	if current != nil {
		baseFee = misc.CalcBaseFee(config, current)
	}
	return newRPCTransaction(tx, common.Hash{}, 0, 0, baseFee)
}

// newRPCRawTransactionFromBlockIndex returns the bytes of a transaction given a block and a transaction index.
func newRPCRawTransactionFromBlockIndex(b *types.Block, index uint64) (hexutility.Bytes, error) {
	txs := b.Transactions()
	if index >= uint64(len(txs)) {
		return nil, nil
	}
	var buf bytes.Buffer
	err := txs[index].MarshalBinary(&buf)
	return buf.Bytes(), err
}

type GasPriceCache struct {
	latestPrice *big.Int
	latestHash  common.Hash
	mtx         sync.Mutex
}

func NewGasPriceCache() *GasPriceCache {
	return &GasPriceCache{
		latestPrice: big.NewInt(0),
		latestHash:  common.Hash{},
	}
}

func (c *GasPriceCache) GetLatest() (common.Hash, *big.Int) {
	var hash common.Hash
	var price *big.Int
	c.mtx.Lock()
	hash = c.latestHash
	price = c.latestPrice
	c.mtx.Unlock()
	return hash, price
}

func (c *GasPriceCache) SetLatest(hash common.Hash, price *big.Int) {
	c.mtx.Lock()
	c.latestPrice = price
	c.latestHash = hash
	c.mtx.Unlock()
}
