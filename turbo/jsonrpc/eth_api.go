package jsonrpc

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

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
	GetBlockReceipts(ctx context.Context, numberOrHash rpc.BlockNumberOrHash) ([]map[string]interface{}, error)

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
	// all caches are thread-safe
	stateCache    kvcache.Cache
	blocksLRU     *lru.Cache[common.Hash, *types.Block]
	receiptsCache *lru.Cache[common.Hash, []*types.Receipt]

	filters      *rpchelper.Filters
	_chainConfig atomic.Pointer[chain.Config]
	_genesis     atomic.Pointer[types.Block]
	_historyV3   atomic.Pointer[bool]
	_pruneMode   atomic.Pointer[prune.Mode]

	_blockReader services.FullBlockReader
	_txnReader   services.TxnReader
	_agg         *libstate.Aggregator
	_engine      consensus.EngineReader

	evmCallTimeout time.Duration
	dirs           datadir.Dirs
}

func NewBaseApi(f *rpchelper.Filters, stateCache kvcache.Cache, blockReader services.FullBlockReader, agg *libstate.Aggregator, singleNodeMode bool, evmCallTimeout time.Duration, engine consensus.EngineReader, dirs datadir.Dirs) *BaseAPI {
	var (
		blocksLRUSize      = 128 // ~32Mb
		receiptsCacheLimit = 32
	)
	// if RPCDaemon deployed as independent process: increase cache sizes
	if !singleNodeMode {
		blocksLRUSize *= 5
		receiptsCacheLimit *= 5
	}
	blocksLRU, err := lru.New[common.Hash, *types.Block](blocksLRUSize)
	if err != nil {
		panic(err)
	}
	receiptsCache, err := lru.New[common.Hash, []*types.Receipt](receiptsCacheLimit)
	if err != nil {
		panic(err)
	}

	return &BaseAPI{
		filters:        f,
		stateCache:     stateCache,
		blocksLRU:      blocksLRU,
		receiptsCache:  receiptsCache,
		_blockReader:   blockReader,
		_txnReader:     blockReader,
		_agg:           agg,
		evmCallTimeout: evmCallTimeout,
		_engine:        engine,
		dirs:           dirs,
	}
}

func (api *BaseAPI) chainConfig(ctx context.Context, tx kv.Tx) (*chain.Config, error) {
	cfg, _, err := api.chainConfigWithGenesis(ctx, tx)
	return cfg, err
}

func (api *BaseAPI) engine() consensus.EngineReader {
	return api._engine
}

// nolint:unused
func (api *BaseAPI) genesis(ctx context.Context, tx kv.Tx) (*types.Block, error) {
	_, genesis, err := api.chainConfigWithGenesis(ctx, tx)
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

func (api *BaseAPI) chainConfigWithGenesis(ctx context.Context, tx kv.Tx) (*chain.Config, *types.Block, error) {
	cc, genesisBlock := api._chainConfig.Load(), api._genesis.Load()
	if cc != nil && genesisBlock != nil {
		return cc, genesisBlock, nil
	}

	genesisBlock, err := api.blockByRPCNumber(ctx, 0, tx)
	if err != nil {
		return nil, nil, err
	}
	if genesisBlock == nil {
		return nil, nil, fmt.Errorf("genesis block not found in database")
	}
	cc, err = rawdb.ReadChainConfig(tx, genesisBlock.Hash())
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

	// it's ok to use context.Background(), because in "Remote RPCDaemon" `tx` already contains internal ctx
	block, err := api.blockWithSenders(ctx, tx, h, n)
	return block, err
}

func (api *BaseAPI) headerByRPCNumber(ctx context.Context, number rpc.BlockNumber, tx kv.Tx) (*types.Header, error) {
	n, h, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithNumber(number), tx, api.filters)
	if err != nil {
		return nil, err
	}
	return api._blockReader.Header(ctx, tx, h, n)
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
	ethBackend                  rpchelper.ApiBackend
	txPool                      txpool.TxpoolClient
	mining                      txpool.MiningClient
	gasCache                    *GasPriceCache
	db                          kv.RoDB
	GasCap                      uint64
	FeeCap                      float64
	ReturnDataLimit             int
	AllowUnprotectedTxs         bool
	MaxGetProofRewindBlockCount int
	SubscribeLogsChannelSize    int
	logger                      log.Logger
}

// NewEthAPI returns APIImpl instance
func NewEthAPI(base *BaseAPI, db kv.RoDB, eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, gascap uint64, feecap float64, returnDataLimit int, allowUnprotectedTxs bool, maxGetProofRewindBlockCount int, subscribeLogsChannelSize int, logger log.Logger) *APIImpl {
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
		FeeCap:                      feecap,
		AllowUnprotectedTxs:         allowUnprotectedTxs,
		ReturnDataLimit:             returnDataLimit,
		MaxGetProofRewindBlockCount: maxGetProofRewindBlockCount,
		SubscribeLogsChannelSize:    subscribeLogsChannelSize,
		logger:                      logger,
	}
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash           *common.Hash               `json:"blockHash"`
	BlockNumber         *hexutil.Big               `json:"blockNumber"`
	From                common.Address             `json:"from"`
	Gas                 hexutil.Uint64             `json:"gas"`
	GasPrice            *hexutil.Big               `json:"gasPrice,omitempty"`
	Tip                 *hexutil.Big               `json:"maxPriorityFeePerGas,omitempty"`
	FeeCap              *hexutil.Big               `json:"maxFeePerGas,omitempty"`
	Hash                common.Hash                `json:"hash"`
	Input               hexutility.Bytes           `json:"input"`
	Nonce               hexutil.Uint64             `json:"nonce"`
	To                  *common.Address            `json:"to"`
	TransactionIndex    *hexutil.Uint64            `json:"transactionIndex"`
	Value               *hexutil.Big               `json:"value"`
	Type                hexutil.Uint64             `json:"type"`
	Accesses            *types2.AccessList         `json:"accessList,omitempty"`
	ChainID             *hexutil.Big               `json:"chainId,omitempty"`
	MaxFeePerBlobGas    *hexutil.Big               `json:"maxFeePerBlobGas,omitempty"`
	BlobVersionedHashes []common.Hash              `json:"blobVersionedHashes,omitempty"`
	Authorizations      *[]types.JsonAuthorization `json:"authorizationList,omitempty"`
	V                   *hexutil.Big               `json:"v"`
	YParity             *hexutil.Big               `json:"yParity,omitempty"`
	R                   *hexutil.Big               `json:"r"`
	S                   *hexutil.Big               `json:"s"`
}

// NewRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func NewRPCTransaction(txn types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int) *RPCTransaction {
	// Determine the signer. For replay-protected transactions, use the most permissive
	// signer, because we assume that signers are backwards-compatible with old
	// transactions. For non-protected transactions, the homestead signer is used
	// because the return value of ChainId is zero for those transactions.
	chainId := uint256.NewInt(0)
	result := &RPCTransaction{
		Type:  hexutil.Uint64(txn.Type()),
		Gas:   hexutil.Uint64(txn.GetGas()),
		Hash:  txn.Hash(),
		Input: hexutility.Bytes(txn.GetData()),
		Nonce: hexutil.Uint64(txn.GetNonce()),
		To:    txn.GetTo(),
		Value: (*hexutil.Big)(txn.GetValue().ToBig()),
	}
	if t, ok := txn.(*types.BlobTxWrapper); ok {
		txn = &t.Tx
	}

	v, r, s := txn.RawSignatureValues()
	result.V = (*hexutil.Big)(v.ToBig())
	result.R = (*hexutil.Big)(r.ToBig())
	result.S = (*hexutil.Big)(s.ToBig())

	if txn.Type() == types.LegacyTxType {
		chainId = types.DeriveChainId(v)
		// if a legacy transaction has an EIP-155 chain id, include it explicitly, otherwise chain id is not included
		if !chainId.IsZero() {
			result.ChainID = (*hexutil.Big)(chainId.ToBig())
		}
		result.GasPrice = (*hexutil.Big)(txn.GetPrice().ToBig())
	} else {
		chainId.Set(txn.GetChainID())
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.YParity = (*hexutil.Big)(v.ToBig())
		acl := txn.GetAccessList()
		result.Accesses = &acl

		if txn.Type() == types.AccessListTxType {
			result.GasPrice = (*hexutil.Big)(txn.GetPrice().ToBig())
		} else {
			result.GasPrice = computeGasPrice(txn, blockHash, baseFee)
			result.Tip = (*hexutil.Big)(txn.GetTip().ToBig())
			result.FeeCap = (*hexutil.Big)(txn.GetFeeCap().ToBig())
		}

		if txn.Type() == types.BlobTxType {
			txn.GetBlobGas()
			blobTx := txn.(*types.BlobTx)
			result.MaxFeePerBlobGas = (*hexutil.Big)(blobTx.MaxFeePerBlobGas.ToBig())
			result.BlobVersionedHashes = blobTx.BlobVersionedHashes
		} else if txn.Type() == types.SetCodeTxType {
			setCodeTx := txn.(*types.SetCodeTransaction)
			ats := make([]types.JsonAuthorization, len(setCodeTx.GetAuthorizations()))
			for i, a := range setCodeTx.GetAuthorizations() {
				ats[i] = types.JsonAuthorization{}.FromAuthorization(a)
			}
			result.Authorizations = &ats
		}
	}

	signer := types.LatestSignerForChainID(chainId.ToBig())
	result.From, _ = txn.Sender(*signer)
	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

func computeGasPrice(txn types.Transaction, blockHash common.Hash, baseFee *big.Int) *hexutil.Big {
	fee, overflow := uint256.FromBig(baseFee)
	if fee != nil && !overflow && blockHash != (common.Hash{}) {
		// price = min(tip + baseFee, gasFeeCap)
		price := math.Min256(new(uint256.Int).Add(txn.GetTip(), fee), txn.GetFeeCap())
		return (*hexutil.Big)(price.ToBig())
	}
	return nil
}

// newRPCBorTransaction returns a Bor transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCBorTransaction(opaqueTxn types.Transaction, txHash common.Hash, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int, chainId *big.Int) *RPCTransaction {
	txn := opaqueTxn.(*types.LegacyTx)
	result := &RPCTransaction{
		Type:     hexutil.Uint64(txn.Type()),
		ChainID:  (*hexutil.Big)(new(big.Int)),
		GasPrice: (*hexutil.Big)(txn.GasPrice.ToBig()),
		Gas:      hexutil.Uint64(txn.GetGas()),
		Hash:     txHash,
		Input:    hexutility.Bytes(txn.GetData()),
		Nonce:    hexutil.Uint64(txn.GetNonce()),
		From:     common.Address{},
		To:       txn.GetTo(),
		Value:    (*hexutil.Big)(txn.GetValue().ToBig()),
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
func newRPCPendingTransaction(txn types.Transaction, current *types.Header, config *chain.Config) *RPCTransaction {
	var baseFee *big.Int
	if current != nil {
		baseFee = misc.CalcBaseFee(config, current)
	}
	return NewRPCTransaction(txn, common.Hash{}, 0, 0, baseFee)
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
