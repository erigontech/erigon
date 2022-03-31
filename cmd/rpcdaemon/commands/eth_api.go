package commands

import (
	"bytes"
	"context"
	"math/big"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	ethFilters "github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
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
	GetRawTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (hexutil.Bytes, error)
	GetRawTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index hexutil.Uint) (hexutil.Bytes, error)
	GetRawTransactionByHash(ctx context.Context, hash common.Hash) (hexutil.Bytes, error)

	// Receipt related (see ./eth_receipts.go)
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
	GetLogs(ctx context.Context, crit ethFilters.FilterCriteria) ([]*types.Log, error)
	GetBlockReceipts(ctx context.Context, number rpc.BlockNumber) ([]map[string]interface{}, error)

	// Uncle related (see ./eth_uncles.go)
	GetUncleByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (map[string]interface{}, error)
	GetUncleByBlockHashAndIndex(ctx context.Context, hash common.Hash, index hexutil.Uint) (map[string]interface{}, error)
	GetUncleCountByBlockNumber(ctx context.Context, number rpc.BlockNumber) (*hexutil.Uint, error)
	GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (*hexutil.Uint, error)

	// Filter related (see ./eth_filters.go)
	NewPendingTransactionFilter(_ context.Context) (hexutil.Uint64, error)
	NewBlockFilter(_ context.Context) (hexutil.Uint64, error)
	NewFilter(_ context.Context, filter interface{}) (hexutil.Uint64, error)
	UninstallFilter(_ context.Context, index hexutil.Uint64) (bool, error)
	GetFilterChanges(_ context.Context, index hexutil.Uint64) ([]interface{}, error)

	// Account related (see ./eth_accounts.go)
	Accounts(ctx context.Context) ([]common.Address, error)
	GetBalance(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Big, error)
	GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error)
	GetStorageAt(ctx context.Context, address common.Address, index string, blockNrOrHash rpc.BlockNumberOrHash) (string, error)
	GetCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error)

	// System related (see ./eth_system.go)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	Syncing(ctx context.Context) (interface{}, error)
	ChainId(ctx context.Context) (hexutil.Uint64, error) /* called eth_protocolVersion elsewhere */
	ProtocolVersion(_ context.Context) (hexutil.Uint, error)
	GasPrice(_ context.Context) (*hexutil.Big, error)

	// Sending related (see ./eth_call.go)
	Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *ethapi.StateOverrides) (hexutil.Bytes, error)
	EstimateGas(ctx context.Context, argsOrNil *ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash) (hexutil.Uint64, error)
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
	SendTransaction(_ context.Context, txObject interface{}) (common.Hash, error)
	Sign(ctx context.Context, _ common.Address, _ hexutil.Bytes) (hexutil.Bytes, error)
	SignTransaction(_ context.Context, txObject interface{}) (common.Hash, error)
	GetProof(ctx context.Context, address common.Address, storageKeys []string, blockNr rpc.BlockNumber) (*interface{}, error)
	CreateAccessList(ctx context.Context, args ethapi.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, optimizeGas *bool) (*accessListResult, error)

	// Mining related (see ./eth_mining.go)
	Coinbase(ctx context.Context) (common.Address, error)
	Hashrate(ctx context.Context) (uint64, error)
	Mining(ctx context.Context) (bool, error)
	GetWork(ctx context.Context) ([4]string, error)
	SubmitWork(ctx context.Context, nonce types.BlockNonce, powHash, digest common.Hash) (bool, error)
	SubmitHashrate(ctx context.Context, hashRate hexutil.Uint64, id common.Hash) (bool, error)
}

type BaseAPI struct {
	stateCache   kvcache.Cache // thread-safe
	blocksLRU    *lru.Cache    // thread-safe
	filters      *filters.Filters
	_chainConfig *params.ChainConfig
	_genesis     *types.Block
	_genesisLock sync.RWMutex

	_blockReader interfaces.BlockReader
	_txnReader   interfaces.TxnReader
	TevmEnabled  bool // experiment
}

func NewBaseApi(f *filters.Filters, stateCache kvcache.Cache, blockReader interfaces.BlockAndTxnReader, singleNodeMode bool) *BaseAPI {
	blocksLRUSize := 128 // ~32Mb
	if !singleNodeMode {
		blocksLRUSize = 512
	}
	blocksLRU, err := lru.New(blocksLRUSize)
	if err != nil {
		panic(err)
	}

	return &BaseAPI{filters: f, stateCache: stateCache, blocksLRU: blocksLRU, _blockReader: blockReader, _txnReader: blockReader}
}

func (api *BaseAPI) chainConfig(tx kv.Tx) (*params.ChainConfig, error) {
	cfg, _, err := api.chainConfigWithGenesis(tx)
	return cfg, err
}

func (api *BaseAPI) EnableTevmExperiment() { api.TevmEnabled = true }

// nolint:unused
func (api *BaseAPI) genesis(tx kv.Tx) (*types.Block, error) {
	_, genesis, err := api.chainConfigWithGenesis(tx)
	return genesis, err
}

func (api *BaseAPI) txnLookup(ctx context.Context, tx kv.Tx, txnHash common.Hash) (uint64, bool, error) {
	return api._txnReader.TxnLookup(ctx, tx, txnHash)
}

func (api *BaseAPI) blockByNumberWithSenders(tx kv.Tx, number uint64) (*types.Block, error) {
	hash, hashErr := rawdb.ReadCanonicalHash(tx, number)
	if hashErr != nil {
		return nil, hashErr
	}
	return api.blockWithSenders(tx, hash, number)
}
func (api *BaseAPI) blockByHashWithSenders(tx kv.Tx, hash common.Hash) (*types.Block, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it.(*types.Block), nil
		}
	}
	number := rawdb.ReadHeaderNumber(tx, hash)
	if number == nil {
		return nil, nil
	}
	return api.blockWithSenders(tx, hash, *number)
}
func (api *BaseAPI) blockWithSenders(tx kv.Tx, hash common.Hash, number uint64) (*types.Block, error) {
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			return it.(*types.Block), nil
		}
	}
	block, _, err := api._blockReader.BlockWithSenders(context.Background(), tx, hash, number)
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

func (api *BaseAPI) chainConfigWithGenesis(tx kv.Tx) (*params.ChainConfig, *types.Block, error) {
	api._genesisLock.RLock()
	cc, genesisBlock := api._chainConfig, api._genesis
	api._genesisLock.RUnlock()

	if cc != nil {
		return cc, genesisBlock, nil
	}
	genesisBlock, err := rawdb.ReadBlockByNumber(tx, 0)
	if err != nil {
		return nil, nil, err
	}
	cc, err = rawdb.ReadChainConfig(tx, genesisBlock.Hash())
	if err != nil {
		return nil, nil, err
	}
	if cc != nil && genesisBlock != nil {
		api._genesisLock.Lock()
		api._genesis = genesisBlock
		api._chainConfig = cc
		api._genesisLock.Unlock()
	}
	return cc, genesisBlock, nil
}

func (api *BaseAPI) pendingBlock() *types.Block {
	return api.filters.LastPendingBlock()
}

func (api *BaseAPI) blockByRPCNumber(number rpc.BlockNumber, tx kv.Tx) (*types.Block, error) {
	if number == rpc.PendingBlockNumber {
		return api.pendingBlock(), nil
	}

	n, err := getBlockNumber(number, tx)
	if err != nil {
		return nil, err
	}

	block, err := api.blockByNumberWithSenders(tx, n)
	return block, err
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	*BaseAPI
	ethBackend services.ApiBackend
	txPool     txpool.TxpoolClient
	mining     txpool.MiningClient
	db         kv.RoDB
	GasCap     uint64
}

// NewEthAPI returns APIImpl instance
func NewEthAPI(base *BaseAPI, db kv.RoDB, eth services.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, gascap uint64) *APIImpl {
	if gascap == 0 {
		gascap = uint64(math.MaxUint64 / 2)
	}

	return &APIImpl{
		BaseAPI:    base,
		db:         db,
		ethBackend: eth,
		txPool:     txPool,
		mining:     mining,
		GasCap:     gascap,
	}
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash        *common.Hash      `json:"blockHash"`
	BlockNumber      *hexutil.Big      `json:"blockNumber"`
	From             common.Address    `json:"from"`
	Gas              hexutil.Uint64    `json:"gas"`
	GasPrice         *hexutil.Big      `json:"gasPrice,omitempty"`
	Tip              *hexutil.Big      `json:"maxPriorityFeePerGas,omitempty"`
	FeeCap           *hexutil.Big      `json:"maxFeePerGas,omitempty"`
	Hash             common.Hash       `json:"hash"`
	Input            hexutil.Bytes     `json:"input"`
	Nonce            hexutil.Uint64    `json:"nonce"`
	To               *common.Address   `json:"to"`
	TransactionIndex *hexutil.Uint64   `json:"transactionIndex"`
	Value            *hexutil.Big      `json:"value"`
	Type             hexutil.Uint64    `json:"type"`
	Accesses         *types.AccessList `json:"accessList,omitempty"`
	ChainID          *hexutil.Big      `json:"chainId,omitempty"`
	V                *hexutil.Big      `json:"v"`
	R                *hexutil.Big      `json:"r"`
	S                *hexutil.Big      `json:"s"`
}

// newRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCTransaction(tx types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int) *RPCTransaction {
	// Determine the signer. For replay-protected transactions, use the most permissive
	// signer, because we assume that signers are backwards-compatible with old
	// transactions. For non-protected transactions, the homestead signer signer is used
	// because the return value of ChainId is zero for those transactions.
	var chainId *big.Int
	result := &RPCTransaction{
		Type:  hexutil.Uint64(tx.Type()),
		Gas:   hexutil.Uint64(tx.GetGas()),
		Hash:  tx.Hash(),
		Input: hexutil.Bytes(tx.GetData()),
		Nonce: hexutil.Uint64(tx.GetNonce()),
		To:    tx.GetTo(),
		Value: (*hexutil.Big)(tx.GetValue().ToBig()),
	}
	switch t := tx.(type) {
	case *types.LegacyTx:
		chainId = types.DeriveChainId(&t.V).ToBig()
		result.GasPrice = (*hexutil.Big)(t.GasPrice.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
	case *types.AccessListTx:
		chainId = t.ChainID.ToBig()
		result.ChainID = (*hexutil.Big)(chainId)
		result.GasPrice = (*hexutil.Big)(t.GasPrice.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
		result.Accesses = &t.AccessList
	case *types.DynamicFeeTransaction:
		chainId = t.ChainID.ToBig()
		result.ChainID = (*hexutil.Big)(chainId)
		result.Tip = (*hexutil.Big)(t.Tip.ToBig())
		result.FeeCap = (*hexutil.Big)(t.FeeCap.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
		result.Accesses = &t.AccessList
		baseFee, overflow := uint256.FromBig(baseFee)
		if baseFee != nil && !overflow && blockHash != (common.Hash{}) {
			// price = min(tip + baseFee, gasFeeCap)
			price := math.Min256(new(uint256.Int).Add(tx.GetTip(), baseFee), tx.GetFeeCap())
			result.GasPrice = (*hexutil.Big)(price.ToBig())
		} else {
			result.GasPrice = nil
		}
	}
	signer := types.LatestSignerForChainID(chainId)
	result.From, _ = tx.Sender(*signer)
	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

// newRPCPendingTransaction returns a pending transaction that will serialize to the RPC representation
func newRPCPendingTransaction(tx types.Transaction, current *types.Header, config *params.ChainConfig) *RPCTransaction {
	var baseFee *big.Int
	if current != nil {
		baseFee = misc.CalcBaseFee(config, current)
	}
	return newRPCTransaction(tx, common.Hash{}, 0, 0, baseFee)
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
