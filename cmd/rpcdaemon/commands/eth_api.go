package commands

import (
	"context"
	"math/big"
	"sync"

	rpcfilters "github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/filters"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
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

	// Receipt related (see ./eth_receipts.go)
	GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error)
	GetLogs(ctx context.Context, crit filters.FilterCriteria) ([]*types.Log, error)

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
	Call(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *map[common.Address]ethapi.Account) (hexutil.Bytes, error)
	EstimateGas(ctx context.Context, args ethapi.CallArgs) (hexutil.Uint64, error)
	SendRawTransaction(ctx context.Context, encodedTx hexutil.Bytes) (common.Hash, error)
	SendTransaction(_ context.Context, txObject interface{}) (common.Hash, error)
	Sign(ctx context.Context, _ common.Address, _ hexutil.Bytes) (hexutil.Bytes, error)
	SignTransaction(_ context.Context, txObject interface{}) (common.Hash, error)
	GetProof(ctx context.Context, address common.Address, storageKeys []string, blockNr rpc.BlockNumber) (*interface{}, error)

	// Mining related (see ./eth_mining.go)
	Coinbase(ctx context.Context) (common.Address, error)
	Hashrate(ctx context.Context) (uint64, error)
	Mining(ctx context.Context) (bool, error)
	GetWork(ctx context.Context) ([4]string, error)
	SubmitWork(ctx context.Context, nonce types.BlockNonce, powHash, digest common.Hash) (bool, error)
	SubmitHashrate(ctx context.Context, hashRate hexutil.Uint64, id common.Hash) (bool, error)

	// Deprecated commands in eth_ (proposed file: ./eth_deprecated.go)
	GetCompilers(_ context.Context) ([]string, error)
	CompileLLL(_ context.Context, _ string) (hexutil.Bytes, error)
	CompileSolidity(ctx context.Context, _ string) (hexutil.Bytes, error)
	CompileSerpent(ctx context.Context, _ string) (hexutil.Bytes, error)
}

type BaseAPI struct {
	_chainConfig    *params.ChainConfig
	_genesis        *types.Block
	_genesisSetOnce sync.Once
}

func (api *BaseAPI) chainConfig(db ethdb.Database) (*params.ChainConfig, error) {
	cfg, _, err := api.chainConfigWithGenesis(db)
	return cfg, err
}

//nolint:unused
func (api *BaseAPI) genesis(db ethdb.Database) (*types.Block, error) {
	_, genesis, err := api.chainConfigWithGenesis(db)
	return genesis, err
}

func (api *BaseAPI) chainConfigWithGenesis(db ethdb.Database) (*params.ChainConfig, *types.Block, error) {
	if api._chainConfig != nil {
		return api._chainConfig, api._genesis, nil
	}

	genesisBlock, err := rawdb.ReadBlockByNumber(db, 0)
	if err != nil {
		return nil, nil, err
	}
	cc, err := rawdb.ReadChainConfig(db, genesisBlock.Hash())
	if err != nil {
		return nil, nil, err
	}
	if cc != nil && genesisBlock != nil {
		api._genesisSetOnce.Do(func() {
			api._genesis = genesisBlock
			api._chainConfig = cc
		})
	}
	return cc, genesisBlock, nil
}

// APIImpl is implementation of the EthAPI interface based on remote Db access
type APIImpl struct {
	*BaseAPI
	ethBackend core.ApiBackend
	db         ethdb.Database
	GasCap     uint64
	filters    *rpcfilters.Filters
	pending    *rpchelper.Pending
}

// NewEthAPI returns APIImpl instance
func NewEthAPI(db ethdb.Database, eth core.ApiBackend, gascap uint64, filters *rpcfilters.Filters, pending *rpchelper.Pending) *APIImpl {
	return &APIImpl{
		BaseAPI:    &BaseAPI{},
		db:         db,
		ethBackend: eth,
		GasCap:     gascap,
		filters:    filters,
		pending:    pending,
	}
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash        *common.Hash    `json:"blockHash"`
	BlockNumber      *hexutil.Big    `json:"blockNumber"`
	From             common.Address  `json:"from"`
	Gas              hexutil.Uint64  `json:"gas"`
	GasPrice         *hexutil.Big    `json:"gasPrice"`
	Hash             common.Hash     `json:"hash"`
	Input            hexutil.Bytes   `json:"input"`
	Nonce            hexutil.Uint64  `json:"nonce"`
	R                *hexutil.Big    `json:"r"`
	S                *hexutil.Big    `json:"s"`
	To               *common.Address `json:"to"`
	TransactionIndex *hexutil.Uint64 `json:"transactionIndex"`
	V                *hexutil.Big    `json:"v"`
	Value            *hexutil.Big    `json:"value"`
}

// newRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCTransaction(tx *types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64) *RPCTransaction {
	var signer types.Signer = types.FrontierSigner{}
	if tx.Protected() {
		signer = types.NewEIP155Signer(tx.ChainId().ToBig())
	}
	from, _ := types.Sender(signer, tx)
	v, r, s := tx.RawSignatureValues()

	result := &RPCTransaction{
		From:     from,
		Gas:      hexutil.Uint64(tx.Gas()),
		GasPrice: (*hexutil.Big)(tx.GasPrice().ToBig()),
		Hash:     tx.Hash(),
		Input:    hexutil.Bytes(tx.Data()),
		Nonce:    hexutil.Uint64(tx.Nonce()),
		R:        (*hexutil.Big)(r.ToBig()),
		S:        (*hexutil.Big)(s.ToBig()),
		To:       tx.To(),
		V:        (*hexutil.Big)(v.ToBig()),
		Value:    (*hexutil.Big)(tx.Value().ToBig()),
	}
	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}
