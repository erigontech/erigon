// Copyright 2016 The go-ethereum Authors
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

// Package ethclient provides a client for the Ethereum RPC API.
package ethclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/holiman/uint256"
)

var EmptyTxsHash = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

// Client defines typed wrappers for the Ethereum RPC API.
type Client struct {
	c *rpc.Client
}

// Dial connects a client to the given URL.
func Dial(rawurl string) (*Client, error) {
	return DialContext(context.Background(), rawurl)
}

func DialContext(ctx context.Context, rawurl string) (*Client, error) {
	c, err := rpc.DialContext(ctx, rawurl, log.New())
	if err != nil {
		return nil, err
	}
	return NewClient(c), nil
}

// NewClient creates a client that uses the given RPC client.
func NewClient(c *rpc.Client) *Client {
	return &Client{c}
}

func (ec *Client) Close() {
	ec.c.Close()
}

// Blockchain Access

// ChainID retrieves the current chain ID for transaction replay protection.
func (ec *Client) ChainID(ctx context.Context) (*big.Int, error) {
	var result hexutil.Big
	err := ec.c.CallContext(ctx, &result, "eth_chainId")
	if err != nil {
		return nil, err
	}
	return (*big.Int)(&result), err
}

// BlockByHash returns the given full block.
//
// Note that loading full blocks requires two requests. Use HeaderByHash
// if you don't need all transactions or uncle headers.
func (ec *Client) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return ec.getBlock(ctx, "eth_getBlockByHash", hash, true)
}

// BlockByNumber returns a block from the current canonical chain. If number is nil, the
// latest known block is returned.
//
// Note that loading full blocks requires two requests. Use HeaderByNumber
// if you don't need all transactions or uncle headers.
func (ec *Client) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	return ec.getBlock(ctx, "eth_getBlockByNumber", toBlockNumArg(number), true)
}

// BlockNumber returns the most recent block number
func (ec *Client) BlockNumber(ctx context.Context) (uint64, error) {
	var result hexutil.Uint64
	err := ec.c.CallContext(ctx, &result, "eth_blockNumber")
	return uint64(result), err
}

// PeerCount returns the number of p2p peers as reported by the net_peerCount method.
func (ec *Client) PeerCount(ctx context.Context) (uint64, error) {
	var result hexutil.Uint64
	err := ec.c.CallContext(ctx, &result, "net_peerCount")
	return uint64(result), err
}

type rpcBlock struct {
	Hash         common.Hash         `json:"hash"`
	Transactions []rpcTransaction    `json:"transactions"`
	UncleHashes  []common.Hash       `json:"uncles"`
	Withdrawals  []*types.Withdrawal `json:"withdrawals,omitempty"`
}

func (ec *Client) getBlock(ctx context.Context, method string, args ...interface{}) (*types.Block, error) {
	var raw json.RawMessage
	err := ec.c.CallContext(ctx, &raw, method, args...)
	if err != nil {
		return nil, err
	}

	// Decode header and transactions.
	var head *types.Header
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, err
	}
	// When the block is not found, the API returns JSON null.
	if head == nil {
		return nil, ethereum.NotFound
	}

	var body rpcBlock
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	// Quick-verify transaction and uncle lists. This mostly helps with debugging the server.
	if head.UncleHash == types.EmptyUncleHash && len(body.UncleHashes) > 0 {
		return nil, fmt.Errorf("server returned non-empty uncle list but block header indicates no uncles")
	}
	if head.UncleHash != types.EmptyUncleHash && len(body.UncleHashes) == 0 {
		return nil, fmt.Errorf("server returned empty uncle list but block header indicates uncles")
	}
	if head.TxHash == EmptyTxsHash && len(body.Transactions) > 0 {
		return nil, fmt.Errorf("server returned non-empty transaction list but block header indicates no transactions")
	}
	if head.TxHash != EmptyTxsHash && len(body.Transactions) == 0 {
		return nil, fmt.Errorf("server returned empty transaction list but block header indicates transactions")
	}
	// Load uncles because they are not included in the block response.
	var uncles []*types.Header
	if len(body.UncleHashes) > 0 {
		uncles = make([]*types.Header, len(body.UncleHashes))
		reqs := make([]rpc.BatchElem, len(body.UncleHashes))
		for i := range reqs {
			reqs[i] = rpc.BatchElem{
				Method: "eth_getUncleByBlockHashAndIndex",
				Args:   []interface{}{body.Hash, hexutil.EncodeUint64(uint64(i))},
				Result: &uncles[i],
			}
		}
		if err := ec.c.BatchCallContext(ctx, reqs); err != nil {
			return nil, err
		}
		for i := range reqs {
			if reqs[i].Error != nil {
				return nil, reqs[i].Error
			}
			if uncles[i] == nil {
				return nil, fmt.Errorf("got null header for uncle %d of block %x", i, body.Hash[:])
			}
		}
	}
	// Fill the sender cache of transactions in the block.
	txs := make([]types.Transaction, len(body.Transactions))
	for i, tx := range body.Transactions {
		txs[i] = tx.Tx()
	}
	return types.NewBlockWithHeader(head).WithBody(txs, uncles).WithWithdrawals(body.Withdrawals), nil
}

// HeaderByHash returns the block header with the given hash.
func (ec *Client) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	var head *types.Header
	err := ec.c.CallContext(ctx, &head, "eth_getBlockByHash", hash, false)
	if err == nil && head == nil {
		err = ethereum.NotFound
	}
	return head, err
}

// HeaderByNumber returns a block header from the current canonical chain. If number is
// nil, the latest known header is returned.
func (ec *Client) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	var head *types.Header
	err := ec.c.CallContext(ctx, &head, "eth_getBlockByNumber", toBlockNumArg(number), false)
	if err == nil && head == nil {
		err = ethereum.NotFound
	}
	return head, err
}

// HeadersByNumbers returns a list of block headers corresponding to the provided block numbers.
// The returned slice order matches the input numbers order.
func (ec *Client) HeadersByNumbers(ctx context.Context, numbers []*big.Int) ([]*types.Header, error) {
	if len(numbers) == 0 {
		return nil, nil
	}
	heads := make([]*types.Header, len(numbers))
	reqs := make([]rpc.BatchElem, len(numbers))
	for i, n := range numbers {
		reqs[i] = rpc.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []interface{}{toBlockNumArg(n), false},
			Result: &heads[i],
		}
	}
	if err := ec.c.BatchCallContext(ctx, reqs); err != nil {
		return nil, err
	}
	// Do not fail the entire batch on per-element errors; callers may
	// inspect nil entries and decide how to handle missing headers.
	return heads, nil
}

type BigInt struct {
	*big.Int
}

func (bi *BigInt) UnmarshalJSON(data []byte) error {
	if bi.Int == nil {
		bi.Int = new(big.Int)
	}
	// Remove quotes
	unquotedData, err := strconv.Unquote(string(data))
	if err != nil {
		return err
	}

	if unquotedData[0] == '0' && unquotedData[1] == 'x' {
		unquotedData = unquotedData[2:]
	}
	// big.Int SetString method expects a base; 16 is used here because the data is hexadecimal
	_, success := bi.SetString(unquotedData, 16)
	if !success {
		return errors.New("failed to convert string to big.Int")
	}
	return nil
}

// ToInt method returns the big.Int value as an uint256.Int
func (bi *BigInt) ToInt() *uint256.Int {
	newInt := &uint256.Int{}
	newInt.SetBytes(bi.Bytes())
	return newInt
}

type Int int64

func (i *Int) UnmarshalJSON(data []byte) error {
	unquotedData, err := strconv.Unquote(string(data))
	if err != nil {
		return err
	}

	if unquotedData[0] == '0' && unquotedData[1] == 'x' {
		unquotedData = unquotedData[2:]
	}

	value, err := strconv.ParseInt(unquotedData, 16, 64)
	if err != nil {
		return err
	}
	*i = Int(value)
	return nil
}

type rpcTransaction struct {
	BlockNumber      *string         `json:"blockNumber,omitempty"`
	BlockHash        *common.Hash    `json:"blockHash,omitempty"`
	From             *common.Address `json:"from,omitempty"`
	Gas              *BigInt         `json:"gas,omitempty"`
	GasPrice         *BigInt         `json:"gasPrice,omitempty"`
	Hash             *string         `json:"hash,omitempty"`
	Input            *string         `json:"input,omitempty"`
	Nonce            *BigInt         `json:"nonce,omitempty"`
	R                *string         `json:"r,omitempty"`
	S                *string         `json:"s,omitempty"`
	To               *common.Address `json:"to,omitempty"`
	TransactionIndex *Int            `json:"transactionIndex,omitempty"`
	Type             *Int            `json:"type,omitempty"`
	V                *string         `json:"v,omitempty"`
	Value            *BigInt         `json:"value,omitempty"`
}

// Hex2Bytes converts a hex string to a byte slice.
func Hex2Bytes(hexR *string) []byte {
	if hexR == nil {
		return nil
	}
	hex := *hexR
	if len(hex) == 0 {
		return nil
	}
	if len(hex)%2 != 0 {
		hex = "0x0" + hex[2:]
	}
	b, err := hexutil.Decode(hex)
	if err != nil {
		panic(err)
	}
	return b
}

// Tx return types.Transaction from rpcTransaction
func (tx *rpcTransaction) Tx() types.Transaction {
	if tx == nil {
		return nil
	}

	if tx.Type == nil {
		panic("transaction type is nil")
	}

	var legacy *types.LegacyTx
	if tx.To == nil {
		legacy = types.NewContractCreation(
			tx.Nonce.Uint64(),
			tx.Value.ToInt(),
			tx.Gas.Uint64(),
			tx.GasPrice.ToInt(),
			Hex2Bytes(tx.Input),
		)
	} else {
		legacy = types.NewTransaction(
			tx.Nonce.Uint64(),
			*tx.To,
			tx.Value.ToInt(),
			tx.Gas.Uint64(),
			tx.GasPrice.ToInt(),
			Hex2Bytes(tx.Input),
		)
	}

	if tx.V != nil && tx.R != nil && tx.S != nil {
		// parse signature raw values V, R, S from local hex strings
		legacy.V = Hex2Int256(*tx.V)
		legacy.R = Hex2Int256(*tx.R)
		legacy.S = Hex2Int256(*tx.S)
	}

	if *tx.Type == 0x0 /*legacy*/ {
		return legacy
	}

	if *tx.Type == 0x1 /*dynamic*/ {
		return &types.DynamicFeeTransaction{CommonTx: legacy.CommonTx}
	}

	if *tx.Type == 0x2 /*access list*/ {
		return &types.AccessListTx{LegacyTx: *legacy}
	}

	return nil
}

// Hex2Int256 converts a hex string to a uint256.Int.
func Hex2Int256(hex string) uint256.Int {
	b := Hex2Bytes(&hex)
	i := uint256.NewInt(0).SetBytes(b)
	if i == nil {
		panic("failed to convert hex to uint256.Int")
	}
	return *i
}

/*
{
   "blockHash": "0x0090e430b7a7429ee3d8b64d899e58c93b6560f7d8167d57fe1ad9618d82d4bf",
   "blockNumber": "0xbbaf",
   "from": "0xc759dc67e0c3816fdc56a2f5bb29920fc07e767d",
   "gas": "0x55f0",
   "gasPrice": "0xcd247221f",
   "hash": "0x69b06b4fd1d723e1baf2fce55a39d9f4532fbd0301f73db2c1790a427d14ae19",
   "input": "0x454e444d31394d3552",
   "nonce": "0x3",
   "r": "0x8a4eb65cb1afc9159b77bff7a02c0c0d605f78141ca4501d2333c43a17d82e31",
   "s": "0x3158d9fb01983b0ea2aca244af5c32a512b3bdc3cc6cd1ed3ac4a2230a6654b8",
   "to": "0x2910543af39aba0cd09dbb2d50200b3e800a63d2",
   "transactionIndex": "0x0",
   "type": "0x0",
   "v": "0x1c",
   "value": "0x56bc75e2d63100000"
   }
*/

// TransactionByHash returns the transaction with the given hash.
func (ec *Client) TransactionByHash(ctx context.Context, hash common.Hash) (tx types.Transaction, isPending bool, err error) {
	var json *rpcTransaction
	err = ec.c.CallContext(ctx, &json, "eth_getTransactionByHash", hash)
	if err != nil {
		return nil, false, err
	} else if json == nil {
		return nil, false, ethereum.NotFound
	} else if json.Tx() == nil {
		return nil, false, fmt.Errorf("server returned transaction without type")
	} else if _, r, _ := json.Tx().RawSignatureValues(); r == nil {
		return nil, false, fmt.Errorf("server returned transaction without signature")
	}
	return json.Tx(), json.BlockNumber == nil, nil
}

/*
// TransactionSender returns the sender address of the given transaction. The transaction
// must be known to the remote node and included in the blockchain at the given block and
// index. The sender is the one derived by the protocol at the time of inclusion.
//
// There is a fast-path for transactions retrieved by TransactionByHash and
// TransactionInBlock. Getting their sender address can be done without an RPC interaction.
func (ec *Client) TransactionSender(ctx context.Context, tx types.Transaction, block common.Hash, index uint) (common.Address, error) {
	// Try to load the address from the cache.
	sender, err := types.Sender(&senderFromServer{blockhash: block}, tx)
	if err == nil {
		return sender, nil
	}

	// It was not found in cache, ask the server.
	var meta struct {
		Hash common.Hash
		From common.Address
	}
	if err = ec.c.CallContext(ctx, &meta, "eth_getTransactionByBlockHashAndIndex", block, hexutil.Uint64(index)); err != nil {
		return common.Address{}, err
	}
	if meta.Hash == (common.Hash{}) || meta.Hash != tx.Hash() {
		return common.Address{}, errors.New("wrong inclusion block/index")
	}
	return meta.From, nil
}
*/

// TransactionCount returns the total number of transactions in the given block.
func (ec *Client) TransactionCount(ctx context.Context, blockHash common.Hash) (uint, error) {
	var num hexutil.Uint
	err := ec.c.CallContext(ctx, &num, "eth_getBlockTransactionCountByHash", blockHash)
	return uint(num), err
}

// TransactionInBlock returns a single transaction at index in the given block.
func (ec *Client) TransactionInBlock(ctx context.Context, blockHash common.Hash, index uint) (types.Transaction, error) {
	var json *rpcTransaction
	err := ec.c.CallContext(ctx, &json, "eth_getTransactionByBlockHashAndIndex", blockHash, hexutil.Uint64(index))
	if err != nil {
		return nil, err
	}
	if json == nil {
		return nil, ethereum.NotFound
	} else if _, r, _ := json.Tx().RawSignatureValues(); r == nil {
		return nil, fmt.Errorf("server returned transaction without signature")
	}
	return json.Tx(), err
}

// TransactionReceipt returns the receipt of a transaction by transaction hash.
// Note that the receipt is not available for pending transactions.
func (ec *Client) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	var r *types.Receipt
	err := ec.c.CallContext(ctx, &r, "eth_getTransactionReceipt", txHash)
	if err == nil {
		if r == nil {
			return nil, ethereum.NotFound
		}
	}
	return r, err
}

// SyncProgress retrieves the current progress of the sync algorithm. If there's
// no sync currently running, it returns nil.
func (ec *Client) SyncProgress(ctx context.Context) (*ethereum.SyncProgress, error) {
	var raw json.RawMessage
	if err := ec.c.CallContext(ctx, &raw, "eth_syncing"); err != nil {
		return nil, err
	}
	// Handle the possible response types
	var syncing bool
	if err := json.Unmarshal(raw, &syncing); err == nil {
		return nil, nil // Not syncing (always false)
	}
	var p *rpcProgress
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	return p.toSyncProgress(), nil
}

// SubscribeNewHead subscribes to notifications about the current blockchain head
// on the given channel.
func (ec *Client) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	sub, err := ec.c.EthSubscribe(ctx, ch, "newHeads")
	if err != nil {
		// Defensively prefer returning nil interface explicitly on error-path, instead
		// of letting default golang behavior wrap it with non-nil interface that stores
		// nil concrete type value.
		return nil, err
	}
	return sub, nil
}

// State Access

// NetworkID returns the network ID for this client.
func (ec *Client) NetworkID(ctx context.Context) (*big.Int, error) {
	version := new(big.Int)
	var ver string
	if err := ec.c.CallContext(ctx, &ver, "net_version"); err != nil {
		return nil, err
	}
	if _, ok := version.SetString(ver, 10); !ok {
		return nil, fmt.Errorf("invalid net_version result %q", ver)
	}
	return version, nil
}

func b2i(s *big.Int) *uint256.Int {
	iii := &uint256.Int{}
	iii.SetBytes(s.Bytes())
	return iii
}

// BalanceAt returns the wei balance of the given account.
// The block number can be nil, in which case the balance is taken from the latest known block.
func (ec *Client) BalanceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (*uint256.Int, error) {
	var result hexutil.Big
	err := ec.c.CallContext(ctx, &result, "eth_getBalance", account, toBlockNumArg(blockNumber))
	return b2i((*big.Int)(&result)), err
}

// StorageAt returns the value of key in the contract storage of the given account.
// The block number can be nil, in which case the value is taken from the latest known block.
func (ec *Client) StorageAt(ctx context.Context, account common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error) {
	var result hexutil.Bytes
	err := ec.c.CallContext(ctx, &result, "eth_getStorageAt", account, key, toBlockNumArg(blockNumber))
	return result, err
}

// CodeAt returns the contract code of the given account.
// The block number can be nil, in which case the code is taken from the latest known block.
func (ec *Client) CodeAt(ctx context.Context, account common.Address, blockNumber *big.Int) ([]byte, error) {
	var result hexutil.Bytes
	err := ec.c.CallContext(ctx, &result, "eth_getCode", account, toBlockNumArg(blockNumber))
	return result, err
}

// NonceAt returns the account nonce of the given account.
// The block number can be nil, in which case the nonce is taken from the latest known block.
func (ec *Client) NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error) {
	var result hexutil.Uint64
	err := ec.c.CallContext(ctx, &result, "eth_getTransactionCount", account, toBlockNumArg(blockNumber))
	return uint64(result), err
}

// Filters

// FilterLogs executes a filter query.
func (ec *Client) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	var result []types.Log
	arg, err := toFilterArg(q)
	if err != nil {
		return nil, err
	}
	err = ec.c.CallContext(ctx, &result, "eth_getLogs", arg)
	return result, err
}

// SubscribeFilterLogs subscribes to the results of a streaming filter query.
func (ec *Client) SubscribeFilterLogs(ctx context.Context, q ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	arg, err := toFilterArg(q)
	if err != nil {
		return nil, err
	}
	sub, err := ec.c.EthSubscribe(ctx, ch, "logs", arg)
	if err != nil {
		// Defensively prefer returning nil interface explicitly on error-path, instead
		// of letting default golang behavior wrap it with non-nil interface that stores
		// nil concrete type value.
		return nil, err
	}
	return sub, nil
}

func toFilterArg(q ethereum.FilterQuery) (interface{}, error) {
	arg := map[string]interface{}{
		"address": q.Addresses,
		"topics":  q.Topics,
	}
	if q.BlockHash != nil {
		arg["blockHash"] = *q.BlockHash
		if q.FromBlock != nil || q.ToBlock != nil {
			return nil, fmt.Errorf("cannot specify both BlockHash and FromBlock/ToBlock")
		}
	} else {
		if q.FromBlock == nil {
			arg["fromBlock"] = "0x0"
		} else {
			arg["fromBlock"] = toBlockNumArg(q.FromBlock)
		}
		arg["toBlock"] = toBlockNumArg(q.ToBlock)
	}
	return arg, nil
}

// Pending State

// PendingBalanceAt returns the wei balance of the given account in the pending state.
func (ec *Client) PendingBalanceAt(ctx context.Context, account common.Address) (*big.Int, error) {
	var result hexutil.Big
	err := ec.c.CallContext(ctx, &result, "eth_getBalance", account, "pending")
	return (*big.Int)(&result), err
}

// PendingStorageAt returns the value of key in the contract storage of the given account in the pending state.
func (ec *Client) PendingStorageAt(ctx context.Context, account common.Address, key common.Hash) ([]byte, error) {
	var result hexutil.Bytes
	err := ec.c.CallContext(ctx, &result, "eth_getStorageAt", account, key, "pending")
	return result, err
}

// PendingCodeAt returns the contract code of the given account in the pending state.
func (ec *Client) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	var result hexutil.Bytes
	err := ec.c.CallContext(ctx, &result, "eth_getCode", account, "pending")
	return result, err
}

// PendingNonceAt returns the account nonce of the given account in the pending state.
// This is the nonce that should be used for the next transaction.
func (ec *Client) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	var result hexutil.Uint64
	err := ec.c.CallContext(ctx, &result, "eth_getTransactionCount", account, "pending")
	return uint64(result), err
}

// PendingTransactionCount returns the total number of transactions in the pending state.
func (ec *Client) PendingTransactionCount(ctx context.Context) (uint, error) {
	var num hexutil.Uint
	err := ec.c.CallContext(ctx, &num, "eth_getBlockTransactionCountByNumber", "pending")
	return uint(num), err
}

// Contract Calling

// CallContract executes a message call transaction, which is directly executed in the VM
// of the node, but never mined into the blockchain.
//
// blockNumber selects the block height at which the call runs. It can be nil, in which
// case the code is taken from the latest known block. Note that state from very old
// blocks might not be available.
func (ec *Client) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	var hex hexutil.Bytes
	err := ec.c.CallContext(ctx, &hex, "eth_call", toCallArg(msg), toBlockNumArg(blockNumber))
	if err != nil {
		return nil, err
	}
	return hex, nil
}

// CallContractAtHash is almost the same as CallContract except that it selects
// the block by block hash instead of block height.
func (ec *Client) CallContractAtHash(ctx context.Context, msg ethereum.CallMsg, blockHash common.Hash) ([]byte, error) {
	var hex hexutil.Bytes
	err := ec.c.CallContext(ctx, &hex, "eth_call", toCallArg(msg), rpc.BlockNumberOrHashWithHash(blockHash, false))
	if err != nil {
		return nil, err
	}
	return hex, nil
}

// PendingCallContract executes a message call transaction using the EVM.
// The state seen by the contract call is the pending state.
func (ec *Client) PendingCallContract(ctx context.Context, msg ethereum.CallMsg) ([]byte, error) {
	var hex hexutil.Bytes
	err := ec.c.CallContext(ctx, &hex, "eth_call", toCallArg(msg), "pending")
	if err != nil {
		return nil, err
	}
	return hex, nil
}

// SuggestGasPrice retrieves the currently suggested gas price to allow a timely
// execution of a transaction.
func (ec *Client) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	var hex hexutil.Big
	if err := ec.c.CallContext(ctx, &hex, "eth_gasPrice"); err != nil {
		return nil, err
	}
	return (*big.Int)(&hex), nil
}

// SuggestGasTipCap retrieves the currently suggested gas tip cap after 1559 to
// allow a timely execution of a transaction.
func (ec *Client) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	var hex hexutil.Big
	if err := ec.c.CallContext(ctx, &hex, "eth_maxPriorityFeePerGas"); err != nil {
		return nil, err
	}
	return (*big.Int)(&hex), nil
}

type feeHistoryResultMarshaling struct {
	OldestBlock  *hexutil.Big     `json:"oldestBlock"`
	Reward       [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee      []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio []float64        `json:"gasUsedRatio"`
}

/*
// FeeHistory retrieves the fee market history.
func (ec *Client) FeeHistory(ctx context.Context, blockCount uint64, lastBlock *big.Int, rewardPercentiles []float64) (*ethereum.FeeHistory, error) {
	var res feeHistoryResultMarshaling
	if err := ec.c.CallContext(ctx, &res, "eth_feeHistory", hexutil.Uint(blockCount), toBlockNumArg(lastBlock), rewardPercentiles); err != nil {
		return nil, err
	}
	reward := make([][]*big.Int, len(res.Reward))
	for i, r := range res.Reward {
		reward[i] = make([]*big.Int, len(r))
		for j, r := range r {
			reward[i][j] = (*big.Int)(r)
		}
	}
	baseFee := make([]*big.Int, len(res.BaseFee))
	for i, b := range res.BaseFee {
		baseFee[i] = (*big.Int)(b)
	}
	return &ethereum.FeeHistory{
		OldestBlock:  (*big.Int)(res.OldestBlock),
		Reward:       reward,
		BaseFee:      baseFee,
		GasUsedRatio: res.GasUsedRatio,
	}, nil
}
*/

// EstimateGas tries to estimate the gas needed to execute a specific transaction based on
// the current pending state of the backend blockchain. There is no guarantee that this is
// the true gas limit requirement as other transactions may be added or removed by miners,
// but it should provide a basis for setting a reasonable default.
func (ec *Client) EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
	var hex hexutil.Uint64
	err := ec.c.CallContext(ctx, &hex, "eth_estimateGas", toCallArg(msg))
	if err != nil {
		return 0, err
	}
	return uint64(hex), nil
}

// SendTransaction injects a signed transaction into the pending pool for execution.
//
// If the transaction was a contract creation use the TransactionReceipt method to get the
// contract address after the transaction has been mined.
func (ec *Client) SendTransaction(ctx context.Context, tx types.Transaction) error {
	var data []byte
	writer := bytes.NewBuffer(data)
	err := tx.MarshalBinary(writer)
	if err != nil {
		return err
	}
	return ec.c.CallContext(ctx, nil, "eth_sendRawTransaction", hexutil.Encode(data))
}

func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	pending := big.NewInt(-1)
	if number.Cmp(pending) == 0 {
		return "pending"
	}
	finalized := big.NewInt(int64(rpc.FinalizedBlockNumber))
	if number.Cmp(finalized) == 0 {
		return "finalized"
	}
	safe := big.NewInt(int64(rpc.SafeBlockNumber))
	if number.Cmp(safe) == 0 {
		return "safe"
	}
	return hexutil.EncodeBig(number)
}

func toCallArg(msg ethereum.CallMsg) interface{} {
	arg := map[string]interface{}{
		"from": msg.From,
		"to":   msg.To,
	}
	if len(msg.Data) > 0 {
		arg["data"] = hexutil.Bytes(msg.Data)
	}
	if msg.Value != nil {
		arg["value"] = (*hexutil.Big)(msg.Value.ToBig())
	}
	if msg.Gas != 0 {
		arg["gas"] = hexutil.Uint64(msg.Gas)
	}
	if msg.GasPrice != nil {
		arg["gasPrice"] = (*hexutil.Big)(msg.GasPrice.ToBig())
	}
	return arg
}

// rpcProgress is a copy of SyncProgress with hex-encoded fields.
type rpcProgress struct {
	StartingBlock hexutil.Uint64
	CurrentBlock  hexutil.Uint64
	HighestBlock  hexutil.Uint64

	PulledStates hexutil.Uint64
	KnownStates  hexutil.Uint64

	SyncedAccounts      hexutil.Uint64
	SyncedAccountBytes  hexutil.Uint64
	SyncedBytecodes     hexutil.Uint64
	SyncedBytecodeBytes hexutil.Uint64
	SyncedStorage       hexutil.Uint64
	SyncedStorageBytes  hexutil.Uint64
	HealedTrienodes     hexutil.Uint64
	HealedTrienodeBytes hexutil.Uint64
	HealedBytecodes     hexutil.Uint64
	HealedBytecodeBytes hexutil.Uint64
	HealingTrienodes    hexutil.Uint64
	HealingBytecode     hexutil.Uint64
}

func (p *rpcProgress) toSyncProgress() *ethereum.SyncProgress {
	if p == nil {
		return nil
	}
	return &ethereum.SyncProgress{
		StartingBlock: uint64(p.StartingBlock),
		CurrentBlock:  uint64(p.CurrentBlock),
		HighestBlock:  uint64(p.HighestBlock),
		PulledStates:  uint64(p.PulledStates),
		KnownStates:   uint64(p.KnownStates),
	}
}
