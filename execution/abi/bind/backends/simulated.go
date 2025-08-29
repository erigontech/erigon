// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package backends

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/ethash"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/event"
	"github.com/erigontech/erigon/turbo/services"
)

// This nil assignment ensures at compile time that SimulatedBackend implements bind.ContractBackend.
var _ bind.ContractBackend = (*SimulatedBackend)(nil)

var (
	errBlockNumberUnsupported  = errors.New("simulatedBackend cannot access blocks other than the latest block")
	errBlockDoesNotExist       = errors.New("block does not exist in blockchain")
	errTransactionDoesNotExist = errors.New("transaction does not exist")
)

// SimulatedBackend implements bind.ContractBackend, simulating a blockchain in
// the background. Its main purpose is to allow for easy testing of contract bindings.
// Simulated backend implements the following interfaces:
// ChainReader, ChainStateReader, ContractBackend, ContractCaller, ContractFilterer, ContractTransactor,
// DeployBackend, GasEstimator, GasPricer, LogFilterer, PendingContractCaller, TransactionReader, and TransactionSender
type SimulatedBackend struct {
	m         *mock.MockSentry
	getHeader func(hash common.Hash, number uint64) (*types.Header, error)

	mu              sync.Mutex
	prependBlock    *types.Block
	pendingReceipts types.Receipts
	pendingHeader   *types.Header
	gasPool         *core.GasPool
	pendingBlock    *types.Block // Currently pending block that will be imported on request
	pendingReader   state.StateReader
	pendingReaderTx kv.TemporalTx
	pendingState    *state.IntraBlockState // Currently pending state that will be the active on request

	rmLogsFeed event.Feed
	chainFeed  event.Feed
	logsFeed   event.Feed
}

func NewSimulatedBackendWithConfig(t *testing.T, alloc types.GenesisAlloc, config *chain.Config, gasLimit uint64) *SimulatedBackend {
	genesis := types.Genesis{Config: config, GasLimit: gasLimit, Alloc: alloc}
	engine := ethash.NewFaker()
	//SimulatedBackend - it's remote blockchain node. This is reason why it has own `MockSentry` and own `DB` (even if external unit-test have one already)
	m := mock.MockWithGenesisEngine(t, &genesis, engine, false)

	backend := &SimulatedBackend{
		m:            m,
		prependBlock: m.Genesis,
		getHeader: func(hash common.Hash, number uint64) (h *types.Header, err error) {
			err = m.DB.View(context.Background(), func(tx kv.Tx) error {
				h, err = m.BlockReader.Header(context.Background(), tx, hash, number)
				return nil
			})
			return h, err
		},
	}
	if t != nil {
		t.Cleanup(backend.Close)
	}
	backend.emptyPendingBlock()
	return backend
}

// NewSimulatedBackend A simulated backend always uses chainID 1337.
func NewSimulatedBackend(t *testing.T, alloc types.GenesisAlloc, gasLimit uint64) *SimulatedBackend {
	b := NewSimulatedBackendWithConfig(t, alloc, chain.TestChainConfig, gasLimit)
	return b
}

func (b *SimulatedBackend) DB() kv.TemporalRwDB                   { return b.m.DB }
func (b *SimulatedBackend) HistoryV3() bool                       { return b.m.HistoryV3 }
func (b *SimulatedBackend) Engine() consensus.Engine              { return b.m.Engine }
func (b *SimulatedBackend) BlockReader() services.FullBlockReader { return b.m.BlockReader }

// Close terminates the underlying blockchain's update loop.
func (b *SimulatedBackend) Close() {
	if b.pendingReaderTx != nil {
		b.pendingReaderTx.Rollback()
	}
	b.m.Close()
}

// Commit imports all the pending transactions as a single block and starts a
// fresh new state.
func (b *SimulatedBackend) Commit() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.m.InsertChain(&core.ChainPack{
		Headers:  []*types.Header{b.pendingHeader},
		Blocks:   []*types.Block{b.pendingBlock},
		TopBlock: b.pendingBlock,
	}); err != nil {
		panic(err)
	}
	//nolint:prealloc
	var allLogs []*types.Log
	for _, r := range b.pendingReceipts {
		allLogs = append(allLogs, r.Logs...)
	}
	b.logsFeed.Send(allLogs)
	b.prependBlock = b.pendingBlock
	b.emptyPendingBlock()
}

// Rollback aborts all pending transactions, reverting to the last committed state.
func (b *SimulatedBackend) Rollback() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.emptyPendingBlock()
}

func (b *SimulatedBackend) emptyPendingBlock() {
	blockChain, _ := core.GenerateChain(b.m.ChainConfig, b.prependBlock, b.m.Engine, b.m.DB, 1, func(int, *core.BlockGen) {})
	b.pendingBlock = blockChain.Blocks[0]
	b.pendingReceipts = blockChain.Receipts[0]
	b.pendingHeader = blockChain.Headers[0]
	b.gasPool = new(core.GasPool).AddGas(b.pendingHeader.GasLimit).AddBlobGas(b.m.ChainConfig.GetMaxBlobGasPerBlock(b.pendingHeader.Time))
	if b.pendingReaderTx != nil {
		b.pendingReaderTx.Rollback()
	}
	tx, err := b.m.DB.BeginTemporalRo(context.Background()) //nolint:gocritic
	if err != nil {
		panic(err)
	}
	b.pendingReaderTx = tx
	b.pendingReader = b.m.NewStateReader(b.pendingReaderTx)
	b.pendingState = state.New(b.pendingReader)
}

// stateByBlockNumber retrieves a state by a given blocknumber.
func (b *SimulatedBackend) stateByBlockNumber(db kv.TemporalTx, blockNumber *big.Int) *state.IntraBlockState {
	if blockNumber == nil || blockNumber.Cmp(b.pendingBlock.Number()) == 0 {
		return state.New(b.m.NewHistoryStateReader(b.pendingBlock.NumberU64()+1, db))
	}
	return state.New(b.m.NewHistoryStateReader(blockNumber.Uint64()+1, db))
}

// CodeAt returns the code associated with a certain account in the blockchain.
func (b *SimulatedBackend) CodeAt(ctx context.Context, contract common.Address, blockNumber *big.Int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, err := b.m.DB.BeginTemporalRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	stateDB := b.stateByBlockNumber(tx, blockNumber)
	return stateDB.GetCode(contract)
}

// BalanceAt returns the wei balance of a certain account in the blockchain.
func (b *SimulatedBackend) BalanceAt(ctx context.Context, contract common.Address, blockNumber *big.Int) (*uint256.Int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, err := b.m.DB.BeginTemporalRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	stateDB := b.stateByBlockNumber(tx, blockNumber)
	balance, err := stateDB.GetBalance(contract)
	return &balance, err
}

// NonceAt returns the nonce of a certain account in the blockchain.
func (b *SimulatedBackend) NonceAt(ctx context.Context, contract common.Address, blockNumber *big.Int) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, err := b.m.DB.BeginTemporalRo(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stateDB := b.stateByBlockNumber(tx, blockNumber)
	return stateDB.GetNonce(contract)
}

// StorageAt returns the value of key in the storage of an account in the blockchain.
func (b *SimulatedBackend) StorageAt(ctx context.Context, contract common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, err := b.m.DB.BeginTemporalRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stateDB := b.stateByBlockNumber(tx, blockNumber)
	var val uint256.Int
	stateDB.GetState(contract, key, &val)
	return val.Bytes(), nil
}

// TransactionReceipt returns the receipt of a transaction.
func (b *SimulatedBackend) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	tx, err := b.m.DB.BeginTemporalRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the context of the receipt based on the transaction hash
	blockNumber, _, err := rawdb.ReadTxLookupEntry(tx, txHash)
	if err != nil {
		return nil, err
	}
	if blockNumber == nil {
		return nil, nil
	}
	block, err := b.BlockReader().BlockByNumber(b.m.Ctx, tx, *blockNumber)
	if err != nil {
		return nil, err
	}

	// Read all the receipts from the block and return the one with the matching hash
	receipts, err := b.m.ReceiptsReader.GetReceipts(ctx, b.m.ChainConfig, tx, block)
	if err != nil {
		panic(err)
	}
	for _, receipt := range receipts {
		if receipt.TxHash == txHash {
			return receipt, nil
		}
	}
	return nil, nil
}

// TransactionByHash checks the pool of pending transactions in addition to the
// blockchain. The isPending return value indicates whether the transaction has been
// mined yet. Note that the transaction may not be part of the canonical chain even if
// it's not pending.
func (b *SimulatedBackend) TransactionByHash(ctx context.Context, txHash common.Hash) (types.Transaction, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	tx, err := b.m.DB.BeginRo(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	txn := b.pendingBlock.Transaction(txHash)
	if txn != nil {
		return txn, true, nil
	}
	blockNumber, _, ok, err := b.BlockReader().TxnLookup(ctx, tx, txHash)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, ethereum.NotFound
	}
	blockHash, ok, err := b.BlockReader().CanonicalHash(ctx, tx, blockNumber)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, ethereum.NotFound
	}
	body, err := b.BlockReader().BodyWithTransactions(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, false, err
	}
	if body == nil {
		return nil, false, ethereum.NotFound
	}
	for _, txn = range body.Transactions {
		if txn.Hash() == txHash {
			return txn, false, nil
		}
	}
	return nil, false, ethereum.NotFound
}

// BlockByHash retrieves a block based on the block hash.
func (b *SimulatedBackend) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if hash == b.pendingBlock.Hash() {
		return b.pendingBlock, nil
	}
	tx, err := b.m.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, err := b.BlockReader().BlockByHash(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if block != nil {
		return block, nil
	}

	return nil, errBlockDoesNotExist
}

// BlockByNumber retrieves a block from the database by number, caching it
// (associated with its hash) if found.
func (b *SimulatedBackend) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.blockByNumberNoLock(ctx, number)
}

// blockByNumberNoLock retrieves a block from the database by number, caching it
// (associated with its hash) if found without Lock.
func (b *SimulatedBackend) blockByNumberNoLock(ctx context.Context, number *big.Int) (*types.Block, error) {
	if number == nil || number.Cmp(b.prependBlock.Number()) == 0 {
		return b.prependBlock, nil
	}

	tx, err := b.m.DB.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, err := b.BlockReader().BlockByNumber(ctx, tx, number.Uint64())
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, errBlockDoesNotExist
	}

	return block, nil
}

// HeaderByHash returns a block header from the current canonical chain.
func (b *SimulatedBackend) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if hash == b.pendingBlock.Hash() {
		return b.pendingBlock.Header(), nil
	}
	tx, err := b.m.DB.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	number := rawdb.ReadHeaderNumber(tx, hash)
	if number == nil {
		return nil, errBlockDoesNotExist
	}
	header, err := b.BlockReader().Header(ctx, tx, hash, *number)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, errBlockDoesNotExist
	}

	return header, nil
}

// HeaderByNumber returns a block header from the current canonical chain. If number is
// nil, the latest known header is returned.
func (b *SimulatedBackend) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tx, err := b.m.DB.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if number == nil || number.Cmp(b.prependBlock.Number()) == 0 {
		return b.prependBlock.Header(), nil
	}
	header, err := b.BlockReader().HeaderByNumber(ctx, tx, number.Uint64())
	if err != nil {
		return nil, err
	}
	return header, nil
}

// TransactionCount returns the number of transactions in a given block.
func (b *SimulatedBackend) TransactionCount(ctx context.Context, blockHash common.Hash) (uint, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if blockHash == b.pendingBlock.Hash() {
		return uint(b.pendingBlock.Transactions().Len()), nil
	}
	tx, err := b.m.DB.BeginRo(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	blockNum := rawdb.ReadHeaderNumber(tx, blockHash)
	if blockNum == nil {
		return 0, nil
	}
	block, _, err := b.BlockReader().BlockWithSenders(ctx, tx, blockHash, *blockNum)
	if err != nil {
		return 0, err
	}
	if block == nil {
		return uint(0), errBlockDoesNotExist
	}

	return uint(block.Transactions().Len()), nil
}

// TransactionInBlock returns the transaction for a specific block at a specific index.
func (b *SimulatedBackend) TransactionInBlock(ctx context.Context, blockHash common.Hash, index uint) (types.Transaction, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if blockHash == b.pendingBlock.Hash() {
		transactions := b.pendingBlock.Transactions()
		if uint(len(transactions)) < index+1 {
			return nil, errTransactionDoesNotExist
		}

		return transactions[index], nil
	}
	tx, err := b.m.DB.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum := rawdb.ReadHeaderNumber(tx, blockHash)
	if blockNum == nil {
		return nil, nil
	}
	block, _, err := b.BlockReader().BlockWithSenders(ctx, tx, blockHash, *blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, errBlockDoesNotExist
	}

	transactions := block.Transactions()
	if uint(len(transactions)) < index+1 {
		return nil, errTransactionDoesNotExist
	}

	return transactions[index], nil
}

// PendingCodeAt returns the code associated with an account in the pending state.
func (b *SimulatedBackend) PendingCodeAt(ctx context.Context, contract common.Address) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.pendingState.GetCode(contract)
}

func newRevertError(result *evmtypes.ExecutionResult) *revertError {
	reason, errUnpack := abi.UnpackRevert(result.Revert())
	err := errors.New("execution reverted")
	if errUnpack == nil {
		err = fmt.Errorf("execution reverted: %v", reason)
	}
	return &revertError{
		error:  err,
		reason: hexutil.Encode(result.Revert()),
	}
}

// revertError is an API error that encompasses an EVM revert with JSON error
// code and a binary data blob.
type revertError struct {
	error
	reason string // revert reason hex encoded
}

// ErrorCode returns the JSON error code for a revert.
// See: https://eips.ethereum.org/EIPS/eip-1474#json-rpc
func (e *revertError) ErrorCode() int {
	return 3
}

// ErrorData returns the hex encoded revert reason.
func (e *revertError) ErrorData() interface{} {
	return e.reason
}

// CallContract executes a contract call.
func (b *SimulatedBackend) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if blockNumber != nil && blockNumber.Cmp(b.pendingBlock.Number()) != 0 {
		return nil, errBlockNumberUnsupported
	}
	var res *evmtypes.ExecutionResult
	if err := b.m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) (err error) {
		s := state.New(b.m.NewStateReader(tx))
		res, err = b.callContract(ctx, call, b.pendingBlock, s)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// If the result contains a revert reason, try to unpack and return it.
	if len(res.Revert()) > 0 {
		return nil, newRevertError(res)
	}
	return res.Return(), res.Err
}

// PendingCallContract executes a contract call on the pending state.
func (b *SimulatedBackend) PendingCallContract(ctx context.Context, call ethereum.CallMsg) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.pendingState.RevertToSnapshot(b.pendingState.Snapshot(), nil)

	res, err := b.callContract(ctx, call, b.pendingBlock, b.pendingState)
	if err != nil {
		return nil, err
	}
	// If the result contains a revert reason, try to unpack and return it.
	if len(res.Revert()) > 0 {
		return nil, newRevertError(res)
	}
	return res.Return(), res.Err
}

// PendingNonceAt implements PendingStateReader.PendingNonceAt, retrieving
// the nonce currently pending for the account.
func (b *SimulatedBackend) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.pendingState.GetNonce(account)
}

// SuggestGasPrice implements ContractTransactor.SuggestGasPrice. Since the simulated
// chain doesn't have miners, we just return a gas price of 1 for any call.
func (b *SimulatedBackend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return big.NewInt(1), nil
}

// EstimateGas executes the requested code against the currently pending block/state and
// returns the used amount of gas.
func (b *SimulatedBackend) EstimateGas(ctx context.Context, call ethereum.CallMsg) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Determine the lowest and highest possible gas limits to binary search in between
	var (
		lo     = params.TxGas - 1
		hi     uint64
		gasCap uint64
	)
	if call.Gas >= params.TxGas {
		hi = call.Gas
	} else {
		hi = b.pendingBlock.GasLimit()
	}
	// Recap the highest gas allowance with account's balance.
	if call.GasPrice != nil && !call.GasPrice.IsZero() {
		balance, err := b.pendingState.GetBalance(call.From) // from can't be nil
		if err != nil {
			return 0, err
		}
		available := balance.ToBig()
		if call.Value != nil {
			if call.Value.ToBig().Cmp(available) >= 0 {
				return 0, errors.New("insufficient funds for transfer")
			}
			available.Sub(available, call.Value.ToBig())
		}
		allowance := new(big.Int).Div(available, call.GasPrice.ToBig())
		if allowance.IsUint64() && hi > allowance.Uint64() {
			transfer := call.Value
			if transfer == nil {
				transfer = new(uint256.Int)
			}
			log.Warn("Gas estimation capped by limited funds", "original", hi, "balance", balance,
				"sent", transfer, "gasprice", call.GasPrice, "fundable", allowance)
			hi = allowance.Uint64()
		}
	}
	gasCap = hi
	b.pendingState.SetTxContext(b.pendingBlock.NumberU64(), len(b.pendingBlock.Transactions()))

	// Create a helper to check if a gas allowance results in an executable transaction
	executable := func(gas uint64) (bool, *evmtypes.ExecutionResult, error) {
		call.Gas = gas

		snapshot := b.pendingState.Snapshot()
		res, err := b.callContract(ctx, call, b.pendingBlock, b.pendingState)
		b.pendingState.RevertToSnapshot(snapshot, nil)

		if err != nil {
			if errors.Is(err, core.ErrIntrinsicGas) {
				return true, nil, nil // Special case, raise gas limit
			}
			return true, nil, err // Bail out
		}
		return res.Failed(), res, nil
	}
	// Execute the binary search and hone in on an executable gas limit
	for lo+1 < hi {
		mid := (hi + lo) / 2
		failed, _, err := executable(mid)

		// If the error is not nil(consensus error), it means the provided message
		// call or transaction will never be accepted no matter how much gas it is
		// assigned. Return the error directly, don't struggle any more
		if err != nil {
			return 0, err
		}
		if failed {
			lo = mid
		} else {
			hi = mid
		}
	}
	// Reject the transaction as invalid if it still fails at the highest allowance
	if hi == gasCap {
		failed, result, err := executable(hi)
		if err != nil {
			return 0, err
		}
		if failed {
			if result != nil && result.Err != vm.ErrOutOfGas {
				if len(result.Revert()) > 0 {
					return 0, newRevertError(result)
				}
				return 0, result.Err
			}
			// Otherwise, the specified gas cap is too low
			return 0, fmt.Errorf("gas required exceeds allowance (%d)", gasCap)
		}
	}
	return hi, nil
}

// callContract implements common code between normal and pending contract calls.
// state is modified during execution, make sure to copy it if necessary.
func (b *SimulatedBackend) callContract(_ context.Context, call ethereum.CallMsg, block *types.Block, statedb *state.IntraBlockState) (*evmtypes.ExecutionResult, error) {
	const baseFeeUpperLimit = 880000000
	// Ensure message is initialized properly.
	if call.GasPrice == nil {
		call.GasPrice = u256.Num1
	}
	if call.FeeCap == nil {
		call.FeeCap = uint256.NewInt(baseFeeUpperLimit)
	}
	if call.TipCap == nil {
		call.TipCap = uint256.NewInt(baseFeeUpperLimit)
	}
	if call.Gas == 0 {
		call.Gas = 50000000
	}
	if call.Value == nil {
		call.Value = new(uint256.Int)
	}
	// Set infinite balance to the fake caller account.
	from, err := statedb.GetOrNewStateObject(call.From)
	if err != nil {
		return nil, err
	}
	from.SetBalance(*uint256.NewInt(0).SetAllOne(), tracing.BalanceChangeUnspecified)
	// Execute the call.
	msg := callMsg{call}

	txContext := core.NewEVMTxContext(msg)
	header := block.Header()
	evmContext := core.NewEVMBlockContext(header, core.GetHashFn(header, b.getHeader), b.m.Engine, nil, b.m.ChainConfig)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmEnv := vm.NewEVM(evmContext, txContext, statedb, b.m.ChainConfig, vm.Config{})
	gasPool := new(core.GasPool).AddGas(math.MaxUint64).AddBlobGas(math.MaxUint64)

	return core.NewStateTransition(vmEnv, msg, gasPool).TransitionDb(true /* refunds */, false /* gasBailout */)
}

// SendTransaction updates the pending block to include the given transaction.
// It panics if the transaction is invalid.
func (b *SimulatedBackend) SendTransaction(ctx context.Context, txn types.Transaction) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check transaction validity.
	signer := types.MakeSigner(b.m.ChainConfig, b.pendingBlock.NumberU64(), b.pendingBlock.Time())
	sender, senderErr := txn.Sender(*signer)
	if senderErr != nil {
		return fmt.Errorf("invalid transaction: %w", senderErr)
	}
	nonce, err := b.pendingState.GetNonce(sender)
	if err != nil {
		return err
	}
	if txn.GetNonce() != nonce {
		return fmt.Errorf("invalid transaction nonce: got %d, want %d", txn.GetNonce(), nonce)
	}

	b.pendingState.SetTxContext(b.pendingBlock.NumberU64(), len(b.pendingBlock.Transactions()))
	//fmt.Printf("==== Start producing block %d, header: %d\n", b.pendingBlock.NumberU64(), b.pendingHeader.Number.Uint64())
	if _, _, err := core.ApplyTransaction(
		b.m.ChainConfig, core.GetHashFn(b.pendingHeader, b.getHeader), b.m.Engine,
		&b.pendingHeader.Coinbase, b.gasPool,
		b.pendingState, state.NewNoopWriter(),
		b.pendingHeader, txn,
		&b.pendingHeader.GasUsed, b.pendingHeader.BlobGasUsed,
		vm.Config{}); err != nil {
		return err
	}
	//fmt.Printf("==== Start producing block %d\n", (b.prependBlock.NumberU64() + 1))
	chain, err := core.GenerateChain(b.m.ChainConfig, b.prependBlock, b.m.Engine, b.m.DB, 1, func(number int, block *core.BlockGen) {
		for _, txn := range b.pendingBlock.Transactions() {
			block.AddTxWithChain(b.getHeader, b.m.Engine, txn)
		}
		block.AddTxWithChain(b.getHeader, b.m.Engine, txn)
	})
	if err != nil {
		return err
	}
	//fmt.Printf("==== End producing block %d\n", b.pendingBlock.NumberU64())
	b.pendingBlock = chain.Blocks[0]
	b.pendingReceipts = chain.Receipts[0]
	b.pendingHeader = chain.Headers[0]
	return nil
}

// FilterLogs executes a log filter operation, blocking during execution and
// returning all the results in one batch.
//
// TODO(karalabe): Deprecate when the subscription one can return past data too.
func (b *SimulatedBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return nil, nil
}

// SubscribeFilterLogs creates a background log filtering operation, returning a
// subscription immediately, which can be used to stream the found events.
func (b *SimulatedBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return nil, nil
}

// SubscribeNewHead returns an event subscription for a new header.
func (b *SimulatedBackend) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	return nil, nil
}

// AdjustTime adds a time shift to the simulated clock.
// It can only be called on empty blocks.
func (b *SimulatedBackend) AdjustTime(adjustment time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pendingBlock.Transactions()) != 0 {
		return errors.New("could not adjust time on non-empty block")
	}

	chain, err := core.GenerateChain(b.m.ChainConfig, b.prependBlock, b.m.Engine, b.m.DB, 1, func(number int, block *core.BlockGen) {
		for _, txn := range b.pendingBlock.Transactions() {
			block.AddTxWithChain(b.getHeader, b.m.Engine, txn)
		}
		block.OffsetTime(int64(adjustment.Seconds()))
	})
	if err != nil {
		return err
	}
	b.pendingBlock = chain.Blocks[0]
	b.pendingHeader = chain.Headers[0]

	return nil
}

// callMsg implements core.Message to allow passing it as a transaction simulator.
type callMsg struct {
	ethereum.CallMsg
}

func (m callMsg) From() common.Address                  { return m.CallMsg.From }
func (m callMsg) Nonce() uint64                         { return 0 }
func (m callMsg) CheckNonce() bool                      { return false }
func (m callMsg) To() *common.Address                   { return m.CallMsg.To }
func (m callMsg) GasPrice() *uint256.Int                { return m.CallMsg.GasPrice }
func (m callMsg) FeeCap() *uint256.Int                  { return m.CallMsg.FeeCap }
func (m callMsg) TipCap() *uint256.Int                  { return m.CallMsg.TipCap }
func (m callMsg) Gas() uint64                           { return m.CallMsg.Gas }
func (m callMsg) Value() *uint256.Int                   { return m.CallMsg.Value }
func (m callMsg) Data() []byte                          { return m.CallMsg.Data }
func (m callMsg) AccessList() types.AccessList          { return m.CallMsg.AccessList }
func (m callMsg) Authorizations() []types.Authorization { return m.CallMsg.Authorizations }
func (m callMsg) IsFree() bool                          { return false }
func (m callMsg) SetIsFree(_ bool)                      {}

func (m callMsg) BlobGas() uint64                { return misc.GetBlobGasUsed(len(m.CallMsg.BlobHashes)) }
func (m callMsg) MaxFeePerBlobGas() *uint256.Int { return m.CallMsg.MaxFeePerBlobGas }
func (m callMsg) BlobHashes() []common.Hash      { return m.CallMsg.BlobHashes }
