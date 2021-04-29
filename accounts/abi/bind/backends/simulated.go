// Copyright 2015 The go-ethereum Authors
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

package backends

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"

	ethereum "github.com/ledgerwatch/turbo-geth"
	"github.com/ledgerwatch/turbo-geth/accounts/abi"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/common/u256"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/bloombits"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/filters"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
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
	database  *ethdb.ObjectDatabase // In memory database to store our testing data
	engine    consensus.Engine
	getHeader func(hash common.Hash, number uint64) *types.Header
	checkTEVM func(addr common.Address) (bool, error)

	mu              sync.Mutex
	prependBlock    *types.Block
	pendingReceipts types.Receipts
	pendingHeader   *types.Header
	gasPool         *core.GasPool
	pendingBlock    *types.Block // Currently pending block that will be imported on request
	pendingReader   *state.PlainStateReader
	pendingState    *state.IntraBlockState // Currently pending state that will be the active on request

	events *filters.EventSystem // Event system for filtering log events live

	config     *params.ChainConfig
	rmLogsFeed event.Feed
	chainFeed  event.Feed
	logsFeed   event.Feed
}

// NewSimulatedBackendWithDatabase creates a new binding backend based on the given database
// and uses a simulated blockchain for testing purposes.
// A simulated backend always uses chainID 1337.
func NewSimulatedBackendWithDatabase(database *ethdb.ObjectDatabase, alloc core.GenesisAlloc, gasLimit uint64) *SimulatedBackend {
	genesis := core.Genesis{Config: params.AllEthashProtocolChanges, GasLimit: gasLimit, Alloc: alloc}
	genesisBlock := genesis.MustCommit(database)
	engine := ethash.NewFaker()

	backend := &SimulatedBackend{
		prependBlock: genesisBlock,
		database:     database,
		engine:       engine,
		getHeader: func(hash common.Hash, number uint64) *types.Header {
			return rawdb.ReadHeader(database, hash, number)
		},
		checkTEVM: func(addr common.Address) (bool, error) {
			return database.Has(dbutils.ContractTEVMCodeBucket, addr.Bytes())
		},
		config: genesis.Config,
	}
	backend.events = filters.NewEventSystem(&filterBackend{database, backend})
	backend.emptyPendingBlock()
	return backend
}

// NewSimulatedBackend creates a new binding backend using a simulated blockchain
// for testing purposes.
func NewSimulatedBackendWithConfig(alloc core.GenesisAlloc, config *params.ChainConfig, gasLimit uint64) *SimulatedBackend {
	database := ethdb.NewMemDatabase()
	genesis := core.Genesis{Config: config, GasLimit: gasLimit, Alloc: alloc}
	genesisBlock := genesis.MustCommit(database)
	engine := ethash.NewFaker()

	backend := &SimulatedBackend{
		prependBlock: genesisBlock,
		database:     database,
		engine:       engine,
		config:       genesis.Config,
		getHeader: func(hash common.Hash, number uint64) *types.Header {
			return rawdb.ReadHeader(database, hash, number)
		},
		checkTEVM: func(addr common.Address) (bool, error) {
			return database.Has(dbutils.ContractTEVMCodeBucket, addr.Bytes())
		},
	}
	backend.events = filters.NewEventSystem(&filterBackend{database, backend})
	backend.emptyPendingBlock()
	return backend
}

// A simulated backend always uses chainID 1337.
func NewSimulatedBackend(alloc core.GenesisAlloc, gasLimit uint64) *SimulatedBackend {
	return NewSimulatedBackendWithDatabase(ethdb.NewMemDatabase(), alloc, gasLimit)
}

func (b *SimulatedBackend) DB() ethdb.Database {
	return b.database
}

// Close terminates the underlying blockchain's update loop.
func (b *SimulatedBackend) Close() error {
	b.database.Close()
	return nil
}

// Commit imports all the pending transactions as a single block and starts a
// fresh new state.
func (b *SimulatedBackend) Commit() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, err := stagedsync.InsertBlockInStages(b.database, b.config, &vm.Config{}, b.engine, b.pendingBlock, false /* checkRoot */); err != nil {
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
	blocks, receipts, _ := core.GenerateChain(b.config, b.prependBlock, ethash.NewFaker(), b.database, 1, func(int, *core.BlockGen) {}, false /* intermediateHashes */)
	b.pendingBlock = blocks[0]
	b.pendingReceipts = receipts[0]
	b.pendingHeader = b.pendingBlock.Header()
	b.gasPool = new(core.GasPool).AddGas(b.pendingHeader.GasLimit)
	b.pendingReader = state.NewPlainStateReader(b.database)
	b.pendingState = state.New(b.pendingReader)
}

// stateByBlockNumber retrieves a state by a given blocknumber.
func (b *SimulatedBackend) stateByBlockNumber(db ethdb.Database, blockNumber *big.Int) *state.IntraBlockState {
	if blockNumber == nil || blockNumber.Cmp(b.pendingBlock.Number()) == 0 {
		return state.New(state.NewPlainDBState(db, b.pendingBlock.NumberU64()))
	}
	return state.New(state.NewPlainDBState(db, uint64(blockNumber.Int64())))
}

// CodeAt returns the code associated with a certain account in the blockchain.
func (b *SimulatedBackend) CodeAt(ctx context.Context, contract common.Address, blockNumber *big.Int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	dbtx, err1 := b.database.Begin(ctx, ethdb.RO)
	if err1 != nil {
		return nil, err1
	}
	defer dbtx.Rollback()
	stateDB := b.stateByBlockNumber(dbtx, blockNumber)
	return stateDB.GetCode(contract), nil
}

// BalanceAt returns the wei balance of a certain account in the blockchain.
func (b *SimulatedBackend) BalanceAt(ctx context.Context, contract common.Address, blockNumber *big.Int) (*uint256.Int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	dbtx, err1 := b.database.Begin(ctx, ethdb.RO)
	if err1 != nil {
		return nil, err1
	}
	defer dbtx.Rollback()
	stateDB := b.stateByBlockNumber(dbtx, blockNumber)
	return stateDB.GetBalance(contract), nil
}

// NonceAt returns the nonce of a certain account in the blockchain.
func (b *SimulatedBackend) NonceAt(ctx context.Context, contract common.Address, blockNumber *big.Int) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	dbtx, err1 := b.database.Begin(ctx, ethdb.RO)
	if err1 != nil {
		return 0, err1
	}
	defer dbtx.Rollback()
	stateDB := b.stateByBlockNumber(dbtx, blockNumber)
	return stateDB.GetNonce(contract), nil
}

// StorageAt returns the value of key in the storage of an account in the blockchain.
func (b *SimulatedBackend) StorageAt(ctx context.Context, contract common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	dbtx, err1 := b.database.Begin(ctx, ethdb.RO)
	if err1 != nil {
		return nil, err1
	}
	defer dbtx.Rollback()
	stateDB := b.stateByBlockNumber(dbtx, blockNumber)
	var val uint256.Int
	stateDB.GetState(contract, &key, &val)
	return val.Bytes(), nil
}

// TransactionReceipt returns the receipt of a transaction.
func (b *SimulatedBackend) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	receipt, _, _, _ := rawdb.ReadReceipt(b.database, txHash)
	return receipt, nil
}

// TransactionByHash checks the pool of pending transactions in addition to the
// blockchain. The isPending return value indicates whether the transaction has been
// mined yet. Note that the transaction may not be part of the canonical chain even if
// it's not pending.
func (b *SimulatedBackend) TransactionByHash(ctx context.Context, txHash common.Hash) (types.Transaction, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	tx := b.pendingBlock.Transaction(txHash)
	if tx != nil {
		return tx, true, nil
	}
	tx, _, _, _ = rawdb.ReadTransaction(b.database, txHash)
	if tx != nil {
		return tx, false, nil
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

	block, err := rawdb.ReadBlockByHash(b.database, hash)
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
func (b *SimulatedBackend) blockByNumberNoLock(_ context.Context, number *big.Int) (*types.Block, error) {
	if number == nil || number.Cmp(b.prependBlock.Number()) == 0 {
		return b.prependBlock, nil
	}

	hash, err := rawdb.ReadCanonicalHash(b.database, number.Uint64())
	if err != nil {
		return nil, err
	}
	block := rawdb.ReadBlock(b.database, hash, number.Uint64())
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

	number := rawdb.ReadHeaderNumber(b.database, hash)
	if number == nil {
		return nil, errBlockDoesNotExist
	}
	header := rawdb.ReadHeader(b.database, hash, *number)
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

	if number == nil || number.Cmp(b.prependBlock.Number()) == 0 {
		return b.prependBlock.Header(), nil
	}
	hash, err := rawdb.ReadCanonicalHash(b.database, number.Uint64())
	if err != nil {
		return nil, err
	}
	header := rawdb.ReadHeader(b.database, hash, number.Uint64())
	return header, nil
}

// TransactionCount returns the number of transactions in a given block.
func (b *SimulatedBackend) TransactionCount(ctx context.Context, blockHash common.Hash) (uint, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if blockHash == b.pendingBlock.Hash() {
		return uint(b.pendingBlock.Transactions().Len()), nil
	}

	block, err := rawdb.ReadBlockByHash(b.database, blockHash)
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

	block, err := rawdb.ReadBlockByHash(b.database, blockHash)
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

	return b.pendingState.GetCode(contract), nil
}

func newRevertError(result *core.ExecutionResult) *revertError {
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
// See: https://github.com/ethereum/wiki/wiki/JSON-RPC-Error-Codes-Improvement-Proposal
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
	s := state.New(state.NewPlainStateReader(b.database))
	res, err := b.callContract(ctx, call, b.pendingBlock, s)
	if err != nil {
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
	defer b.pendingState.RevertToSnapshot(b.pendingState.Snapshot())

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

	return b.pendingState.GetNonce(account), nil
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
		lo  = params.TxGas - 1
		hi  uint64
		cap uint64
	)
	if call.Gas >= params.TxGas {
		hi = call.Gas
	} else {
		hi = b.pendingBlock.GasLimit()
	}
	// Recap the highest gas allowance with account's balance.
	if call.GasPrice != nil && call.GasPrice.BitLen() != 0 {
		balance := b.pendingState.GetBalance(call.From) // from can't be nil
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
	cap = hi
	b.pendingState.Prepare(common.Hash{}, common.Hash{}, len(b.pendingBlock.Transactions()))

	// Create a helper to check if a gas allowance results in an executable transaction
	executable := func(gas uint64) (bool, *core.ExecutionResult, error) {
		call.Gas = gas

		snapshot := b.pendingState.Snapshot()
		res, err := b.callContract(ctx, call, b.pendingBlock, b.pendingState)
		b.pendingState.RevertToSnapshot(snapshot)

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
	if hi == cap {
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
			return 0, fmt.Errorf("gas required exceeds allowance (%d)", cap)
		}
	}
	return hi, nil
}

// callContract implements common code between normal and pending contract calls.
// state is modified during execution, make sure to copy it if necessary.
func (b *SimulatedBackend) callContract(_ context.Context, call ethereum.CallMsg, block *types.Block, statedb *state.IntraBlockState) (*core.ExecutionResult, error) {
	// Ensure message is initialized properly.
	if call.GasPrice == nil {
		call.GasPrice = u256.Num1
	}
	if call.Gas == 0 {
		call.Gas = 50000000
	}
	if call.Value == nil {
		call.Value = new(uint256.Int)
	}
	// Set infinite balance to the fake caller account.
	from := statedb.GetOrNewStateObject(call.From)
	from.SetBalance(uint256.NewInt().SetAllOne())
	// Execute the call.
	msg := callMsg{call}

	txContext := core.NewEVMTxContext(msg)
	evmContext := core.NewEVMBlockContext(block.Header(), b.getHeader, b.engine, nil, b.checkTEVM)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmEnv := vm.NewEVM(evmContext, txContext, statedb, b.config, vm.Config{})
	gasPool := new(core.GasPool).AddGas(math.MaxUint64)

	return core.NewStateTransition(vmEnv, msg, gasPool).TransitionDb(true /* refunds */, false /* gasBailout */)
}

// SendTransaction updates the pending block to include the given transaction.
// It panics if the transaction is invalid.
func (b *SimulatedBackend) SendTransaction(ctx context.Context, tx types.Transaction) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check transaction validity.
	signer := types.MakeSigner(b.config, b.pendingBlock.NumberU64())
	sender, senderErr := tx.Sender(*signer)
	if senderErr != nil {
		return fmt.Errorf("invalid transaction: %v", senderErr)
	}
	nonce := b.pendingState.GetNonce(sender)
	if tx.GetNonce() != nonce {
		return fmt.Errorf("invalid transaction nonce: got %d, want %d", tx.GetNonce(), nonce)
	}

	b.pendingState.Prepare(tx.Hash(), common.Hash{}, len(b.pendingBlock.Transactions()))
	//fmt.Printf("==== Start producing block %d, header: %d\n", b.pendingBlock.NumberU64(), b.pendingHeader.Number.Uint64())
	if _, err := core.ApplyTransaction(
		b.config, b.getHeader, b.engine,
		&b.pendingHeader.Coinbase, b.gasPool,
		b.pendingState, state.NewNoopWriter(),
		b.pendingHeader, tx,
		&b.pendingHeader.GasUsed, vm.Config{}, b.checkTEVM); err != nil {
		return err
	}
	//fmt.Printf("==== Start producing block %d\n", (b.prependBlock.NumberU64() + 1))
	blocks, receipts, err := core.GenerateChain(b.config, b.prependBlock, ethash.NewFaker(), b.database, 1, func(number int, block *core.BlockGen) {
		for _, tx := range b.pendingBlock.Transactions() {
			block.AddTxWithChain(b.getHeader, b.engine, tx)
		}
		block.AddTxWithChain(b.getHeader, b.engine, tx)
	}, false /* intermediateHashes */)
	if err != nil {
		return err
	}
	//fmt.Printf("==== End producing block %d\n", b.pendingBlock.NumberU64())
	b.pendingBlock = blocks[0]
	b.pendingReceipts = receipts[0]
	b.pendingHeader = b.pendingBlock.Header()
	return nil
}

// FilterLogs executes a log filter operation, blocking during execution and
// returning all the results in one batch.
//
// TODO(karalabe): Deprecate when the subscription one can return past data too.
func (b *SimulatedBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	var filter *filters.Filter
	if query.BlockHash != nil {
		// Block filter requested, construct a single-shot filter
		filter = filters.NewBlockFilter(&filterBackend{b.database, b}, *query.BlockHash, query.Addresses, query.Topics)
	} else {
		// Initialize unset filter boundaries to run from genesis to chain head
		from := int64(0)
		if query.FromBlock != nil {
			from = query.FromBlock.Int64()
		}
		to := int64(-1)
		if query.ToBlock != nil {
			to = query.ToBlock.Int64()
		}
		// Construct the range filter
		filter = filters.NewRangeFilter(&filterBackend{b.database, b}, from, to, query.Addresses, query.Topics)
	}
	// Run the filter and return all the logs
	logs, err := filter.Logs(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]types.Log, len(logs))
	for i, nLog := range logs {
		res[i] = *nLog
	}
	return res, nil
}

// SubscribeFilterLogs creates a background log filtering operation, returning a
// subscription immediately, which can be used to stream the found events.
func (b *SimulatedBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	// Subscribe to contract events
	sink := make(chan []*types.Log)

	sub, err := b.events.SubscribeLogs(query, sink)
	if err != nil {
		return nil, err
	}
	// Since we're getting logs in batches, we need to flatten them into a plain stream
	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case logs := <-sink:
				for _, nlog := range logs {
					select {
					case ch <- *nlog:
					case err := <-sub.Err():
						return err
					case <-quit:
						return nil
					}
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// SubscribeNewHead returns an event subscription for a new header.
func (b *SimulatedBackend) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	// subscribe to a new head
	sink := make(chan *types.Header)
	sub := b.events.SubscribeNewHeads(sink)

	return event.NewSubscription(func(quit <-chan struct{}) error {
		defer sub.Unsubscribe()
		for {
			select {
			case head := <-sink:
				select {
				case ch <- head:
				case err := <-sub.Err():
					return err
				case <-quit:
					return nil
				}
			case err := <-sub.Err():
				return err
			case <-quit:
				return nil
			}
		}
	}), nil
}

// AdjustTime adds a time shift to the simulated clock.
// It can only be called on empty blocks.
func (b *SimulatedBackend) AdjustTime(adjustment time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pendingBlock.Transactions()) != 0 {
		return errors.New("could not adjust time on non-empty block")
	}

	blocks, _, err := core.GenerateChain(b.config, b.prependBlock, ethash.NewFaker(), b.database, 1, func(number int, block *core.BlockGen) {
		for _, tx := range b.pendingBlock.Transactions() {
			block.AddTxWithChain(b.getHeader, b.engine, tx)
		}
		block.OffsetTime(int64(adjustment.Seconds()))
	}, false /* intermediateHashes */)
	if err != nil {
		return err
	}
	b.pendingBlock = blocks[0]
	b.pendingHeader = b.pendingBlock.Header()

	return nil
}

// callMsg implements core.Message to allow passing it as a transaction simulator.
type callMsg struct {
	ethereum.CallMsg
}

func (m callMsg) From() common.Address         { return m.CallMsg.From }
func (m callMsg) Nonce() uint64                { return 0 }
func (m callMsg) CheckNonce() bool             { return false }
func (m callMsg) To() *common.Address          { return m.CallMsg.To }
func (m callMsg) GasPrice() *uint256.Int       { return m.CallMsg.GasPrice }
func (m callMsg) FeeCap() *uint256.Int         { return m.CallMsg.FeeCap }
func (m callMsg) Tip() *uint256.Int            { return m.CallMsg.Tip }
func (m callMsg) Gas() uint64                  { return m.CallMsg.Gas }
func (m callMsg) Value() *uint256.Int          { return m.CallMsg.Value }
func (m callMsg) Data() []byte                 { return m.CallMsg.Data }
func (m callMsg) AccessList() types.AccessList { return m.CallMsg.AccessList }

// filterBackend implements filters.Backend to support filtering for logs without
// taking bloom-bits acceleration structures into account.
type filterBackend struct {
	db ethdb.Database
	b  *SimulatedBackend
}

func (fb *filterBackend) ChainDb() ethdb.Database { return fb.db }

func (fb *filterBackend) HeaderByNumber(ctx context.Context, block rpc.BlockNumber) (*types.Header, error) {
	if block == rpc.LatestBlockNumber {
		return fb.b.HeaderByNumber(ctx, nil)
	}
	return fb.b.HeaderByNumber(ctx, big.NewInt(block.Int64()))
}

func (fb *filterBackend) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	return fb.b.HeaderByHash(ctx, hash)
}

func (fb *filterBackend) GetReceipts(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	number := rawdb.ReadHeaderNumber(fb.db, hash)
	if number == nil {
		return nil, nil
	}
	return rawdb.ReadReceipts(fb.db, hash, *number), nil
}

func (fb *filterBackend) GetLogs(ctx context.Context, hash common.Hash) ([][]*types.Log, error) {
	number := rawdb.ReadHeaderNumber(fb.db, hash)
	if number == nil {
		return nil, nil
	}
	receipts := rawdb.ReadReceipts(fb.db, hash, *number)
	if receipts == nil {
		return nil, nil
	}
	logs := make([][]*types.Log, len(receipts))
	for i, receipt := range receipts {
		logs[i] = receipt.Logs
	}
	return logs, nil
}

func (fb *filterBackend) SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription {
	return nullSubscription()
}

func (fb *filterBackend) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	return fb.b.chainFeed.Subscribe(ch)
}

func (fb *filterBackend) SubscribeRemovedLogsEvent(ch chan<- core.RemovedLogsEvent) event.Subscription {
	return fb.b.rmLogsFeed.Subscribe(ch)
}

func (fb *filterBackend) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	return fb.b.logsFeed.Subscribe(ch)
}

func (fb *filterBackend) SubscribePendingLogsEvent(ch chan<- []*types.Log) event.Subscription {
	return nullSubscription()
}

func (fb *filterBackend) BloomStatus() (uint64, uint64) { return 4096, 0 }

func (fb *filterBackend) ServiceFilter(ctx context.Context, ms *bloombits.MatcherSession) {
	panic("not supported")
}

func nullSubscription() event.Subscription {
	return event.NewSubscription(func(quit <-chan struct{}) error {
		<-quit
		return nil
	})
}
