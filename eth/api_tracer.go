// Copyright 2017 The go-ethereum Authors
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

package eth

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/tracers"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/trie"
)

const (
	// defaultTraceTimeout is the amount of time a single transaction can execute
	// by default before being forcefully aborted.
	defaultTraceTimeout = 5 * time.Second

	// defaultTraceReexec is the number of blocks the tracer is willing to go back
	// and reexecute to produce missing historical state necessary to run a specific
	// trace.
	defaultTraceReexec = uint64(128)
)

// TraceConfig holds extra parameters to trace functions.
type TraceConfig struct {
	*vm.LogConfig
	Tracer  *string
	Timeout *string
	Reexec  *uint64
}

// StdTraceConfig holds extra parameters to standard-json trace functions.
type StdTraceConfig struct {
	*vm.LogConfig
	Reexec *uint64
	TxHash common.Hash
}

// txTraceResult is the result of a single transaction trace.
type txTraceResult struct {
	Result interface{} `json:"result,omitempty"` // Trace results produced by the tracer
	Error  string      `json:"error,omitempty"`  // Trace failure produced by the tracer
}

// blockTraceTask represents a single block trace task when an entire chain is
// being traced.
type blockTraceTask struct {
	tds     *state.TrieDbState
	block   *types.Block     // Block to trace the transactions from
	rootref common.Hash      // Trie root reference held for this task
	results []*txTraceResult // Trace results procudes by the task
}

// blockTraceResult represets the results of tracing a single block when an entire
// chain is being traced.
type blockTraceResult struct {
	Block  hexutil.Uint64   `json:"block"`  // Block number corresponding to this trace
	Hash   common.Hash      `json:"hash"`   // Block hash corresponding to this trace
	Traces []*txTraceResult `json:"traces"` // Trace results produced by the task
}

// txTraceTask represents a single transaction trace task when an entire block
// is being traced.
type txTraceTask struct {
	statedb *state.IntraBlockState // Intermediate state prepped for tracing
	index   int                    // Transaction offset in the block
}

// TraceChain returns the structured logs created during the execution of EVM
// between two blocks (excluding start) and returns them as a JSON object.
func (api *PrivateDebugAPI) TraceChain(ctx context.Context, start, end rpc.BlockNumber, config *TraceConfig) (*rpc.Subscription, error) {
	// Fetch the block interval that we want to trace
	var from, to *types.Block

	switch start {
	case rpc.PendingBlockNumber:
		from = api.eth.miner.PendingBlock()
	case rpc.LatestBlockNumber:
		from = api.eth.blockchain.CurrentBlock()
	default:
		from = api.eth.blockchain.GetBlockByNumber(uint64(start))
	}
	switch end {
	case rpc.PendingBlockNumber:
		to = api.eth.miner.PendingBlock()
	case rpc.LatestBlockNumber:
		to = api.eth.blockchain.CurrentBlock()
	default:
		to = api.eth.blockchain.GetBlockByNumber(uint64(end))
	}
	// Trace the chain if we've found all our blocks
	if from == nil {
		return nil, fmt.Errorf("starting block #%d not found", start)
	}
	if to == nil {
		return nil, fmt.Errorf("end block #%d not found", end)
	}
	if from.Number().Cmp(to.Number()) >= 0 {
		return nil, fmt.Errorf("end block (#%d) needs to come after start block (#%d)", end, start)
	}
	return api.traceChain(ctx, from, to, config)
}

// traceChain configures a new tracer according to the provided configuration, and
// executes all the transactions contained within. The return value will be one item
// per transaction, dependent on the requested tracer.
func (api *PrivateDebugAPI) traceChain(ctx context.Context, start, end *types.Block, config *TraceConfig) (*rpc.Subscription, error) {
	// Tracing a chain is a **long** operation, only do with subscriptions
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()

	// Ensure we have a valid starting state before doing any work
	origin := start.NumberU64()

	if number := start.NumberU64(); number > 0 {
		start = api.eth.blockchain.GetBlock(start.ParentHash(), start.NumberU64()-1)
		if start == nil {
			return nil, fmt.Errorf("parent block #%d not found", number-1)
		}
	}
	tds, err := state.NewTrieDbState(start.Root(), api.eth.ChainDb(), start.NumberU64())
	if err != nil {
		// If the starting state is missing, allow some number of blocks to be re-executed
		reexec := defaultTraceReexec
		if config != nil && config.Reexec != nil {
			reexec = *config.Reexec
		}
		// Find the most recent block that has the state available
		for i := uint64(0); i < reexec; i++ {
			start = api.eth.blockchain.GetBlock(start.ParentHash(), start.NumberU64()-1)
			if start == nil {
				break
			}
			if tds, err = state.NewTrieDbState(start.Root(), api.eth.ChainDb(), start.NumberU64()); err == nil {
				break
			}
		}
		// If we still don't have the state available, bail out
		if err != nil {
			switch err.(type) {
			case *trie.MissingNodeError:
				return nil, errors.New("required historical state unavailable")
			default:
				return nil, err
			}
		}
	}
	statedb := state.New(tds)
	// Execute all the transaction contained within the chain concurrently for each block
	blocks := int(end.NumberU64() - origin)

	threads := runtime.NumCPU()
	if threads > blocks {
		threads = blocks
	}
	var (
		pend    = new(sync.WaitGroup)
		tasks   = make(chan *blockTraceTask, threads)
		results = make(chan *blockTraceTask, threads)
	)
	for th := 0; th < threads; th++ {
		pend.Add(1)
		go func() {
			defer pend.Done()

			// Fetch and execute the next block trace tasks
			for task := range tasks {
				signer := types.MakeSigner(api.eth.blockchain.Config(), task.block.Number())

				// Trace all the transactions contained within
				for i, tx := range task.block.Transactions() {
					msg, _ := tx.AsMessage(signer)
					vmctx := core.NewEVMContext(msg, task.block.Header(), api.eth.blockchain, nil)

					statedb := state.New(task.tds)

					res, err := api.traceTx(ctx, msg, vmctx, statedb, config)
					if err != nil {
						task.results[i] = &txTraceResult{Error: err.Error()}
						log.Warn("Tracing failed", "hash", tx.Hash(), "block", task.block.NumberU64(), "err", err)
						break
					}
					// Only delete empty objects if EIP158/161 (a.k.a Spurious Dragon) is in effect
					_ = statedb.FinalizeTx(api.eth.blockchain.Config().WithEIPsFlags(context.Background(), task.block.Number()), task.tds.TrieStateWriter())
					task.results[i] = &txTraceResult{Result: res}
				}
				// Stream the result back to the user or abort on teardown
				select {
				case results <- task:
				case <-notifier.Closed():
					return
				}
			}
		}()
	}
	// Start a goroutine to feed all the blocks into the tracers
	begin := time.Now()

	go func() {
		var (
			logged time.Time
			number uint64
			traced uint64
			failed error
			proot  common.Hash
		)
		// Ensure everything is properly cleaned up on any exit path
		defer func() {
			close(tasks)
			pend.Wait()

			switch {
			case failed != nil:
				log.Warn("Chain tracing failed", "start", start.NumberU64(), "end", end.NumberU64(), "transactions", traced, "elapsed", time.Since(begin), "err", failed)
			case number < end.NumberU64():
				log.Warn("Chain tracing aborted", "start", start.NumberU64(), "end", end.NumberU64(), "abort", number, "transactions", traced, "elapsed", time.Since(begin))
			default:
				log.Info("Chain tracing finished", "start", start.NumberU64(), "end", end.NumberU64(), "transactions", traced, "elapsed", time.Since(begin))
			}
			close(results)
		}()
		// Feed all the blocks both into the tracer, as well as fast process concurrently
		for number = start.NumberU64() + 1; number <= end.NumberU64(); number++ {
			// Stop tracing if interruption was requested
			select {
			case <-notifier.Closed():
				return
			default:
			}
			// Print progress logs if long enough time elapsed
			if time.Since(logged) > 8*time.Second {
				if number > origin {
					log.Info("Tracing chain segment", "start", origin, "end", end.NumberU64(), "current", number, "transactions", traced, "elapsed", time.Since(begin), "database", api.eth.ChainDb().DiskSize())
				} else {
					log.Info("Preparing state for chain trace", "block", number, "start", origin, "elapsed", time.Since(begin))
				}
				logged = time.Now()
			}
			// Retrieve the next block to trace
			block := api.eth.blockchain.GetBlockByNumber(number)
			if block == nil {
				failed = fmt.Errorf("block #%d not found", number)
				break
			}
			// Send the block over to the concurrent tracers (if not in the fast-forward phase)
			if number > origin {
				txs := block.Transactions()

				select {
				case tasks <- &blockTraceTask{tds: tds.Copy(), block: block, rootref: proot, results: make([]*txTraceResult, len(txs))}:
				case <-notifier.Closed():
					return
				}
				traced += uint64(len(txs))
			}
			// Generate the next state snapshot fast without tracing
			_, _, _, err := api.eth.blockchain.Processor().Process(block, statedb, tds, vm.Config{})
			if err != nil {
				failed = err
				break
			}
			// Finalize the state so any modifications are written to the trie
			ctx := api.eth.blockchain.Config().WithEIPsFlags(context.Background(), big.NewInt(int64(number)))
			tds.SetBlockNr(number)
			err = statedb.CommitBlock(ctx, tds.DbStateWriter())
			if err != nil {
				failed = err
				break
			}
			proot = tds.LastRoot()

			// TODO(karalabe): Do we need the preimages? Won't they accumulate too much?
		}
	}()

	// Keep reading the trace results and stream the to the user
	go func() {
		var (
			done = make(map[uint64]*blockTraceResult)
			next = origin + 1
		)
		for res := range results {
			// Queue up next received result
			result := &blockTraceResult{
				Block:  hexutil.Uint64(res.block.NumberU64()),
				Hash:   res.block.Hash(),
				Traces: res.results,
			}
			done[uint64(result.Block)] = result

			// Stream completed traces to the user, aborting on the first error
			for result, ok := done[next]; ok; result, ok = done[next] {
				if len(result.Traces) > 0 || next == end.NumberU64() {
					notifier.Notify(sub.ID, result)
				}
				delete(done, next)
				next++
			}
		}
	}()
	return sub, nil
}

// TraceBlockByNumber returns the structured logs created during the execution of
// EVM and returns them as a JSON object.
func (api *PrivateDebugAPI) TraceBlockByNumber(ctx context.Context, number rpc.BlockNumber, config *TraceConfig) ([]*txTraceResult, error) {
	// Fetch the block that we want to trace
	var block *types.Block

	switch number {
	case rpc.PendingBlockNumber:
		block = api.eth.miner.PendingBlock()
	case rpc.LatestBlockNumber:
		block = api.eth.blockchain.CurrentBlock()
	default:
		block = api.eth.blockchain.GetBlockByNumber(uint64(number))
	}
	// Trace the block if it was found
	if block == nil {
		return nil, fmt.Errorf("block #%d not found", number)
	}
	return api.traceBlock(ctx, block, config)
}

// TraceBlockByHash returns the structured logs created during the execution of
// EVM and returns them as a JSON object.
func (api *PrivateDebugAPI) TraceBlockByHash(ctx context.Context, hash common.Hash, config *TraceConfig) ([]*txTraceResult, error) {
	block := api.eth.blockchain.GetBlockByHash(hash)
	if block == nil {
		return nil, fmt.Errorf("block %#x not found", hash)
	}
	return api.traceBlock(ctx, block, config)
}

// TraceBlock returns the structured logs created during the execution of EVM
// and returns them as a JSON object.
func (api *PrivateDebugAPI) TraceBlock(ctx context.Context, blob []byte, config *TraceConfig) ([]*txTraceResult, error) {
	block := new(types.Block)
	if err := rlp.Decode(bytes.NewReader(blob), block); err != nil {
		return nil, fmt.Errorf("could not decode block: %v", err)
	}
	return api.traceBlock(ctx, block, config)
}

// TraceBlockFromFile returns the structured logs created during the execution of
// EVM and returns them as a JSON object.
func (api *PrivateDebugAPI) TraceBlockFromFile(ctx context.Context, file string, config *TraceConfig) ([]*txTraceResult, error) {
	blob, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("could not read file: %v", err)
	}
	return api.TraceBlock(ctx, blob, config)
}

// TraceBadBlockByHash returns the structured logs created during the execution of
// EVM against a block pulled from the pool of bad ones and returns them as a JSON
// object.
func (api *PrivateDebugAPI) TraceBadBlock(ctx context.Context, hash common.Hash, config *TraceConfig) ([]*txTraceResult, error) {
	blocks := api.eth.blockchain.BadBlocks()
	for _, block := range blocks {
		if block.Hash() == hash {
			return api.traceBlock(ctx, block, config)
		}
	}
	return nil, fmt.Errorf("bad block %#x not found", hash)
}

// StandardTraceBlockToFile dumps the structured logs created during the
// execution of EVM to the local file system and returns a list of files
// to the caller.
func (api *PrivateDebugAPI) StandardTraceBlockToFile(ctx context.Context, hash common.Hash, config *StdTraceConfig) ([]string, error) {
	block := api.eth.blockchain.GetBlockByHash(hash)
	if block == nil {
		return nil, fmt.Errorf("block %#x not found", hash)
	}
	return api.standardTraceBlockToFile(ctx, block, config)
}

// StandardTraceBadBlockToFile dumps the structured logs created during the
// execution of EVM against a block pulled from the pool of bad ones to the
// local file system and returns a list of files to the caller.
func (api *PrivateDebugAPI) StandardTraceBadBlockToFile(ctx context.Context, hash common.Hash, config *StdTraceConfig) ([]string, error) {
	blocks := api.eth.blockchain.BadBlocks()
	for _, block := range blocks {
		if block.Hash() == hash {
			return api.standardTraceBlockToFile(ctx, block, config)
		}
	}
	return nil, fmt.Errorf("bad block %#x not found", hash)
}

// traceBlock configures a new tracer according to the provided configuration, and
// executes all the transactions contained within. The return value will be one item
// per transaction, dependent on the requestd tracer.
func (api *PrivateDebugAPI) traceBlock(ctx context.Context, block *types.Block, config *TraceConfig) ([]*txTraceResult, error) {
	// Create the parent state database
	if err := api.eth.engine.VerifyHeader(api.eth.blockchain, block.Header(), true); err != nil {
		return nil, err
	}
	parent := api.eth.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, fmt.Errorf("parent %#x not found", block.ParentHash())
	}
	statedb, dbstate := ComputeIntraBlockState(api.eth.ChainDb(), parent)
	// Execute all the transaction contained within the block concurrently
	var (
		signer = types.MakeSigner(api.eth.blockchain.Config(), block.Number())

		txs     = block.Transactions()
		results = make([]*txTraceResult, len(txs))

		pend = new(sync.WaitGroup)
		jobs = make(chan *txTraceTask, len(txs))
	)
	threads := runtime.NumCPU()
	if threads > len(txs) {
		threads = len(txs)
	}
	for th := 0; th < threads; th++ {
		pend.Add(1)
		go func() {
			defer pend.Done()

			// Fetch and execute the next transaction trace tasks
			for task := range jobs {
				msg, _ := txs[task.index].AsMessage(signer)
				vmctx := core.NewEVMContext(msg, block.Header(), api.eth.blockchain, nil)

				res, err := api.traceTx(ctx, msg, vmctx, task.statedb, config)
				if err != nil {
					results[task.index] = &txTraceResult{Error: err.Error()}
					continue
				}
				results[task.index] = &txTraceResult{Result: res}
			}
		}()
	}
	// Feed the transactions into the tracers and return
	var failed error
	for i, tx := range txs {
		// Send the trace task over for execution
		jobs <- &txTraceTask{statedb: state.New(dbstate), index: i}

		// Generate the next state snapshot fast without tracing
		msg, _ := tx.AsMessage(signer)
		vmctx := core.NewEVMContext(msg, block.Header(), api.eth.blockchain, nil)

		vmenv := vm.NewEVM(vmctx, statedb, api.eth.blockchain.Config(), vm.Config{})
		if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(msg.Gas())); err != nil {
			failed = err
			break
		}
		// Finalize the state so any modifications are written to the trie
		// Only delete empty objects if EIP158/161 (a.k.a Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(vmenv.ChainConfig().WithEIPsFlags(context.Background(), block.Number()), dbstate)
	}
	close(jobs)
	pend.Wait()

	// If execution failed in between, abort
	if failed != nil {
		return nil, failed
	}
	return results, nil
}

// standardTraceBlockToFile configures a new tracer which uses standard JSON output,
// and traces either a full block or an individual transaction. The return value will
// be one filename per transaction traced.
func (api *PrivateDebugAPI) standardTraceBlockToFile(ctx context.Context, block *types.Block, config *StdTraceConfig) ([]string, error) {
	// If we're tracing a single transaction, make sure it's present
	if config != nil && config.TxHash != (common.Hash{}) {
		if !containsTx(block, config.TxHash) {
			return nil, fmt.Errorf("transaction %#x not found in block", config.TxHash)
		}
	}
	// Create the parent state database
	if err := api.eth.engine.VerifyHeader(api.eth.blockchain, block.Header(), true); err != nil {
		return nil, err
	}
	parent := api.eth.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, fmt.Errorf("parent %#x not found", block.ParentHash())
	}
	/*
		reexec := defaultTraceReexec
		if config != nil && config.Reexec != nil {
			reexec = *config.Reexec
		}
	*/
	statedb, dbstate := ComputeIntraBlockState(api.eth.ChainDb(), parent)
	// Retrieve the tracing configurations, or use default values
	var (
		logConfig vm.LogConfig
		txHash    common.Hash
	)
	if config != nil {
		if config.LogConfig != nil {
			logConfig = *config.LogConfig
		}
		txHash = config.TxHash
	}
	logConfig.Debug = true

	// Execute transaction, either tracing all or just the requested one
	var (
		signer = types.MakeSigner(api.eth.blockchain.Config(), block.Number())
		dumps  []string
	)
	for i, tx := range block.Transactions() {
		// Prepare the trasaction for un-traced execution
		var (
			msg, _ = tx.AsMessage(signer)
			vmctx  = core.NewEVMContext(msg, block.Header(), api.eth.blockchain, nil)

			vmConf vm.Config
			dump   *os.File
			writer *bufio.Writer
			err    error
		)
		// If the transaction needs tracing, swap out the configs
		if tx.Hash() == txHash || txHash == (common.Hash{}) {
			// Generate a unique temporary file to dump it into
			prefix := fmt.Sprintf("block_%#x-%d-%#x-", block.Hash().Bytes()[:4], i, tx.Hash().Bytes()[:4])

			dump, err = ioutil.TempFile(os.TempDir(), prefix)
			if err != nil {
				return nil, err
			}
			dumps = append(dumps, dump.Name())

			// Swap out the noop logger to the standard tracer
			writer = bufio.NewWriter(dump)
			vmConf = vm.Config{
				Debug:                   true,
				Tracer:                  vm.NewJSONLogger(&logConfig, writer),
				EnablePreimageRecording: true,
			}
		}
		// Execute the transaction and flush any traces to disk
		vmenv := vm.NewEVM(vmctx, statedb, api.eth.blockchain.Config(), vmConf)
		_, _, _, err = core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(msg.Gas()))
		if writer != nil {
			writer.Flush()
		}
		if dump != nil {
			dump.Close()
			log.Info("Wrote standard trace", "file", dump.Name())
		}
		if err != nil {
			return dumps, err
		}
		// Finalize the state so any modifications are written to the trie
		// Only delete empty objects if EIP158/161 (a.k.a Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(vmenv.ChainConfig().WithEIPsFlags(context.Background(), block.Number()), dbstate)

		// If we've traced the transaction we were looking for, abort
		if tx.Hash() == txHash {
			break
		}
	}
	return dumps, nil
}

// containsTx reports whether the transaction with a certain hash
// is contained within the specified block.
func containsTx(block *types.Block, hash common.Hash) bool {
	for _, tx := range block.Transactions() {
		if tx.Hash() == hash {
			return true
		}
	}
	return false
}

// computeIntraBlockState retrieves the state database associated with a certain block.
// If no state is locally available for the given block, a number of blocks are
// attempted to be reexecuted to generate the desired state.
func ComputeIntraBlockState(chainDb ethdb.Getter, block *types.Block) (*state.IntraBlockState, *state.DbState) {
	// If we have the state fully available, use that
	dbstate := state.NewDbState(chainDb, block.NumberU64())
	statedb := state.New(dbstate)
	return statedb, dbstate
}

// TraceTransaction returns the structured logs created during the execution of EVM
// and returns them as a JSON object.
func (api *PrivateDebugAPI) TraceTransaction(ctx context.Context, hash common.Hash, config *TraceConfig) (interface{}, error) {
	// Retrieve the transaction and assemble its EVM context
	tx, blockHash, _, index := rawdb.ReadTransaction(api.eth.ChainDb(), hash)
	if tx == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}
	msg, vmctx, statedb, _, err := ComputeTxEnv(ctx, api.eth.blockchain, api.eth.blockchain.Config(), api.eth.blockchain, api.eth.ChainDb(), blockHash, index)
	if err != nil {
		return nil, err
	}
	// Trace the transaction and return
	return api.traceTx(ctx, msg, vmctx, statedb, config)
}

// traceTx configures a new tracer according to the provided configuration, and
// executes the given message in the provided environment. The return value will
// be tracer dependent.
func (api *PrivateDebugAPI) traceTx(ctx context.Context, message core.Message, vmctx vm.Context, state vm.IntraBlockState,
	config *TraceConfig) (interface{}, error) {
	// Assemble the structured logger or the JavaScript tracer
	var (
		tracer vm.Tracer
		err    error
	)
	switch {
	case config != nil && config.Tracer != nil:
		// Define a meaningful timeout of a single transaction trace
		timeout := defaultTraceTimeout
		if config.Timeout != nil {
			if timeout, err = time.ParseDuration(*config.Timeout); err != nil {
				return nil, err
			}
		}
		// Constuct the JavaScript tracer to execute with
		if tracer, err = tracers.New(*config.Tracer); err != nil {
			return nil, err
		}
		// Handle timeouts and RPC cancellations
		deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
		go func() {
			<-deadlineCtx.Done()
			tracer.(*tracers.Tracer).Stop(errors.New("execution timeout"))
		}()
		defer cancel()

	case config == nil:
		tracer = vm.NewStructLogger(nil)

	default:
		tracer = vm.NewStructLogger(config.LogConfig)
	}
	// Run the transaction with tracing enabled.
	vmenv := vm.NewEVM(vmctx, state, api.eth.blockchain.Config(), vm.Config{Debug: true, Tracer: tracer})

	ret, gas, failed, err := core.ApplyMessage(vmenv, message, new(core.GasPool).AddGas(message.Gas()))
	if err != nil {
		return nil, fmt.Errorf("tracing failed: %v", err)
	}
	// Depending on the tracer type, format and return the output
	switch tracer := tracer.(type) {
	case *vm.StructLogger:
		return &ethapi.ExecutionResult{
			Gas:         gas,
			Failed:      failed,
			ReturnValue: fmt.Sprintf("%x", ret),
			StructLogs:  ethapi.FormatLogs(tracer.StructLogs()),
		}, nil

	case *tracers.Tracer:
		return tracer.GetResult()

	default:
		panic(fmt.Sprintf("bad tracer type %T", tracer))
	}
}

type BlockGetter interface {
	// GetBlockByHash retrieves a block from the database by hash, caching it if found.
	GetBlockByHash(hash common.Hash) *types.Block
	// GetBlock retrieves a block from the database by hash and number,
	// caching it if found.
	GetBlock(hash common.Hash, number uint64) *types.Block
}

// computeTxEnv returns the execution environment of a certain transaction.
func ComputeTxEnv(ctx context.Context, blockGetter BlockGetter, cfg *params.ChainConfig, chain core.ChainContext, chainDb ethdb.Getter, blockHash common.Hash, txIndex uint64) (core.Message, vm.Context, *state.IntraBlockState, *state.DbState, error) {
	// Create the parent state database
	block := blockGetter.GetBlockByHash(blockHash)
	if block == nil {
		return nil, vm.Context{}, nil, nil, fmt.Errorf("block %x not found", blockHash)
	}
	parent := blockGetter.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, vm.Context{}, nil, nil, fmt.Errorf("parent %x not found", block.ParentHash())
	}
	statedb, dbstate := ComputeIntraBlockState(chainDb, parent)
	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, block.Number())

	for idx, tx := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return nil, vm.Context{}, nil, nil, ctx.Err()
		}

		// Assemble the transaction call message and return if the requested offset
		msg, _ := tx.AsMessage(signer)
		EVMcontext := core.NewEVMContext(msg, block.Header(), chain, nil)
		if idx == int(txIndex) {
			return msg, EVMcontext, statedb, dbstate, nil
		}
		// Not yet the searched for transaction, execute on top of the current state
		vmenv := vm.NewEVM(EVMcontext, statedb, cfg, vm.Config{})
		if _, _, _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
			return nil, vm.Context{}, nil, nil, fmt.Errorf("transaction %x failed: %v", tx.Hash(), err)
		}
		// Ensure any modifications are committed to the state
		// Only delete empty objects if EIP158/161 (a.k.a Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(vmenv.ChainConfig().WithEIPsFlags(context.Background(), block.Number()), dbstate)
	}
	return nil, vm.Context{}, nil, nil, fmt.Errorf("transaction index %d out of range for block %x", txIndex, blockHash)
}
