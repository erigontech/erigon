package transactions

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/tracers"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/ethapi"
	"github.com/ledgerwatch/turbo-geth/params"
	state2 "github.com/ledgerwatch/turbo-geth/turbo/adapter"
)

const (
	// defaultTraceTimeout is the amount of time a single transaction can execute
	// by default before being forcefully aborted.
	defaultTraceTimeout = 5 * time.Second
)

type BlockGetter interface {
	// GetBlockByHash retrieves a block from the database by hash, caching it if found.
	GetBlockByHash(hash common.Hash) (*types.Block, error)
	// GetBlock retrieves a block from the database by hash and number,
	// caching it if found.
	GetBlock(hash common.Hash, number uint64) *types.Block
}

// computeTxEnv returns the execution environment of a certain transaction.
func ComputeTxEnv(ctx context.Context, blockGetter BlockGetter, cfg *params.ChainConfig, chain core.ChainContext, dbtx ethdb.Tx, blockHash common.Hash, txIndex uint64) (core.Message, vm.Context, *state.IntraBlockState, *state2.StateReader, error) {
	// Create the parent state database
	block, err := blockGetter.GetBlockByHash(blockHash)
	if err != nil {
		return nil, vm.Context{}, nil, nil, err
	}
	if block == nil {
		return nil, vm.Context{}, nil, nil, fmt.Errorf("block %x not found", blockHash)
	}
	parent := blockGetter.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, vm.Context{}, nil, nil, fmt.Errorf("parent %x not found", block.ParentHash())
	}

	statedb, reader := state2.ComputeIntraBlockState(dbtx, parent)

	if txIndex == 0 && len(block.Transactions()) == 0 {
		return nil, vm.Context{}, statedb, reader, nil
	}
	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, block.Number())

	for idx, tx := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return nil, vm.Context{}, nil, nil, ctx.Err()
		}
		statedb.Prepare(tx.Hash(), blockHash, idx)

		// Assemble the transaction call message and return if the requested offset
		msg, _ := tx.AsMessage(signer)
		EVMcontext := core.NewEVMContext(msg, block.Header(), chain, nil)
		if idx == int(txIndex) {
			return msg, EVMcontext, statedb, reader, nil
		}
		// Not yet the searched for transaction, execute on top of the current state
		vmenv := vm.NewEVM(EVMcontext, statedb, cfg, vm.Config{})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas()), true /* refunds */, false /* gasBailout */); err != nil {
			return nil, vm.Context{}, nil, nil, fmt.Errorf("transaction %x failed: %v", tx.Hash(), err)
		}
		// Ensure any modifications are committed to the state
		// Only delete empty objects if EIP158/161 (a.k.a Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(vmenv.ChainConfig().WithEIPsFlags(context.Background(), block.Number()), reader)
	}
	return nil, vm.Context{}, nil, nil, fmt.Errorf("transaction index %d out of range for block %x", txIndex, blockHash)
}

// TraceTx configures a new tracer according to the provided configuration, and
// executes the given message in the provided environment. The return value will
// be tracer dependent.
func TraceTx(ctx context.Context, message core.Message, vmctx vm.Context, ibs vm.IntraBlockState, config *eth.TraceConfig, chainConfig *params.ChainConfig) (interface{}, error) {
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
	vmenv := vm.NewEVM(vmctx, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})

	var refunds bool = true
	if config != nil && config.NoRefunds != nil && *config.NoRefunds {
		refunds = false
	}
	result, err := core.ApplyMessage(vmenv, message, new(core.GasPool).AddGas(message.Gas()), refunds, false /* gasBailout */)
	if err != nil {
		return nil, fmt.Errorf("tracing failed: %v", err)
	}
	// Depending on the tracer type, format and return the output
	switch tracer := tracer.(type) {
	case *vm.StructLogger:
		return &ethapi.ExecutionResult{
			Gas:         result.UsedGas,
			Failed:      result.Failed(),
			ReturnValue: fmt.Sprintf("%x", result.Return()),
			StructLogs:  ethapi.FormatLogs(tracer.StructLogs()),
		}, nil

	case *tracers.Tracer:
		return tracer.GetResult()

	default:
		panic(fmt.Sprintf("bad tracer type %T", tracer))
	}
}
