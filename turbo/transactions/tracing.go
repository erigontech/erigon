package transactions

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
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
func ComputeTxEnv(ctx context.Context, blockGetter BlockGetter, cfg *params.ChainConfig, getHeader func(hash common.Hash, number uint64) *types.Header, checkTEVM func(common.Hash) (bool, error), engine consensus.Engine, dbtx ethdb.Tx, blockHash common.Hash, txIndex uint64) (core.Message, vm.BlockContext, vm.TxContext, *state.IntraBlockState, *state.PlainKVState, error) {
	// Create the parent state database
	block, err := blockGetter.GetBlockByHash(blockHash)
	if err != nil {
		return nil, vm.BlockContext{}, vm.TxContext{}, nil, nil, err
	}
	if block == nil {
		return nil, vm.BlockContext{}, vm.TxContext{}, nil, nil, fmt.Errorf("block %x not found", blockHash)
	}
	parent := blockGetter.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		return nil, vm.BlockContext{}, vm.TxContext{}, nil, nil, fmt.Errorf("parent %x not found", block.ParentHash())
	}

	reader := state.NewPlainKvState(dbtx, parent.NumberU64())
	statedb := state.New(reader)

	if txIndex == 0 && len(block.Transactions()) == 0 {
		return nil, vm.BlockContext{}, vm.TxContext{}, statedb, reader, nil
	}
	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, block.NumberU64())

	for idx, tx := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return nil, vm.BlockContext{}, vm.TxContext{}, nil, nil, ctx.Err()
		}
		statedb.Prepare(tx.Hash(), blockHash, idx)

		// Assemble the transaction call message and return if the requested offset
		msg, _ := tx.AsMessage(*signer, block.Header().BaseFee)
		BlockContext := core.NewEVMBlockContext(block.Header(), getHeader, engine, nil, checkTEVM)
		TxContext := core.NewEVMTxContext(msg)
		if idx == int(txIndex) {
			return msg, BlockContext, TxContext, statedb, reader, nil
		}
		// Not yet the searched for transaction, execute on top of the current state
		vmenv := vm.NewEVM(BlockContext, TxContext, statedb, cfg, vm.Config{})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.GetGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return nil, vm.BlockContext{}, vm.TxContext{}, nil, nil, fmt.Errorf("transaction %x failed: %v", tx.Hash(), err)
		}
		// Ensure any modifications are committed to the state
		// Only delete empty objects if EIP158/161 (a.k.a Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(vmenv.ChainConfig().WithEIPsFlags(context.Background(), block.NumberU64()), state.NewNoopWriter())
	}
	return nil, vm.BlockContext{}, vm.TxContext{}, nil, nil, fmt.Errorf("transaction index %d out of range for block %x", txIndex, blockHash)
}

// TraceTx configures a new tracer according to the provided configuration, and
// executes the given message in the provided environment. The return value will
// be tracer dependent.
func TraceTx(
	ctx context.Context,
	message core.Message,
	blockCtx vm.BlockContext,
	txCtx vm.TxContext,
	ibs vm.IntraBlockState,
	config *tracers.TraceConfig,
	chainConfig *params.ChainConfig,
	stream *jsoniter.Stream,
) error {
	// Assemble the structured logger or the JavaScript tracer
	var (
		tracer vm.Tracer
		err    error
	)
	var streaming bool
	switch {
	case config != nil && config.Tracer != nil:
		// Define a meaningful timeout of a single transaction trace
		timeout := defaultTraceTimeout
		if config.Timeout != nil {
			if timeout, err = time.ParseDuration(*config.Timeout); err != nil {
				return err
			}
		}
		// Constuct the JavaScript tracer to execute with
		if tracer, err = tracers.New(*config.Tracer, txCtx); err != nil {
			return err
		}
		// Handle timeouts and RPC cancellations
		deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
		go func() {
			<-deadlineCtx.Done()
			tracer.(*tracers.Tracer).Stop(errors.New("execution timeout"))
		}()
		defer cancel()
		streaming = false

	case config == nil:
		tracer = NewJsonStreamLogger(nil, ctx, stream)
		streaming = true

	default:
		tracer = NewJsonStreamLogger(config.LogConfig, ctx, stream)
		streaming = true
	}
	// Run the transaction with tracing enabled.
	vmenv := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})

	var refunds bool = true
	if config != nil && config.NoRefunds != nil && *config.NoRefunds {
		refunds = false
	}
	if streaming {
		stream.WriteObjectStart()
		stream.WriteObjectField("structLogs")
		stream.WriteArrayStart()
	}
	result, err := core.ApplyMessage(vmenv, message, new(core.GasPool).AddGas(message.Gas()), refunds, false /* gasBailout */)
	if err != nil {
		return fmt.Errorf("tracing failed: %v", err)
	}
	// Depending on the tracer type, format and return the output
	if streaming {
		stream.WriteArrayEnd()
		stream.WriteMore()
		stream.WriteObjectField("gas")
		stream.WriteUint64(result.UsedGas)
		stream.WriteMore()
		stream.WriteObjectField("failed")
		stream.WriteBool(result.Failed())
		stream.WriteMore()
		stream.WriteObjectField("returnValue")
		stream.WriteString(fmt.Sprintf("%x", result.Return()))
		stream.WriteObjectEnd()
	} else {
		if r, err1 := tracer.(*tracers.Tracer).GetResult(); err1 == nil {
			stream.Write(r)
		} else {
			return err1
		}
	}
	return nil
}

// StructLogger is an EVM state logger and implements Tracer.
//
// StructLogger can capture state based on the given Log configuration and also keeps
// a track record of modified storage which is used in reporting snapshots of the
// contract their storage.
type JsonStreamLogger struct {
	ctx          context.Context
	cfg          vm.LogConfig
	stream       *jsoniter.Stream
	firstCapture bool

	storage map[common.Address]vm.Storage
	logs    []vm.StructLog
	output  []byte //nolint
	err     error  //nolint
}

// NewStructLogger returns a new logger
func NewJsonStreamLogger(cfg *vm.LogConfig, ctx context.Context, stream *jsoniter.Stream) *JsonStreamLogger {
	logger := &JsonStreamLogger{
		ctx:          ctx,
		stream:       stream,
		storage:      make(map[common.Address]vm.Storage),
		firstCapture: true,
	}
	if cfg != nil {
		logger.cfg = *cfg
	}
	return logger
}

// CaptureStart implements the Tracer interface to initialize the tracing operation.
func (l *JsonStreamLogger) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, codeHash common.Hash) error {
	return nil
}

// CaptureState logs a new structured log message and pushes it out to the environment
//
// CaptureState also tracks SLOAD/SSTORE ops to track storage change.
func (l *JsonStreamLogger) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	select {
	case <-l.ctx.Done():
		return fmt.Errorf("interrupted")
	default:
	}
	// check if already accumulated the specified number of logs
	if l.cfg.Limit != 0 && l.cfg.Limit <= len(l.logs) {
		return vm.ErrTraceLimitReached
	}
	if !l.firstCapture {
		l.stream.WriteMore()
	} else {
		l.firstCapture = false
	}
	if !l.cfg.DisableStorage {
		// initialise new changed values storage container for this contract
		// if not present.
		if l.storage[contract.Address()] == nil {
			l.storage[contract.Address()] = make(vm.Storage)
		}
		// capture SLOAD opcodes and record the read entry in the local storage
		if op == vm.SLOAD && stack.Len() >= 1 {
			var (
				address = common.Hash(stack.Data[stack.Len()-1].Bytes32())
				value   uint256.Int
			)
			env.IntraBlockState.GetState(contract.Address(), &address, &value)
			l.storage[contract.Address()][address] = common.Hash(value.Bytes32())
		}
		// capture SSTORE opcodes and record the written entry in the local storage.
		if op == vm.SSTORE && stack.Len() >= 2 {
			var (
				value   = common.Hash(stack.Data[stack.Len()-2].Bytes32())
				address = common.Hash(stack.Data[stack.Len()-1].Bytes32())
			)
			l.storage[contract.Address()][address] = value
		}
	}
	// create a new snapshot of the EVM.
	l.stream.WriteObjectStart()
	l.stream.WriteObjectField("pc")
	l.stream.WriteUint64(pc)
	l.stream.WriteMore()
	l.stream.WriteObjectField("op")
	l.stream.WriteString(op.String())
	l.stream.WriteMore()
	l.stream.WriteObjectField("gas")
	l.stream.WriteUint64(gas)
	l.stream.WriteMore()
	l.stream.WriteObjectField("gasCost")
	l.stream.WriteUint64(cost)
	l.stream.WriteMore()
	l.stream.WriteObjectField("depth")
	l.stream.WriteInt(depth)
	if err != nil {
		l.stream.WriteMore()
		l.stream.WriteObjectField("error")
		l.stream.WriteString(err.Error())
	}
	if !l.cfg.DisableStack {
		l.stream.WriteMore()
		l.stream.WriteObjectField("stack")
		l.stream.WriteArrayStart()
		for i, stackValue := range stack.Data {
			if i > 0 {
				l.stream.WriteMore()
			}
			l.stream.WriteString(fmt.Sprintf("%x", stackValue.Bytes32()))
		}
		l.stream.WriteArrayEnd()
	}
	if !l.cfg.DisableMemory {
		memData := memory.Data()
		l.stream.WriteMore()
		l.stream.WriteObjectField("memory")
		l.stream.WriteArrayStart()
		for i := 0; i+32 <= len(memData); i += 32 {
			if i > 0 {
				l.stream.WriteMore()
			}
			l.stream.WriteString(fmt.Sprintf("%x", memData[i:i+32]))
		}
		l.stream.WriteArrayEnd()
	}
	if !l.cfg.DisableStorage {
		l.stream.WriteMore()
		l.stream.WriteObjectField("storage")
		l.stream.WriteObjectStart()
		first := true
		for i, storageValue := range l.storage[contract.Address()] {
			if first {
				first = false
			} else {
				l.stream.WriteMore()
			}
			l.stream.WriteObjectField(fmt.Sprintf("%x", i))
			l.stream.WriteString(fmt.Sprintf("%x", storageValue))
		}
		l.stream.WriteObjectEnd()
	}
	l.stream.WriteObjectEnd()
	return l.stream.Flush()
}

// CaptureFault implements the Tracer interface to trace an execution fault
// while running an opcode.
func (l *JsonStreamLogger) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (l *JsonStreamLogger) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}

func (l *JsonStreamLogger) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
}

func (l *JsonStreamLogger) CaptureAccountRead(account common.Address) error {
	return nil
}

func (l *JsonStreamLogger) CaptureAccountWrite(account common.Address) error {
	return nil
}
