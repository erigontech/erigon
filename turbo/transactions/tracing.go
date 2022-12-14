package transactions

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/kv"
	state2 "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type BlockGetter interface {
	// GetBlockByHash retrieves a block from the database by hash, caching it if found.
	GetBlockByHash(hash common.Hash) (*types.Block, error)
	// GetBlock retrieves a block from the database by hash and number,
	// caching it if found.
	GetBlock(hash common.Hash, number uint64) *types.Block
}

// ComputeTxEnv returns the execution environment of a certain transaction.
func ComputeTxEnv(ctx context.Context, engine consensus.EngineReader, block *types.Block, cfg *params.ChainConfig, headerReader services.HeaderReader, dbtx kv.Tx, txIndex uint64, agg *state2.Aggregator22, historyV3 bool) (core.Message, evmtypes.BlockContext, evmtypes.TxContext, *state.IntraBlockState, state.StateReader, error) {
	header := block.HeaderNoCopy()
	reader, err := rpchelper.CreateHistoryStateReader(dbtx, block.NumberU64(), txIndex, agg, historyV3, cfg.ChainName)
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
	}
	if historyV3 {
		ibs := state.New(reader)
		if txIndex == 0 && len(block.Transactions()) == 0 {
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, reader, nil
		}
		if int(txIndex) > block.Transactions().Len() {
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, reader, nil
		}
		txn := block.Transactions()[txIndex]
		signer := types.MakeSigner(cfg, block.NumberU64())
		msg, _ := txn.AsMessage(*signer, header.BaseFee, cfg.Rules(block.NumberU64(), block.Time()))
		blockCtx := NewEVMBlockContext(engine, header, true /* requireCanonical */, dbtx, headerReader)
		txCtx := core.NewEVMTxContext(msg)
		return msg, blockCtx, txCtx, ibs, reader, nil

	}

	getHeader := func(hash common.Hash, n uint64) *types.Header {
		h, _ := headerReader.HeaderByNumber(ctx, dbtx, n)
		return h
	}

	// Create the parent state database
	statedb := state.New(reader)

	if txIndex == 0 && len(block.Transactions()) == 0 {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, statedb, reader, nil
	}
	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, block.NumberU64())

	BlockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil)
	vmenv := vm.NewEVM(BlockContext, evmtypes.TxContext{}, statedb, cfg, vm.Config{})
	rules := vmenv.ChainRules()

	consensusHeaderReader := stagedsync.NewChainReaderImpl(cfg, dbtx, nil)

	core.InitializeBlockExecution(engine.(consensus.Engine), consensusHeaderReader, nil, header, block.Transactions(), block.Uncles(), cfg, statedb)

	for idx, tx := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, ctx.Err()
		}
		statedb.Prepare(tx.Hash(), block.Hash(), idx)

		// Assemble the transaction call message and return if the requested offset
		msg, _ := tx.AsMessage(*signer, block.BaseFee(), rules)
		if msg.FeeCap().IsZero() && engine != nil {
			syscall := func(contract common.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *cfg, statedb, header, engine, true /* constCall */)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		TxContext := core.NewEVMTxContext(msg)
		if idx == int(txIndex) {
			return msg, BlockContext, TxContext, statedb, reader, nil
		}
		vmenv.Reset(TxContext, statedb)
		// Not yet the searched for transaction, execute on top of the current state
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.GetGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, fmt.Errorf("transaction %x failed: %w", tx.Hash(), err)
		}
		// Ensure any modifications are committed to the state
		// Only delete empty objects if EIP161 (part of Spurious Dragon) is in effect
		_ = statedb.FinalizeTx(rules, reader.(*state.PlainState))

		if idx+1 == len(block.Transactions()) {
			// Return the state from evaluating all txs in the block, note no msg or TxContext in this case
			return nil, BlockContext, evmtypes.TxContext{}, statedb, reader, nil
		}
	}
	return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, fmt.Errorf("transaction index %d out of range for block %x", txIndex, block.Hash())
}

// TraceTx configures a new tracer according to the provided configuration, and
// executes the given message in the provided environment. The return value will
// be tracer dependent.
func TraceTx(
	ctx context.Context,
	message core.Message,
	blockCtx evmtypes.BlockContext,
	txCtx evmtypes.TxContext,
	ibs evmtypes.IntraBlockState,
	config *tracers.TraceConfig,
	chainConfig *params.ChainConfig,
	stream *jsoniter.Stream,
	callTimeout time.Duration,
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
		timeout := callTimeout
		if config.Timeout != nil {
			if timeout, err = time.ParseDuration(*config.Timeout); err != nil {
				stream.WriteNil()
				return err
			}
		}
		// Construct the JavaScript tracer to execute with
		if tracer, err = tracers.New(*config.Tracer, &tracers.Context{
			TxHash: txCtx.TxHash,
		}); err != nil {
			stream.WriteNil()
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
	var refunds = true
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
		if streaming {
			stream.WriteArrayEnd()
			stream.WriteObjectEnd()
		}
		return fmt.Errorf("tracing failed: %w", err)
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
		// If the result contains a revert reason, return it.
		returnVal := hex.EncodeToString(result.Return())
		if len(result.Revert()) > 0 {
			returnVal = hex.EncodeToString(result.Revert())
		}
		stream.WriteObjectField("returnValue")
		stream.WriteString(returnVal)
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
	hexEncodeBuf [128]byte
	firstCapture bool

	locations common.Hashes // For sorting
	storage   map[common.Address]vm.Storage
	logs      []vm.StructLog
	output    []byte //nolint
	err       error  //nolint
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
func (l *JsonStreamLogger) CaptureStart(env *vm.EVM, depth int, from common.Address, to common.Address, precompile bool, create bool, callType vm.CallType, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

// CaptureState logs a new structured log message and pushes it out to the environment
//
// CaptureState also tracks SLOAD/SSTORE ops to track storage change.
func (l *JsonStreamLogger) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	contract := scope.Contract
	memory := scope.Memory
	stack := scope.Stack

	select {
	case <-l.ctx.Done():
		return
	default:
	}
	// check if already accumulated the specified number of logs
	if l.cfg.Limit != 0 && l.cfg.Limit <= len(l.logs) {
		return
	}
	if !l.firstCapture {
		l.stream.WriteMore()
	} else {
		l.firstCapture = false
	}
	var outputStorage bool
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
			env.IntraBlockState().GetState(contract.Address(), &address, &value)
			l.storage[contract.Address()][address] = value.Bytes32()
			outputStorage = true
		}
		// capture SSTORE opcodes and record the written entry in the local storage.
		if op == vm.SSTORE && stack.Len() >= 2 {
			var (
				value   = common.Hash(stack.Data[stack.Len()-2].Bytes32())
				address = common.Hash(stack.Data[stack.Len()-1].Bytes32())
			)
			l.storage[contract.Address()][address] = value
			outputStorage = true
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
		l.stream.WriteObjectStart()
		l.stream.WriteObjectEnd()
		//l.stream.WriteString(err.Error())
	}
	if !l.cfg.DisableStack {
		l.stream.WriteMore()
		l.stream.WriteObjectField("stack")
		l.stream.WriteArrayStart()
		for i, stackValue := range stack.Data {
			if i > 0 {
				l.stream.WriteMore()
			}
			l.stream.WriteString(stackValue.String())
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
			l.stream.WriteString(string(l.hexEncodeBuf[0:hex.Encode(l.hexEncodeBuf[:], memData[i:i+32])]))
		}
		l.stream.WriteArrayEnd()
	}
	if outputStorage {
		l.stream.WriteMore()
		l.stream.WriteObjectField("storage")
		l.stream.WriteObjectStart()
		first := true
		// Sort storage by locations for easier comparison with geth
		if l.locations != nil {
			l.locations = l.locations[:0]
		}
		s := l.storage[contract.Address()]
		for loc := range s {
			l.locations = append(l.locations, loc)
		}
		sort.Sort(l.locations)
		for _, loc := range l.locations {
			value := s[loc]
			if first {
				first = false
			} else {
				l.stream.WriteMore()
			}
			l.stream.WriteObjectField(string(l.hexEncodeBuf[0:hex.Encode(l.hexEncodeBuf[:], loc[:])]))
			l.stream.WriteString(string(l.hexEncodeBuf[0:hex.Encode(l.hexEncodeBuf[:], value[:])]))
		}
		l.stream.WriteObjectEnd()
	}
	l.stream.WriteObjectEnd()
	_ = l.stream.Flush()
}

// CaptureFault implements the Tracer interface to trace an execution fault
// while running an opcode.
func (l *JsonStreamLogger) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (l *JsonStreamLogger) CaptureEnd(depth int, output []byte, startGas, endGas uint64, t time.Duration, err error) {
}

func (l *JsonStreamLogger) CaptureSelfDestruct(from common.Address, to common.Address, value *uint256.Int) {
}

func (l *JsonStreamLogger) CaptureAccountRead(account common.Address) error {
	return nil
}

func (l *JsonStreamLogger) CaptureAccountWrite(account common.Address) error {
	return nil
}
