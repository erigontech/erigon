package transactions

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

type BlockGetter interface {
	// GetBlockByHash retrieves a block from the database by hash, caching it if found.
	GetBlockByHash(hash libcommon.Hash) (*types.Block, error)
	// GetBlock retrieves a block from the database by hash and number,
	// caching it if found.
	GetBlock(hash libcommon.Hash, number uint64) *types.Block
}

// ComputeTxEnv returns the execution environment of a certain transaction.
func ComputeTxEnv(ctx context.Context, engine consensus.EngineReader, block *types.Block, cfg *chain.Config, headerReader services.HeaderReader, dbtx kv.Tx, txIndex int, historyV3 bool) (core.Message, evmtypes.BlockContext, evmtypes.TxContext, *state.IntraBlockState, state.StateReader, error) {
	reader, err := rpchelper.CreateHistoryStateReader(dbtx, block.NumberU64(), txIndex, historyV3, cfg.ChainName)
	if err != nil {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, err
	}

	logger := log.New()
	// Create the parent state database
	statedb := state.New(reader)

	if txIndex == 0 && len(block.Transactions()) == 0 {
		return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, statedb, reader, nil
	}
	getHeader := func(hash libcommon.Hash, n uint64) *types.Header {
		h, _ := headerReader.HeaderByNumber(ctx, dbtx, n)
		return h
	}
	header := block.HeaderNoCopy()

	BlockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil)

	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, block.NumberU64(), 0)
	if historyV3 {
		rules := cfg.Rules(BlockContext.BlockNumber, BlockContext.Time)
		txn := block.Transactions()[txIndex]
		statedb.Init(txn.Hash(), block.Hash(), txIndex)
		msg, _ := txn.AsMessage(*signer, block.BaseFee(), rules)
		if msg.FeeCap().IsZero() && engine != nil {
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, cfg, statedb, header, engine, true /* constCall */)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		TxContext := core.NewEVMTxContext(msg)
		return msg, BlockContext, TxContext, statedb, reader, nil
	}
	vmenv := vm.NewEVM(BlockContext, evmtypes.TxContext{}, statedb, cfg, vm.Config{})
	rules := vmenv.ChainRules()

	consensusHeaderReader := stagedsync.NewChainReaderImpl(cfg, dbtx, nil, logger)

	core.InitializeBlockExecution(engine.(consensus.Engine), consensusHeaderReader, header, cfg, statedb, logger)

	hermezReader := hermez_db.NewHermezDbReader(dbtx)

	for idx, txn := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, ctx.Err()
		}
		statedb.Init(txn.Hash(), block.Hash(), idx)

		// Assemble the transaction call message and return if the requested offset
		msg, _ := txn.AsMessage(*signer, block.BaseFee(), rules)

		effectiveGasPricePercentage, _ := hermezReader.GetEffectiveGasPricePercentage(txn.Hash())
		msg.SetEffectiveGasPricePercentage(effectiveGasPricePercentage)

		if msg.FeeCap().IsZero() && engine != nil {
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, cfg, statedb, header, engine, true /* constCall */)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		TxContext := core.NewEVMTxContext(msg)
		if idx == txIndex {
			return msg, BlockContext, TxContext, statedb, reader, nil
		}
		vmenv.Reset(TxContext, statedb)
		// Not yet the searched for transaction, execute on top of the current state
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(txn.GetGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return nil, evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, nil, fmt.Errorf("transaction %x failed: %w", txn.Hash(), err)
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
	config *tracers.TraceConfig_ZkEvm,
	chainConfig *chain.Config,
	stream *jsoniter.Stream,
	callTimeout time.Duration,
) error {
	// Assemble the structured logger or the JavaScript tracer
	var (
		tracer vm.EVMLogger
		err    error
	)
	var streaming bool

	var counterCollector *vm.TransactionCounter
	var executionCounters *vm.CounterCollector
	if config != nil {
		counterCollector = config.CounterCollector
		if counterCollector != nil {
			executionCounters = counterCollector.ExecutionCounters()
		}
	}
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
		cfg := json.RawMessage("{}")
		if config != nil && config.TracerConfig != nil {
			cfg = *config.TracerConfig
		}
		if tracer, err = tracers.New(*config.Tracer, &tracers.Context{
			TxHash:            txCtx.TxHash,
			Txn:               txCtx.Txn,
			CumulativeGasUsed: txCtx.CumulativeGasUsed,
			BlockNum:          blockCtx.BlockNumber,
		}, cfg); err != nil {
			stream.WriteNil()
			return err
		}
		// Handle timeouts and RPC cancellations
		deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
		go func() {
			<-deadlineCtx.Done()
			tracer.(tracers.Tracer).Stop(errors.New("execution timeout"))
		}()
		defer cancel()
		streaming = false

	case config == nil:
		tracer = logger.NewJsonStreamLogger_ZkEvm(nil, ctx, stream, executionCounters)
		streaming = true

	default:
		tracer = logger.NewJsonStreamLogger_ZkEvm(config.LogConfig, ctx, stream, executionCounters)
		streaming = true
	}

	zkConfig := vm.NewZkConfig(vm.Config{Debug: true, Tracer: tracer}, executionCounters)

	// Run the transaction with tracing enabled.
	vmenv := vm.NewZkEVM(blockCtx, txCtx, ibs, chainConfig, zkConfig)
	var refunds = true
	if config != nil && config.NoRefunds != nil && *config.NoRefunds {
		refunds = false
	}
	if streaming {
		stream.WriteObjectStart()

		if executionCounters != nil {
			stream.WriteObjectField("smtLevels")
			stream.WriteInt(executionCounters.GetSmtLevels())
			stream.WriteMore()
		}

		stream.WriteObjectField("structLogs")
		stream.WriteArrayStart()
	}

	var result *core.ExecutionResult
	result, err = core.ApplyMessage(vmenv, message, new(core.GasPool).AddGas(message.Gas()), refunds, false /* gasBailout */)

	if err != nil {
		if streaming {
			stream.WriteArrayEnd()
			stream.WriteObjectEnd()
			stream.WriteMore()
			stream.WriteObjectField("resultHack") // higher-level func will assing it to NULL
		} else {
			stream.WriteNil()
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

		if config != nil && config.CounterCollector != nil {
			differences := config.CounterCollector.CombineCounters().UsedAsMap()

			stream.WriteMore()
			stream.WriteObjectField("counters")
			stream.WriteObjectStart()
			first := true
			for key, value := range differences {
				if first {
					first = false
				} else {
					stream.WriteMore()
				}
				stream.WriteObjectField(key)
				stream.WriteInt(value)
			}
			stream.WriteObjectEnd()
		}
		stream.WriteObjectEnd()
	} else {
		if r, err1 := tracer.(tracers.Tracer).GetResult(); err1 == nil {
			stream.Write(r)
		} else {
			return err1
		}
	}
	return nil
}

func prepareCallMessage(msg core.Message) ethereum.CallMsg {
	return ethereum.CallMsg{
		From:       msg.From(),
		To:         msg.To(),
		Gas:        msg.Gas(),
		GasPrice:   msg.GasPrice(),
		Value:      msg.Value(),
		Data:       msg.Data(),
		AccessList: msg.AccessList(),
	}
}

func AssembleTracer(
	ctx context.Context,
	config *tracers.TraceConfig,
	txHash libcommon.Hash,
	stream *jsoniter.Stream,
	callTimeout time.Duration,
) (vm.EVMLogger, bool, context.CancelFunc, error) {
	// Assemble the structured logger or the JavaScript tracer
	switch {
	case config != nil && config.Tracer != nil:
		// Define a meaningful timeout of a single transaction trace
		timeout := callTimeout
		if config.Timeout != nil {
			var err error
			timeout, err = time.ParseDuration(*config.Timeout)
			if err != nil {
				return nil, false, func() {}, err
			}
		}

		// Construct the JavaScript tracer to execute with
		cfg := json.RawMessage("{}")
		if config != nil && config.TracerConfig != nil {
			cfg = *config.TracerConfig
		}
		tracer, err := tracers.New(*config.Tracer, &tracers.Context{
			TxHash: txHash,
		}, cfg)
		if err != nil {
			return nil, false, func() {}, err
		}

		// Handle timeouts and RPC cancellations
		deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
		go func() {
			<-deadlineCtx.Done()
			tracer.Stop(errors.New("execution timeout"))
		}()

		return tracer, false, cancel, nil
	case config == nil:
		return logger.NewJsonStreamLogger(nil, ctx, stream), true, func() {}, nil
	default:
		return logger.NewJsonStreamLogger(config.LogConfig, ctx, stream), true, func() {}, nil
	}
}

func ExecuteTraceTx(
	blockCtx evmtypes.BlockContext,
	txCtx evmtypes.TxContext,
	ibs evmtypes.IntraBlockState,
	config *tracers.TraceConfig,
	chainConfig *chain.Config,
	stream *jsoniter.Stream,
	tracer vm.EVMLogger,
	streaming bool,
	execCb func(evm *vm.EVM, refunds bool) (*core.ExecutionResult, error),
) error {
	// Run the transaction with tracing enabled.
	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer, NoBaseFee: true})

	var refunds = true
	if config != nil && config.NoRefunds != nil && *config.NoRefunds {
		refunds = false
	}

	if streaming {
		stream.WriteObjectStart()
		stream.WriteObjectField("structLogs")
		stream.WriteArrayStart()
	}

	result, err := execCb(evm, refunds)
	if err != nil {
		if streaming {
			stream.WriteArrayEnd()
			stream.WriteObjectEnd()
		} else {
			stream.WriteNil()
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
		r, err := tracer.(tracers.Tracer).GetResult()
		if err != nil {
			stream.WriteNil()
			return err
		}

		_, err = stream.Write(r)
		if err != nil {
			stream.WriteNil()
			return err
		}
	}

	return nil
}
