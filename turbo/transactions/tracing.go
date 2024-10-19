// Copyright 2024 The Erigon Authors
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

package transactions

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/tracers"
	tracersConfig "github.com/erigontech/erigon/eth/tracers/config"
	"github.com/erigontech/erigon/eth/tracers/logger"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
)

type BlockGetter interface {
	// GetBlockByHash retrieves a block from the database by hash, caching it if found.
	GetBlockByHash(hash libcommon.Hash) (*types.Block, error)
	// GetBlock retrieves a block from the database by hash and number,
	// caching it if found.
	GetBlock(hash libcommon.Hash, number uint64) *types.Block
}

// ComputeBlockContext returns the execution environment of a certain block.
func ComputeBlockContext(ctx context.Context, engine consensus.EngineReader, header *types.Header, cfg *chain.Config,
	headerReader services.HeaderReader, txNumsReader rawdbv3.TxNumsReader, dbtx kv.Tx,
	txIndex int) (*state.IntraBlockState, evmtypes.BlockContext, state.StateReader, *chain.Rules, *types.Signer, error) {
	reader, err := rpchelper.CreateHistoryStateReader(dbtx, txNumsReader, header.Number.Uint64(), txIndex, cfg.ChainName)
	if err != nil {
		return nil, evmtypes.BlockContext{}, nil, nil, nil, err
	}

	// Create the parent state database
	statedb := state.New(reader)

	getHeader := func(hash libcommon.Hash, n uint64) *types.Header {
		h, _ := headerReader.HeaderByNumber(ctx, dbtx, n)
		return h
	}

	blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil, cfg)
	rules := cfg.Rules(blockContext.BlockNumber, blockContext.Time)

	// Recompute transactions up to the target index.
	signer := types.MakeSigner(cfg, header.Number.Uint64(), header.Time)

	return statedb, blockContext, reader, rules, signer, err
}

// ComputeTxContext returns the execution environment of a certain transaction.
func ComputeTxContext(statedb *state.IntraBlockState, engine consensus.EngineReader, rules *chain.Rules, signer *types.Signer, block *types.Block, cfg *chain.Config, txIndex int) (core.Message, evmtypes.TxContext, error) {
	txn := block.Transactions()[txIndex]
	statedb.SetTxContext(txIndex)
	msg, _ := txn.AsMessage(*signer, block.BaseFee(), rules)
	if msg.FeeCap().IsZero() && engine != nil {
		syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, cfg, statedb, block.HeaderNoCopy(), engine, true /* constCall */)
		}
		msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
	}

	TxContext := core.NewEVMTxContext(msg)
	return msg, TxContext, nil
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
	config *tracersConfig.TraceConfig,
	chainConfig *chain.Config,
	stream *jsoniter.Stream,
	callTimeout time.Duration,
) error {
	tracer, streaming, cancel, err := AssembleTracer(ctx, config, txCtx.TxHash, stream, callTimeout)
	if err != nil {
		stream.WriteNil()
		return err
	}

	defer cancel()

	execCb := func(evm *vm.EVM, refunds bool) (*evmtypes.ExecutionResult, error) {
		gp := new(core.GasPool).AddGas(message.Gas()).AddBlobGas(message.BlobGas())
		return core.ApplyMessage(evm, message, gp, refunds, false /* gasBailout */)
	}

	return ExecuteTraceTx(blockCtx, txCtx, ibs, config, chainConfig, stream, tracer, streaming, execCb)
}

func AssembleTracer(
	ctx context.Context,
	config *tracersConfig.TraceConfig,
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
		tracer, err := tracers.New(*config.Tracer, &tracers.Context{TxHash: txHash}, cfg)
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
	config *tracersConfig.TraceConfig,
	chainConfig *chain.Config,
	stream *jsoniter.Stream,
	tracer vm.EVMLogger,
	streaming bool,
	execCb func(evm *vm.EVM, refunds bool) (*evmtypes.ExecutionResult, error),
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
