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

package exec3

import (
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/aa"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/transactions"
)

type GenericTracer interface {
	TracingHooks() *tracing.Hooks
	SetTransaction(tx types.Transaction)
	Found() bool
}

type Resetable interface {
	Reset()
}

type TraceWorker struct {
	stateReader  *state.HistoryReaderV3
	engine       consensus.EngineReader
	headerReader services.HeaderReader
	tx           kv.Getter
	chainConfig  *chain.Config
	tracer       GenericTracer
	ibs          *state.IntraBlockState
	evm          *vm.EVM

	// calculated by .changeBlock()
	blockHash common.Hash
	blockNum  uint64
	header    *types.Header
	blockCtx  *evmtypes.BlockContext
	rules     *chain.Rules
	signer    *types.Signer
	vmConfig  *vm.Config
}

func NewTraceWorker(tx kv.TemporalTx, cc *chain.Config, engine consensus.EngineReader, br services.HeaderReader, tracer GenericTracer) *TraceWorker {
	stateReader := state.NewHistoryReaderV3()
	stateReader.SetTx(tx)

	ie := &TraceWorker{
		tx:           tx,
		engine:       engine,
		chainConfig:  cc,
		headerReader: br,
		stateReader:  stateReader,
		tracer:       tracer,
		evm:          vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, cc, vm.Config{}),
		vmConfig:     &vm.Config{NoBaseFee: true},
		ibs:          state.New(stateReader),
	}
	if tracer != nil {
		ie.vmConfig.Tracer = tracer.TracingHooks()
	}
	return ie
}

func (e *TraceWorker) Close() {
	e.evm.Config().JumpDestCache.LogStats()
}

func (e *TraceWorker) ChangeBlock(header *types.Header) {
	e.blockNum = header.Number.Uint64()
	blockCtx := transactions.NewEVMBlockContext(e.engine, header, true /* requireCanonical */, e.tx, e.headerReader, e.evm.ChainConfig())
	e.blockCtx = &blockCtx
	e.blockHash = header.Hash()
	e.header = header
	e.rules = blockCtx.Rules(e.chainConfig)
	e.signer = types.MakeSigner(e.chainConfig, e.blockNum, header.Time)
	e.vmConfig.SkipAnalysis = core.SkipAnalysis(e.chainConfig, e.blockNum)
}

func (e *TraceWorker) GetRawLogs(txIdx int) types.Logs { return e.ibs.GetRawLogs(txIdx) }
func (e *TraceWorker) GetLogs(txIndex int, txnHash common.Hash, blockNumber uint64, blockHash common.Hash) []*types.Log {
	return e.ibs.GetLogs(txIndex, txnHash, blockNumber, blockHash)
}

func (e *TraceWorker) ExecTxn(txNum uint64, txIndex int, txn types.Transaction, gasBailout bool) error {
	e.stateReader.SetTxNum(txNum)
	e.ibs.Reset()
	e.ibs.SetTxContext(e.blockNum, txIndex)

	msg, err := txn.AsMessage(*e.signer, e.header.BaseFee, e.rules)
	if txn.Type() != types.AccountAbstractionTxType && err != nil {
		return err
	}
	msg.SetCheckNonce(!e.vmConfig.StatelessExec)

	txContext := core.NewEVMTxContext(msg)
	if e.vmConfig.TraceJumpDest {
		txContext.TxHash = txn.Hash()
	}
	e.evm.ResetBetweenBlocks(*e.blockCtx, txContext, e.ibs, *e.vmConfig, e.rules)

	gp := new(core.GasPool).AddGas(txn.GetGasLimit()).AddBlobGas(txn.GetBlobGas())

	if txn.Type() == types.AccountAbstractionTxType {
		aaTxn := txn.(*types.AccountAbstractionTransaction)
		evm := vm.NewEVM(*e.blockCtx, txContext, e.ibs, e.chainConfig, *e.vmConfig)
		paymasterContext, validationGasUsed, err := aa.ValidateAATransaction(aaTxn, e.ibs, gp, e.header, evm, e.chainConfig)
		if err != nil {
			return err
		}

		_, _, err = aa.ExecuteAATransaction(aaTxn, paymasterContext, validationGasUsed, gp, evm, e.header, e.ibs)
		if err != nil {
			return err
		}
	} else {
		result, err := core.ApplyMessage(e.evm, msg, gp, true /* refunds */, gasBailout /* gasBailout */, e.engine)
		if err != nil {
			if result == nil {
				return fmt.Errorf("%w: blockNum=%d, txNum=%d", err, e.blockNum, txNum)
			}
			return fmt.Errorf("%w: blockNum=%d, txNum=%d, %s", err, e.blockNum, txNum, result.Err)
		}
	}

	e.ibs.SoftFinalise()
	if e.vmConfig.Tracer != nil {
		if e.tracer.Found() {
			e.tracer.SetTransaction(txn)
		}
	}

	return nil
}
