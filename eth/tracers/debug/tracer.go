// Copyright 2025 The Erigon Authors
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

package debug

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

type Tracer struct {
	outputDir     string
	flushMode     FlushMode
	recordOptions RecordOptions
	wrapped       *tracers.Tracer
	traces        Traces
	currentBlock  *types.Block
}

func New(outputDir string, opts ...Option) *tracers.Tracer {
	t := &Tracer{
		outputDir: outputDir,
		flushMode: FlushModeBlock,
	}

	for _, opt := range opts {
		opt(t)
	}

	return &tracers.Tracer{
		Hooks:     t.Hooks(),
		GetResult: t.GetResult,
		Stop:      t.Stop,
	}
}

func (t *Tracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		// VM events
		OnTxStart:   t.OnTxStart,
		OnTxEnd:     t.OnTxEnd,
		OnEnter:     t.OnEnter,
		OnExit:      t.OnExit,
		OnOpcode:    t.OnOpcode,
		OnFault:     t.OnFault,
		OnGasChange: t.OnGasChange,
		// Chain events
		OnBlockchainInit:  t.OnBlockchainInit,
		OnBlockStart:      t.OnBlockStart,
		OnBlockEnd:        t.OnBlockEnd,
		OnGenesisBlock:    t.OnGenesisBlock,
		OnSystemCallStart: t.OnSystemCallStart,
		OnSystemCallEnd:   t.OnSystemCallEnd,
		// State events
		OnBalanceChange: t.OnBalanceChange,
		OnNonceChange:   t.OnNonceChange,
		OnCodeChange:    t.OnCodeChange,
		OnStorageChange: t.OnStorageChange,
		OnLog:           t.OnLog,
	}
}

func (t *Tracer) OnTxStart(vm *tracing.VMContext, txn types.Transaction, from common.Address) {
	if t.recordOptions.DisableOnTxStartRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnTxStart != nil {
		t.wrapped.OnTxStart(vm, txn, from)
	}

	t.traces.Append(Trace{
		OnTxStart: &OnTxStartTrace{
			VMContext:   vm,
			Transaction: txn,
			From:        from,
		},
	})
}

func (t *Tracer) OnTxEnd(receipt *types.Receipt, err error) {
	if t.recordOptions.DisableOnTxEndRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnTxEnd != nil {
		t.wrapped.OnTxEnd(receipt, err)
	}

	var errStr string
	if err != nil {
		errStr = err.Error()
	}

	t.traces.Append(Trace{
		OnTxEnd: &OnTxEndTrace{
			Receipt: receipt,
			Error:   errStr,
		},
	})

	if t.flushMode != FlushModeTxn {
		return
	}

	txnTraceFile := fmt.Sprintf("txn_trace_%d_%s_%d_%s.json", receipt.TransactionIndex, receipt.TxHash, receipt.BlockNumber, receipt.BlockHash)
	t.mustFlushToFile(path.Join(t.outputDir, txnTraceFile))
}

func (t *Tracer) OnEnter(depth int, typ byte, from, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if t.recordOptions.DisableOnEnterRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnEnter != nil {
		t.wrapped.OnEnter(depth, typ, from, to, precompile, input, gas, value, code)
	}

	inputCopy := make([]byte, len(input))
	copy(inputCopy, input)
	t.traces.Append(Trace{
		OnEnter: &OnEnterTrace{
			Depth:      depth,
			Type:       typ,
			From:       from,
			To:         to,
			Precompile: precompile,
			Input:      inputCopy,
			Gas:        gas,
			Value:      value,
			Code:       code,
		},
	})
}

func (t *Tracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if t.recordOptions.DisableOnExitRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnExit != nil {
		t.wrapped.OnExit(depth, output, gasUsed, err, reverted)
	}

	var errStr string
	if err != nil {
		errStr = err.Error()
	}

	t.traces.Append(Trace{
		OnExit: &OnExitTrace{
			Depth:    depth,
			Output:   output,
			GasUsed:  gasUsed,
			Error:    errStr,
			Reverted: reverted,
		},
	})
}

func (t *Tracer) OnOpcode(pc uint64, op byte, gas, cost uint64, opContext tracing.OpContext, returnData []byte, depth int, err error) {
	if t.recordOptions.DisableOnOpcodeRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnOpcode != nil {
		t.wrapped.OnOpcode(pc, op, gas, cost, opContext, returnData, depth, err)
	}

	var memory hexutil.Bytes
	if !t.recordOptions.DisableOnOpcodeMemoryRecording {
		data := opContext.MemoryData()
		memory = make(hexutil.Bytes, len(data))
		copy(memory, data)
	}

	var stack []hexutil.Bytes
	if !t.recordOptions.DisableOnOpcodeStackRecording {
		data := opContext.StackData()
		stack = make([]hexutil.Bytes, len(data))
		for i, d := range data {
			stack[i] = d.Bytes()
		}
	}

	var errStr string
	if err != nil {
		errStr = err.Error()
	}

	t.traces.Append(Trace{
		OnOpcode: &OnOpcodeTrace{
			PC:         pc,
			Op:         fmt.Sprintf("%v", vm.OpCode(op)),
			Gas:        gas,
			Cost:       cost,
			Caller:     opContext.Caller(),
			Stack:      stack,
			Memory:     memory,
			MemorySize: len(memory),
			ReturnData: returnData,
			Depth:      depth,
			Error:      errStr,
		},
	})
}

func (t *Tracer) OnFault(pc uint64, op byte, gas, cost uint64, opContext tracing.OpContext, depth int, err error) {
	if t.recordOptions.DisableOnFaultRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnFault != nil {
		t.wrapped.OnFault(pc, op, gas, cost, opContext, depth, err)
	}

	var memory hexutil.Bytes
	if !t.recordOptions.DisableOnOpcodeMemoryRecording {
		data := opContext.MemoryData()
		memory = make(hexutil.Bytes, len(data))
		copy(memory, data)
	}

	var stack []hexutil.Bytes
	if !t.recordOptions.DisableOnOpcodeStackRecording {
		data := opContext.StackData()
		stack = make([]hexutil.Bytes, len(data))
		for i, d := range data {
			stack[i] = d.Bytes()
		}
	}

	var errStr string
	if err != nil {
		errStr = err.Error()
	}

	t.traces.Append(Trace{
		OnFault: &OnFaultTrace{
			PC:         pc,
			Op:         op,
			Gas:        gas,
			Cost:       cost,
			Caller:     opContext.Caller(),
			Stack:      stack,
			Memory:     memory,
			MemorySize: len(memory),
			Depth:      depth,
			Error:      errStr,
		},
	})
}

func (t *Tracer) OnGasChange(old, new uint64, reason tracing.GasChangeReason) {
	if t.recordOptions.DisableOnGasChangeRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnGasChange != nil {
		t.wrapped.OnGasChange(old, new, reason)
	}

	t.traces.Append(Trace{
		OnGasChange: &OnGasChangeTrace{
			OldGas: old,
			NewGas: new,
			Reason: fmt.Sprintf("%v", reason),
		},
	})
}

func (t *Tracer) OnBlockchainInit(chainConfig *chain.Config) {
	if t.recordOptions.DisableOnBlockchainInitRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnBlockchainInit != nil {
		t.wrapped.OnBlockchainInit(chainConfig)
	}

	t.traces.Append(Trace{
		OnBlockchainInit: &OnBlockchainInitTrace{
			ChainConfig: chainConfig,
		},
	})
}

func (t *Tracer) OnBlockStart(event tracing.BlockEvent) {
	if t.recordOptions.DisableOnBlockStartRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnBlockStart != nil {
		t.wrapped.OnBlockStart(event)
	}

	t.currentBlock = event.Block
	t.traces.Append(Trace{
		OnBlockStart: &OnBlockStartTrace{
			Event: event,
		},
	})
}

func (t *Tracer) OnBlockEnd(err error) {
	if t.recordOptions.DisableOnBlockEndRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnBlockEnd != nil {
		t.wrapped.OnBlockEnd(err)
	}

	var errStr string
	if err != nil {
		errStr = err.Error()
	}

	t.traces.Append(Trace{
		OnBlockEnd: &OnBlockEndTrace{
			Error: errStr,
		},
	})

	if t.currentBlock == nil || t.flushMode != FlushModeBlock {
		return
	}

	blockTraceFile := fmt.Sprintf("block_trace_%d_%s.json", t.currentBlock.NumberU64(), t.currentBlock.Hash())
	t.mustFlushToFile(path.Join(t.outputDir, blockTraceFile))
}

func (t *Tracer) OnGenesisBlock(genesis *types.Block, alloc types.GenesisAlloc) {
	if t.recordOptions.DisableOnGenesisBlockRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnGenesisBlock != nil {
		t.wrapped.OnGenesisBlock(genesis, alloc)
	}

	t.traces.Append(Trace{
		OnGenesisBlock: &OnGenesisBlockTrace{
			Header: genesis.Header(),
			Alloc:  alloc,
		},
	})
}

func (t *Tracer) OnSystemCallStart() {
	if t.recordOptions.DisableOnSystemCallStartRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnSystemCallStart != nil {
		t.wrapped.OnSystemCallStart()
	}

	t.traces.Append(Trace{
		OnSystemCallStart: &OnSystemCallStartTrace{},
	})
}

func (t *Tracer) OnSystemCallEnd() {
	if t.recordOptions.DisableOnSystemCallEndRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnSystemCallEnd != nil {
		t.wrapped.OnSystemCallEnd()
	}

	t.traces.Append(Trace{
		OnSystemCallEnd: &OnSystemCallEndTrace{},
	})
}

func (t *Tracer) OnBalanceChange(address common.Address, oldBalance, newBalance uint256.Int, reason tracing.BalanceChangeReason) {
	if t.recordOptions.DisableOnBalanceChangeRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnBalanceChange != nil {
		t.wrapped.OnBalanceChange(address, oldBalance, newBalance, reason)
	}

	t.traces.Append(Trace{
		OnBalanceChange: &OnBalanceChangeTrace{
			Address:    address,
			OldBalance: oldBalance,
			NewBalance: newBalance,
			Reason:     fmt.Sprintf("%v", reason),
		},
	})
}

func (t *Tracer) OnNonceChange(address common.Address, oldNonce, newNonce uint64) {
	if t.recordOptions.DisableOnNonceChangeRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnNonceChange != nil {
		t.wrapped.OnNonceChange(address, oldNonce, newNonce)
	}

	t.traces.Append(Trace{
		OnNonceChange: &OnNonceChangeTrace{
			Address:  address,
			OldNonce: oldNonce,
			NewNonce: newNonce,
		},
	})
}

func (t *Tracer) OnCodeChange(address common.Address, prevCodeHash common.Hash, prevCode []byte, newCodeHash common.Hash, newCode []byte) {
	if t.recordOptions.DisableOnCodeChangeRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnCodeChange != nil {
		t.wrapped.OnCodeChange(address, prevCodeHash, prevCode, newCodeHash, newCode)
	}

	t.traces.Append(Trace{
		OnCodeChange: &OnCodeChangeTrace{
			Address:      address,
			PrevCodeHash: prevCodeHash,
			PrevCode:     prevCode,
			NewCodeHash:  newCodeHash,
			NewCode:      newCode,
		},
	})
}

func (t *Tracer) OnStorageChange(address common.Address, slot common.Hash, prev, new uint256.Int) {
	if t.recordOptions.DisableOnStorageChangeRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnStorageChange != nil {
		t.wrapped.OnStorageChange(address, slot, prev, new)
	}

	t.traces.Append(Trace{
		OnStorageChange: &OnStorageChangeTrace{
			Address: address,
			Slot:    slot,
			Prev:    prev,
			New:     new,
		},
	})
}

func (t *Tracer) OnLog(log *types.Log) {
	if t.recordOptions.DisableOnLogRecording {
		return
	}

	if t.wrapped != nil && t.wrapped.OnLog != nil {
		t.wrapped.OnLog(log)
	}

	t.traces.Append(Trace{
		OnLog: &OnLogTrace{
			Log: log,
		},
	})
}

func (t *Tracer) GetResult() (json.RawMessage, error) {
	if t.wrapped != nil && t.wrapped.GetResult != nil {
		return t.wrapped.GetResult()
	}

	return nil, nil
}

func (t *Tracer) Stop(err error) {
	if t.wrapped != nil && t.wrapped.Stop != nil {
		t.wrapped.Stop(err)
	}
}

func (t *Tracer) mustFlushToFile(filePath string) {
	err := t.flushToFile(filePath)
	if err != nil {
		panic(err)
	}
}

func (t *Tracer) flushToFile(filePath string) error {
	b, err := json.MarshalIndent(t.traces, "", "    ")
	if err != nil {
		return err
	}

	dir := path.Dir(filePath)
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	} else if err != nil || !info.IsDir() {
		return fmt.Errorf("%s exists but is not a directory or encountered an error: %w", dir, err)
	}

	err = os.WriteFile(filePath, b, 0644)
	if err != nil {
		return err
	}

	t.traces = Traces{}
	return nil
}

type Option func(*Tracer)

func WithRecordOptions(recordOptions RecordOptions) Option {
	return func(t *Tracer) {
		t.recordOptions = recordOptions
	}
}

func WithFlushMode(flushMode FlushMode) Option {
	return func(t *Tracer) {
		t.flushMode = flushMode
	}
}

func WithWrappedTracer(wrapped *tracers.Tracer) Option {
	return func(t *Tracer) {
		t.wrapped = wrapped
	}
}

type FlushMode int

const (
	FlushModeBlock FlushMode = iota
	FlushModeTxn
)

type RecordOptions struct {
	DisableOnTxStartRecording         bool
	DisableOnTxEndRecording           bool
	DisableOnEnterRecording           bool
	DisableOnExitRecording            bool
	DisableOnOpcodeRecording          bool
	DisableOnOpcodeMemoryRecording    bool
	DisableOnOpcodeStackRecording     bool
	DisableOnFaultRecording           bool
	DisableOnGasChangeRecording       bool
	DisableOnBlockchainInitRecording  bool
	DisableOnBlockStartRecording      bool
	DisableOnBlockEndRecording        bool
	DisableOnGenesisBlockRecording    bool
	DisableOnSystemCallStartRecording bool
	DisableOnSystemCallEndRecording   bool
	DisableOnBalanceChangeRecording   bool
	DisableOnNonceChangeRecording     bool
	DisableOnCodeChangeRecording      bool
	DisableOnStorageChangeRecording   bool
	DisableOnLogRecording             bool
}

type Traces struct {
	Traces []Trace `json:"traces,omitempty"`
}

func (t *Traces) Append(trace Trace) {
	t.Traces = append(t.Traces, trace)
}

type Trace struct {
	// VM events
	OnTxStart   *OnTxStartTrace   `json:"onTxStart,omitempty"`
	OnTxEnd     *OnTxEndTrace     `json:"onTxEnd,omitempty"`
	OnEnter     *OnEnterTrace     `json:"onEnter,omitempty"`
	OnExit      *OnExitTrace      `json:"onExit,omitempty"`
	OnOpcode    *OnOpcodeTrace    `json:"onOpcode,omitempty"`
	OnFault     *OnFaultTrace     `json:"onFault,omitempty"`
	OnGasChange *OnGasChangeTrace `json:"onGasChange,omitempty"`
	// Chain events
	OnBlockchainInit  *OnBlockchainInitTrace  `json:"onBlockchainInit,omitempty"`
	OnBlockStart      *OnBlockStartTrace      `json:"onBlockStart,omitempty"`
	OnBlockEnd        *OnBlockEndTrace        `json:"onBlockEnd,omitempty"`
	OnGenesisBlock    *OnGenesisBlockTrace    `json:"onGenesisBlock,omitempty"`
	OnSystemCallStart *OnSystemCallStartTrace `json:"onSystemCallStart,omitempty"`
	OnSystemCallEnd   *OnSystemCallEndTrace   `json:"onSystemCallEnd,omitempty"`
	// State events
	OnBalanceChange *OnBalanceChangeTrace `json:"onBalanceChange,omitempty"`
	OnNonceChange   *OnNonceChangeTrace   `json:"onNonceChange,omitempty"`
	OnCodeChange    *OnCodeChangeTrace    `json:"onCodeChange,omitempty"`
	OnStorageChange *OnStorageChangeTrace `json:"onStorageChange,omitempty"`
	OnLog           *OnLogTrace           `json:"onLog,omitempty"`
}

type OnTxStartTrace struct {
	VMContext   *tracing.VMContext `json:"vmContext,omitempty"`
	Transaction types.Transaction  `json:"transaction,omitempty"`
	From        common.Address     `json:"from,omitempty"`
}

type OnTxEndTrace struct {
	Receipt *types.Receipt `json:"receipt,omitempty"`
	Error   string         `json:"error,omitempty"`
}

type OnEnterTrace struct {
	Depth      int            `json:"depth,omitempty"`
	Type       byte           `json:"type,omitempty"`
	From       common.Address `json:"from,omitempty"`
	To         common.Address `json:"to,omitempty"`
	Precompile bool           `json:"precompile,omitempty"`
	Input      hexutil.Bytes  `json:"input,omitempty"`
	Gas        uint64         `json:"gas,omitempty"`
	Value      *uint256.Int   `json:"value,omitempty"`
	Code       hexutil.Bytes  `json:"code,omitempty"`
}

type OnExitTrace struct {
	Depth    int           `json:"depth,omitempty"`
	Output   hexutil.Bytes `json:"output,omitempty"`
	GasUsed  uint64        `json:"gasUsed,omitempty"`
	Error    string        `json:"error,omitempty"`
	Reverted bool          `json:"reverted,omitempty"`
}

type OnOpcodeTrace struct {
	PC         uint64          `json:"pc,omitempty"`
	Op         string          `json:"op,omitempty"`
	Gas        uint64          `json:"gas,omitempty"`
	Cost       uint64          `json:"cost,omitempty"`
	Caller     common.Address  `json:"caller,omitempty"`
	Stack      []hexutil.Bytes `json:"stack,omitempty"`
	Memory     hexutil.Bytes   `json:"memory,omitempty"`
	MemorySize int             `json:"memSize,omitempty"`
	ReturnData hexutil.Bytes   `json:"returnData,omitempty"`
	Depth      int             `json:"depth,omitempty"`
	Error      string          `json:"error,omitempty"`
}

type OnFaultTrace struct {
	PC         uint64          `json:"pc,omitempty"`
	Op         byte            `json:"op,omitempty"`
	Gas        uint64          `json:"gas,omitempty"`
	Cost       uint64          `json:"cost,omitempty"`
	Caller     common.Address  `json:"caller,omitempty"`
	Stack      []hexutil.Bytes `json:"stack,omitempty"`
	Memory     hexutil.Bytes   `json:"memory,omitempty"`
	MemorySize int             `json:"memSize,omitempty"`
	Depth      int             `json:"depth,omitempty"`
	Error      string          `json:"error,omitempty"`
}

type OnGasChangeTrace struct {
	OldGas uint64 `json:"oldGas,omitempty"`
	NewGas uint64 `json:"newGas,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type OnBlockchainInitTrace struct {
	ChainConfig *chain.Config `json:"chainConfig,omitempty"`
}

type OnBlockStartTrace struct {
	Event tracing.BlockEvent `json:"event,omitempty"`
}

type OnBlockEndTrace struct {
	Error string `json:"error,omitempty"`
}

type OnGenesisBlockTrace struct {
	Header *types.Header      `json:"header,omitempty"`
	Alloc  types.GenesisAlloc `json:"alloc,omitempty"`
}

type OnSystemCallStartTrace struct{}

type OnSystemCallEndTrace struct{}

type OnBalanceChangeTrace struct {
	Address    common.Address `json:"address,omitempty"`
	OldBalance uint256.Int    `json:"oldBalance,omitempty"`
	NewBalance uint256.Int    `json:"newBalance,omitempty"`
	Reason     string         `json:"reason,omitempty"`
}

type OnNonceChangeTrace struct {
	Address  common.Address `json:"address,omitempty"`
	OldNonce uint64         `json:"oldNonce,omitempty"`
	NewNonce uint64         `json:"newNonce,omitempty"`
}

type OnCodeChangeTrace struct {
	Address      common.Address `json:"address,omitempty"`
	PrevCodeHash common.Hash    `json:"prevCodeHash,omitempty"`
	PrevCode     hexutil.Bytes  `json:"prevCode,omitempty"`
	NewCodeHash  common.Hash    `json:"newCodeHash,omitempty"`
	NewCode      hexutil.Bytes  `json:"newCode,omitempty"`
}

type OnStorageChangeTrace struct {
	Address common.Address `json:"address,omitempty"`
	Slot    common.Hash    `json:"slot,omitempty"`
	Prev    uint256.Int    `json:"prev,omitempty"`
	New     uint256.Int    `json:"new,omitempty"`
}

type OnLogTrace struct {
	Log *types.Log `json:"log,omitempty"`
}
