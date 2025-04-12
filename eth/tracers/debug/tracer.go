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

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
)

type Tracer struct {
	outputDir     string
	flushMode     FlushMode
	recordOptions RecordOptions
	traces        Traces
	currentBlock  *types.Block
}

func New(outputDir string, flushMode FlushMode, recordOptions RecordOptions) *tracers.Tracer {
	debugTracer := &Tracer{
		outputDir:     outputDir,
		flushMode:     flushMode,
		recordOptions: recordOptions,
	}

	return &tracers.Tracer{
		Hooks: debugTracer.Hooks(),
	}
}

func (r *Tracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		// VM events
		OnTxStart:   r.OnTxStart,
		OnTxEnd:     r.OnTxEnd,
		OnEnter:     r.OnEnter,
		OnExit:      r.OnExit,
		OnOpcode:    r.OnOpcode,
		OnFault:     r.OnFault,
		OnGasChange: r.OnGasChange,
		// Chain events
		OnBlockchainInit:  r.OnBlockchainInit,
		OnBlockStart:      r.OnBlockStart,
		OnBlockEnd:        r.OnBlockEnd,
		OnGenesisBlock:    r.OnGenesisBlock,
		OnSystemCallStart: r.OnSystemCallStart,
		OnSystemCallEnd:   r.OnSystemCallEnd,
		// State events
		OnBalanceChange: r.OnBalanceChange,
		OnNonceChange:   r.OnNonceChange,
		OnCodeChange:    r.OnCodeChange,
		OnStorageChange: r.OnStorageChange,
		OnLog:           r.OnLog,
	}
}

func (r *Tracer) OnTxStart(vm *tracing.VMContext, txn types.Transaction, from libcommon.Address) {
	if r.recordOptions.DisableOnTxStartRecording {
		return
	}

	r.traces.Append(Trace{
		OnTxStart: &OnTxStartTrace{
			VMContext:   vm,
			Transaction: txn,
			From:        from,
		},
	})
}

func (r *Tracer) OnTxEnd(receipt *types.Receipt, err error) {
	if r.recordOptions.DisableOnTxEndRecording {
		return
	}

	r.traces.Append(Trace{
		OnTxEnd: &OnTxEndTrace{
			Receipt: receipt,
			Error:   err,
		},
	})

	if r.flushMode != FlushModeTxn {
		return
	}

	txnTraceFile := fmt.Sprintf("txn_trace_%d_%s_%d_%s.json", receipt.TransactionIndex, receipt.TxHash, receipt.BlockNumber, receipt.BlockHash)
	r.mustFlushToFile(path.Join(r.outputDir, txnTraceFile))
}

func (r *Tracer) OnEnter(depth int, typ byte, from, to libcommon.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if r.recordOptions.DisableOnEnterRecording {
		return
	}

	r.traces.Append(Trace{
		OnEnter: &OnEnterTrace{
			Depth:      depth,
			Type:       typ,
			From:       from,
			To:         to,
			Precompile: precompile,
			Input:      input,
			Gas:        gas,
			Value:      value,
			Code:       code,
		},
	})
}

func (r *Tracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if r.recordOptions.DisableOnExitRecording {
		return
	}

	r.traces.Append(Trace{
		OnExit: &OnExitTrace{
			Depth:    depth,
			Output:   output,
			GasUsed:  gasUsed,
			Error:    err,
			Reverted: reverted,
		},
	})
}

func (r *Tracer) OnOpcode(pc uint64, op byte, gas, cost uint64, opContext tracing.OpContext, returnData []byte, depth int, err error) {
	if r.recordOptions.DisableOnOpcodeRecording {
		return
	}

	if r.recordOptions.DisableOpContextRecording {
		opContext = nil
	}

	r.traces.Append(Trace{
		OnOpcode: &OnOpcodeTrace{
			PC:         pc,
			Op:         fmt.Sprintf("%v", vm.OpCode(op)),
			Gas:        gas,
			Cost:       cost,
			OpContext:  opContext,
			ReturnData: returnData,
			Depth:      depth,
			Error:      err,
		},
	})
}

func (r *Tracer) OnFault(pc uint64, op byte, gas, cost uint64, opContext tracing.OpContext, depth int, err error) {
	if r.recordOptions.DisableOnFaultRecording {
		return
	}

	r.traces.Append(Trace{
		OnFault: &OnFaultTrace{
			PC:        pc,
			Op:        op,
			Gas:       gas,
			Cost:      cost,
			OpContext: opContext,
			Depth:     depth,
			Error:     err,
		},
	})
}

func (r *Tracer) OnGasChange(old, new uint64, reason tracing.GasChangeReason) {
	if r.recordOptions.DisableOnGasChangeRecording {
		return
	}

	r.traces.Append(Trace{
		OnGasChange: &OnGasChangeTrace{
			OldGas: old,
			NewGas: new,
			Reason: reason,
		},
	})
}

func (r *Tracer) OnBlockchainInit(chainConfig *chain.Config) {
	if r.recordOptions.DisableOnBlockchainInitRecording {
		return
	}

	r.traces.Append(Trace{
		OnBlockchainInit: &OnBlockchainInitTrace{
			ChainConfig: chainConfig,
		},
	})
}

func (r *Tracer) OnBlockStart(event tracing.BlockEvent) {
	if r.recordOptions.DisableOnBlockStartRecording {
		return
	}

	r.currentBlock = event.Block
	r.traces.Append(Trace{
		OnBlockStart: &OnBlockStartTrace{
			Event: event,
		},
	})
}

func (r *Tracer) OnBlockEnd(err error) {
	if r.recordOptions.DisableOnBlockEndRecording {
		return
	}

	r.traces.Append(Trace{
		OnBlockEnd: &OnBlockEndTrace{
			Error: err,
		},
	})

	if r.currentBlock == nil || r.flushMode != FlushModeBlock {
		return
	}

	blockTraceFile := fmt.Sprintf("block_trace_%d_%s.json", r.currentBlock.NumberU64(), r.currentBlock.Hash())
	r.mustFlushToFile(path.Join(r.outputDir, blockTraceFile))
}

func (r *Tracer) OnGenesisBlock(genesis *types.Block, alloc types.GenesisAlloc) {
	if r.recordOptions.DisableOnGenesisBlockRecording {
		return
	}

	r.traces.Append(Trace{
		OnGenesisBlock: &OnGenesisBlockTrace{
			Header: genesis.Header(),
			Alloc:  alloc,
		},
	})
}

func (r *Tracer) OnSystemCallStart() {
	if r.recordOptions.DisableOnSystemCallStartRecording {
		return
	}

	r.traces.Append(Trace{
		OnSystemCallStart: &OnSystemCallStartTrace{},
	})
}

func (r *Tracer) OnSystemCallEnd() {
	if r.recordOptions.DisableOnSystemCallEndRecording {
		return
	}

	r.traces.Append(Trace{
		OnSystemCallEnd: &OnSystemCallEndTrace{},
	})
}

func (r *Tracer) OnBalanceChange(address libcommon.Address, oldBalance, newBalance *uint256.Int, reason tracing.BalanceChangeReason) {
	if r.recordOptions.DisableOnBalanceChangeRecording {
		return
	}

	r.traces.Append(Trace{
		OnBalanceChange: &OnBalanceChangeTrace{
			Address:    address,
			OldBalance: oldBalance,
			NewBalance: newBalance,
			Reason:     reason,
		},
	})
}

func (r *Tracer) OnNonceChange(address libcommon.Address, oldNonce, newNonce uint64) {
	if r.recordOptions.DisableOnNonceChangeRecording {
		return
	}

	r.traces.Append(Trace{
		OnNonceChange: &OnNonceChangeTrace{
			Address:  address,
			OldNonce: oldNonce,
			NewNonce: newNonce,
		},
	})
}

func (r *Tracer) OnCodeChange(address libcommon.Address, prevCodeHash libcommon.Hash, prevCode []byte, newCodeHash libcommon.Hash, newCode []byte) {
	if r.recordOptions.DisableOnCodeChangeRecording {
		return
	}

	r.traces.Append(Trace{
		OnCodeChange: &OnCodeChangeTrace{
			Address:      address,
			PrevCodeHash: prevCodeHash,
			PrevCode:     prevCode,
			NewCodeHash:  newCodeHash,
			NewCode:      newCode,
		},
	})
}

func (r *Tracer) OnStorageChange(address libcommon.Address, slot *libcommon.Hash, prev, new uint256.Int) {
	if r.recordOptions.DisableOnStorageChangeRecording {
		return
	}

	r.traces.Append(Trace{
		OnStorageChange: &OnStorageChangeTrace{
			Address: address,
			Slot:    slot,
			Prev:    prev,
			New:     new,
		},
	})
}

func (r *Tracer) OnLog(log *types.Log) {
	if r.recordOptions.DisableOnLogRecording {
		return
	}

	r.traces.Append(Trace{
		OnLog: &OnLogTrace{
			Log: log,
		},
	})
}

func (r *Tracer) mustFlushToFile(filePath string) {
	err := r.flushToFile(filePath)
	if err != nil {
		panic(err)
	}
}

func (r *Tracer) flushToFile(filePath string) error {
	b, err := json.MarshalIndent(r.traces, "", "    ")
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

	r.traces = Traces{}
	return nil
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
	DisableOpContextRecording         bool
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
	From        libcommon.Address  `json:"from,omitempty"`
}

type OnTxEndTrace struct {
	Receipt *types.Receipt `json:"receipt,omitempty"`
	Error   error          `json:"error,omitempty"`
}

type OnEnterTrace struct {
	Depth      int               `json:"depth,omitempty"`
	Type       byte              `json:"type,omitempty"`
	From       libcommon.Address `json:"from,omitempty"`
	To         libcommon.Address `json:"to,omitempty"`
	Precompile bool              `json:"precompile,omitempty"`
	Input      []byte            `json:"input,omitempty"`
	Gas        uint64            `json:"gas,omitempty"`
	Value      *uint256.Int      `json:"value,omitempty"`
	Code       []byte            `json:"code,omitempty"`
}

type OnExitTrace struct {
	Depth    int    `json:"depth,omitempty"`
	Output   []byte `json:"output,omitempty"`
	GasUsed  uint64 `json:"gasUsed,omitempty"`
	Error    error  `json:"error,omitempty"`
	Reverted bool   `json:"reverted,omitempty"`
}

type OnOpcodeTrace struct {
	PC         uint64            `json:"pc,omitempty"`
	Op         string            `json:"op,omitempty"`
	Gas        uint64            `json:"gas,omitempty"`
	Cost       uint64            `json:"cost,omitempty"`
	OpContext  tracing.OpContext `json:"opContext,omitempty"`
	ReturnData []byte            `json:"returnData,omitempty"`
	Depth      int               `json:"depth,omitempty"`
	Error      error             `json:"error,omitempty"`
}

type OnFaultTrace struct {
	PC        uint64            `json:"pc,omitempty"`
	Op        byte              `json:"op,omitempty"`
	Gas       uint64            `json:"gas,omitempty"`
	Cost      uint64            `json:"cost,omitempty"`
	OpContext tracing.OpContext `json:"opContext,omitempty"`
	Depth     int               `json:"depth,omitempty"`
	Error     error             `json:"error,omitempty"`
}

type OnGasChangeTrace struct {
	OldGas uint64                  `json:"oldGas,omitempty"`
	NewGas uint64                  `json:"newGas,omitempty"`
	Reason tracing.GasChangeReason `json:"reason,omitempty"`
}

type OnBlockchainInitTrace struct {
	ChainConfig *chain.Config `json:"chainConfig,omitempty"`
}

type OnBlockStartTrace struct {
	Event tracing.BlockEvent `json:"event,omitempty"`
}

type OnBlockEndTrace struct {
	Error error `json:"error,omitempty"`
}

type OnGenesisBlockTrace struct {
	Header *types.Header      `json:"header,omitempty"`
	Alloc  types.GenesisAlloc `json:"alloc,omitempty"`
}

type OnSystemCallStartTrace struct{}

type OnSystemCallEndTrace struct{}

type OnBalanceChangeTrace struct {
	Address    libcommon.Address           `json:"address,omitempty"`
	OldBalance *uint256.Int                `json:"oldBalance,omitempty"`
	NewBalance *uint256.Int                `json:"newBalance,omitempty"`
	Reason     tracing.BalanceChangeReason `json:"reason,omitempty"`
}

type OnNonceChangeTrace struct {
	Address  libcommon.Address `json:"address,omitempty"`
	OldNonce uint64            `json:"oldNonce,omitempty"`
	NewNonce uint64            `json:"newNonce,omitempty"`
}

type OnCodeChangeTrace struct {
	Address      libcommon.Address `json:"address,omitempty"`
	PrevCodeHash libcommon.Hash    `json:"prevCodeHash,omitempty"`
	PrevCode     []byte            `json:"prevCode,omitempty"`
	NewCodeHash  libcommon.Hash    `json:"newCodeHash,omitempty"`
	NewCode      []byte            `json:"newCode,omitempty"`
}

type OnStorageChangeTrace struct {
	Address libcommon.Address `json:"address,omitempty"`
	Slot    *libcommon.Hash   `json:"slot,omitempty"`
	Prev    uint256.Int       `json:"prev,omitempty"`
	New     uint256.Int       `json:"new,omitempty"`
}

type OnLogTrace struct {
	Log *types.Log `json:"log,omitempty"`
}
