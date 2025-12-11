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

package logger

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"os"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

var ErrTraceLimitReached = errors.New("the number of logs reached the specified limit")

// Storage represents a contract's storage.
type Storage map[common.Hash]common.Hash

// Copy duplicates the current storage.
func (s Storage) Copy() Storage {
	return maps.Clone(s)
}

// LogConfig are the configuration options for structured logger the EVM
type LogConfig struct {
	DisableMemory     bool // disable memory capture
	DisableStack      bool // disable stack capture
	DisableStorage    bool // disable storage capture
	DisableReturnData bool // disable return data capture
	Debug             bool // print output during capture end
	Limit             int  // maximum length of output, but zero means unlimited
	// Chain overrides, can be used to execute a trace using future fork rules
	Overrides *chain.Config `json:"overrides,omitempty"`
}

//go:generate gencodec -type StructLog -field-override structLogMarshaling -out gen_structlog.go

// StructLog is emitted to the EVM each cycle and lists information about the current internal state
// prior to the execution of the statement.
type StructLog struct {
	Pc            uint64                      `json:"pc"`
	Op            vm.OpCode                   `json:"op"`
	Gas           uint64                      `json:"gas"`
	GasCost       uint64                      `json:"gasCost"`
	Memory        []byte                      `json:"memory"`
	MemorySize    int                         `json:"memSize"`
	Stack         []*big.Int                  `json:"stack"`
	ReturnData    []byte                      `json:"returnData"`
	Storage       map[common.Hash]common.Hash `json:"-"`
	Depth         int                         `json:"depth"`
	RefundCounter uint64                      `json:"refund"`
	Err           error                       `json:"-"`
}

// overrides for gencodec
type structLogMarshaling struct {
	Stack       []*math.HexOrDecimal256
	Gas         math.HexOrDecimal64
	GasCost     math.HexOrDecimal64
	Memory      hexutil.Bytes
	ReturnData  hexutil.Bytes
	OpName      string `json:"opName"` // adds call to OpName() in MarshalJSON
	ErrorString string `json:"error"`  // adds call to ErrorString() in MarshalJSON
}

// OpName formats the operand name in a human-readable format.
func (s *StructLog) OpName() string {
	return s.Op.String()
}

// ErrorString formats the log's error as a string.
func (s *StructLog) ErrorString() string {
	if s.Err != nil {
		return s.Err.Error()
	}
	return ""
}

// StructLogRes stores a structured log emitted by the EVM while replaying a
// transaction in debug mode
type StructLogRes struct {
	Pc      uint64             `json:"pc"`
	Op      string             `json:"op"`
	Gas     uint64             `json:"gas"`
	GasCost uint64             `json:"gasCost"`
	Depth   int                `json:"depth"`
	Error   error              `json:"error,omitempty"`
	Stack   *[]string          `json:"stack,omitempty"`
	Memory  *[]string          `json:"memory,omitempty"`
	Storage *map[string]string `json:"storage,omitempty"`
}

// StructLogger is an EVM state logger and implements Tracer.
//
// StructLogger can capture state based on the given Log configuration and also keeps
// a track record of modified storage which is used in reporting snapshots of the
// contract their storage.
type StructLogger struct {
	cfg LogConfig

	storage map[common.Address]Storage
	logs    []StructLog
	output  []byte
	err     error
	env     *tracing.VMContext

	gasUsed uint64
}

// NewStructLogger returns a new logger
func NewStructLogger(cfg *LogConfig) *StructLogger {
	logger := &StructLogger{
		storage: make(map[common.Address]Storage),
	}
	if cfg != nil {
		logger.cfg = *cfg
	}
	return logger
}

func (l *StructLogger) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnTxStart: l.OnTxStart,
		OnTxEnd:   l.OnTxEnd,
		OnExit:    l.OnExit,
		OnOpcode:  l.OnOpcode,
		Flush:     l.Flush,
	}
}

func (l *StructLogger) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks:     l.Hooks(),
		GetResult: l.GetResult,
		Stop:      l.Stop,
	}
}

func (l *StructLogger) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	l.env = env
}

func (l *StructLogger) OnTxEnd(receipt *types.Receipt, err error) {
	if err != nil {
		// Don't override vm error
		if l.err == nil {
			l.err = err
		}
		return
	}
	l.gasUsed = receipt.GasUsed
}

// OnOpcode also tracks SLOAD/SSTORE ops to track storage change.
func (l *StructLogger) OnOpcode(pc uint64, opcode byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	op := vm.OpCode(opcode)
	memory := scope.MemoryData()
	stack := scope.StackData()
	contractAddr := scope.Address()

	// check if already accumulated the specified number of logs
	if l.cfg.Limit != 0 && l.cfg.Limit <= len(l.logs) {
		return
	}

	// Copy a snapshot of the current memory state to a new buffer
	var mem []byte
	if !l.cfg.DisableMemory {
		mem = make([]byte, len(memory))
		copy(mem, memory)
	}
	// Copy a snapshot of the current stack state to a new buffer
	var stck []*big.Int
	if !l.cfg.DisableStack {
		stck = make([]*big.Int, len(stack))
		for i, item := range stack {
			stck[i] = new(big.Int).Set(item.ToBig())
		}
	}

	stackLen := len(stack)
	// Copy a snapshot of the current storage to a new container
	var storage Storage
	if !l.cfg.DisableStorage {
		// initialise new changed values storage container for this contract
		// if not present.
		if l.storage[contractAddr] == nil {
			l.storage[contractAddr] = make(Storage)
		}
		// capture SLOAD opcodes and record the read entry in the local storage
		if op == vm.SLOAD && stackLen >= 1 {
			var (
				address = common.Hash(stack[stackLen-1].Bytes32())
				value   uint256.Int
			)
			l.env.IntraBlockState.GetState(contractAddr, address, &value)
			l.storage[contractAddr][address] = value.Bytes32()
		}
		// capture SSTORE opcodes and record the written entry in the local storage.
		if op == vm.SSTORE && stackLen >= 2 {
			var (
				value   = common.Hash(stack[stackLen-2].Bytes32())
				address = common.Hash(stack[stackLen-1].Bytes32())
			)
			l.storage[contractAddr][address] = value
		}
		storage = l.storage[contractAddr].Copy()
	}
	var rdata []byte
	if !l.cfg.DisableReturnData {
		rdata = make([]byte, len(rData))
		copy(rdata, rData)
	}
	// create a new snapshot of the EVM.
	log := StructLog{pc, op, gas, cost, mem, len(memory), stck, rdata, storage, depth, l.env.IntraBlockState.GetRefund(), err}
	l.logs = append(l.logs, log)
}

// OnExit is called after the call finishes to finalize the tracing.
func (l *StructLogger) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if depth != 0 {
		return
	}
	l.output = output
	l.err = err
	if l.cfg.Debug {
		fmt.Printf("0x%x\n", output)
		if err != nil {
			fmt.Printf(" error: %v\n", err)
		}
	}
}

// GetResult returns an empty json object.
func (l *StructLogger) GetResult() (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

// Stop terminates execution of the tracer at the first opportune moment.
func (l *StructLogger) Stop(err error) {
}

// StructLogs returns the captured log entries.
func (l *StructLogger) StructLogs() []StructLog { return l.logs }

// Error returns the VM error captured by the trace.
func (l *StructLogger) Error() error { return l.err }

// Output returns the VM return value captured by the trace.
func (l *StructLogger) Output() []byte { return l.output }

func (l *StructLogger) Flush(tx types.Transaction) {
	w, err1 := os.Create(fmt.Sprintf("txtrace_%x.txt", tx.Hash()))
	if err1 != nil {
		panic(err1)
	}
	encoder := json.NewEncoder(w)
	logs := FormatLogs(l.StructLogs())
	if err2 := encoder.Encode(logs); err2 != nil {
		panic(err2)
	}
	if err2 := w.Close(); err2 != nil {
		panic(err2)
	}
}

// FormatLogs formats EVM returned structured logs for json output
func FormatLogs(logs []StructLog) []StructLogRes {
	formatted := make([]StructLogRes, len(logs))
	for index, trace := range logs {
		formatted[index] = StructLogRes{
			Pc:      trace.Pc,
			Op:      trace.Op.String(),
			Gas:     trace.Gas,
			GasCost: trace.GasCost,
			Depth:   trace.Depth,
			Error:   trace.Err,
		}
		if trace.Stack != nil {
			stack := make([]string, len(trace.Stack))
			for i, stackValue := range trace.Stack {
				stack[i] = hex.EncodeToString(math.PaddedBigBytes(stackValue, 32))
			}
			formatted[index].Stack = &stack
		}
		if trace.Memory != nil {
			memory := make([]string, 0, (len(trace.Memory)+31)/32)
			for i := 0; i+32 <= len(trace.Memory); i += 32 {
				memory = append(memory, hex.EncodeToString(trace.Memory[i:i+32]))
			}
			formatted[index].Memory = &memory
		}
		if trace.Storage != nil {
			storage := make(map[string]string)
			for i, storageValue := range trace.Storage {
				storage[fmt.Sprintf("%x", i)] = fmt.Sprintf("%x", storageValue)
			}
			formatted[index].Storage = &storage
		}
	}
	return formatted
}

// WriteTrace writes a formatted trace to the given writer
func WriteTrace(writer io.Writer, logs []StructLog) {
	for _, log := range logs {
		fmt.Fprintf(writer, "%-16spc=%08d gas=%v cost=%v", log.Op, log.Pc, log.Gas, log.GasCost)
		if log.Err != nil {
			fmt.Fprintf(writer, " ERROR: %v", log.Err)
		}
		fmt.Fprintln(writer)

		if len(log.Stack) > 0 {
			fmt.Fprintln(writer, "Stack:")
			for i := len(log.Stack) - 1; i >= 0; i-- {
				fmt.Fprintf(writer, "%08d  %x\n", len(log.Stack)-i-1, math.PaddedBigBytes(log.Stack[i], 32))
			}
		}
		if len(log.Memory) > 0 {
			fmt.Fprintln(writer, "Memory:")
			fmt.Fprint(writer, hex.Dump(log.Memory))
		}
		if len(log.Storage) > 0 {
			fmt.Fprintln(writer, "Storage:")
			for h, item := range log.Storage {
				fmt.Fprintf(writer, "%x: %x\n", h, item)
			}
		}
		if len(log.ReturnData) > 0 {
			fmt.Fprintln(writer, "ReturnData:")
			fmt.Fprint(writer, hex.Dump(log.ReturnData))
		}
		fmt.Fprintln(writer)
	}
}

// WriteLogs writes vm logs in a readable format to the given writer
func WriteLogs(writer io.Writer, logs []*types.Log) {
	for _, log := range logs {
		fmt.Fprintf(writer, "LOG%d: %x bn=%d txi=%x\n", len(log.Topics), log.Address, log.BlockNumber, log.TxIndex)

		for i, topic := range log.Topics {
			fmt.Fprintf(writer, "%08d  %x\n", i, topic)
		}

		fmt.Fprint(writer, hex.Dump(log.Data))
		fmt.Fprintln(writer)
	}
}

type mdLogger struct {
	out io.Writer
	cfg *LogConfig
	env *tracing.VMContext
}

// NewMarkdownLogger creates a logger which outputs information in a format adapted
// for human readability, and is also a valid markdown table
func NewMarkdownLogger(cfg *LogConfig, writer io.Writer) *mdLogger {
	l := &mdLogger{writer, cfg, nil}
	if l.cfg == nil {
		l.cfg = &LogConfig{}
	}
	return l
}

func (t *mdLogger) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnTxStart: t.OnTxStart,
		OnEnter:   t.OnEnter,
		OnExit:    t.OnExit,
		OnOpcode:  t.OnOpcode,
		OnFault:   t.OnFault,
	}
}

func (t *mdLogger) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	t.env = env
}

func (t *mdLogger) CaptureTxEnd(restGas uint64) {}

func (t *mdLogger) captureStartOrEnter(from, to common.Address, create bool, input []byte, gas uint64, value *uint256.Int) {
	if !create {
		fmt.Fprintf(t.out, "From: `%v`\nTo: `%v`\nData: `0x%x`\nGas: `%d`\nValue `%v` wei\n",
			from.String(), to.String(),
			input, gas, value)
	} else {
		fmt.Fprintf(t.out, "From: `%v`\nCreate at: `%v`\nData: `0x%x`\nGas: `%d`\nValue `%v` wei\n",
			from.String(), to.String(),
			input, gas, value)
	}

	fmt.Fprintf(t.out, `
|  Pc   |      Op     | Cost |   Stack   |   RStack  |  Refund |
|-------|-------------|------|-----------|-----------|---------|
`)
}

func (t *mdLogger) OnEnter(depth int, typ byte, from common.Address, to common.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if depth != 0 {
		return
	}
	create := vm.OpCode(typ) == vm.CREATE
	t.captureStartOrEnter(from, to, create, input, gas, value)
}

func (t *mdLogger) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if depth == 0 {
		fmt.Fprintf(t.out, "\nOutput: `%#x`\nConsumed gas: `%d`\nError: `%v`\n",
			output, gasUsed, err)
	}
}

// OnOpcode also tracks SLOAD/SSTORE ops to track storage change.
func (t *mdLogger) OnOpcode(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	stack := scope.StackData()

	fmt.Fprintf(t.out, "| %4d  | %10v  |  %3d |", pc, op, cost)

	if !t.cfg.DisableStack {
		// format stack
		var a []string
		for _, elem := range stack {
			a = append(a, elem.String())
		}
		b := fmt.Sprintf("[%v]", strings.Join(a, ","))
		fmt.Fprintf(t.out, "%10v |", b)
	}
	fmt.Fprintf(t.out, "%10v |", t.env.IntraBlockState.GetRefund())
	fmt.Fprintln(t.out, "")
	if err != nil {
		fmt.Fprintf(t.out, "Error: %v\n", err)
	}
}

func (t *mdLogger) OnFault(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, depth int, err error) {
	fmt.Fprintf(t.out, "\nError: at pc=%d, op=%v: %v\n", pc, op, err)
}
