// Copyright 2026 The Erigon Authors
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

//go:build gendispatch

// Dispatch-loop generator: emits run_dispatch_gen.go, one switch-dispatch
// twin of EVM.Run valid for every canonical fork table.
//
// The generator dumps all constructed tables (vm.DumpAllDispatch) and, per
// opcode, bakes a field as a compile-time literal only when it is identical
// in every table; anything fork-varying — state-op gas schedules, opcodes
// that appear mid-history, closure-valued slots — is read from the active
// table at run time. ExtraEips-patched table copies fall back to the generic
// interpreter loop.
//
// Usage: go generate ./execution/vm (or: go run -tags gendispatch ./execution/vm/gen)
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/erigontech/erigon/execution/vm"
)

const symbolPrefix = "github.com/erigontech/erigon/execution/vm."

// packageFuncs returns the set of top-level function names declared in the vm
// package, parsed from source. A dumped symbol is directly callable only when
// it resolves to one of these.
func packageFuncs(vmDir string) map[string]bool {
	funcs := make(map[string]bool)
	matches, err := filepath.Glob(filepath.Join(vmDir, "*.go"))
	if err != nil {
		panic(err)
	}
	fset := token.NewFileSet()
	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: cannot parse %s: %v\n", path, err)
			continue
		}
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
				funcs[fn.Name.Name] = true
			}
		}
	}
	return funcs
}

type callMode int

const (
	callNone   callMode = iota // field nil in every table
	callDirect                 // named func, identical in every table
	callEntry                  // non-nil everywhere but varying or closure-valued
	callGuard                  // nil in some tables: needs a runtime nil check
)

// opPlan is the cross-fork emission plan for one opcode.
type opPlan struct {
	op   byte
	name string

	undefinedEverywhere bool

	gasInv bool
	gas    uint64

	stackInv bool
	numPop   int
	maxStack int

	exec       callMode // callDirect or callEntry
	execSymbol string

	dyn       callMode
	dynSymbol string

	mem       callMode
	memSymbol string
}

// needsEntry reports whether the emitted case must load operation := jt[op].
func (p *opPlan) needsEntry() bool {
	return !p.gasInv || !p.stackInv ||
		p.exec == callEntry ||
		p.dyn == callEntry || p.dyn == callGuard ||
		p.mem == callEntry || p.mem == callGuard
}

func analyze(forks []vm.ForkDispatch, funcs map[string]bool, undefinedSymbol string) []opPlan {
	directName := func(symbol string) string {
		name := strings.TrimPrefix(symbol, symbolPrefix)
		if funcs[name] && !strings.Contains(name, ".") {
			return name
		}
		return ""
	}

	plans := make([]opPlan, 0, 256)
	for i := range 256 {
		p := opPlan{op: byte(i), gasInv: true, stackInv: true, undefinedEverywhere: true}
		var execSyms, dynSyms, memSyms []string
		for f, fork := range forks {
			e := fork.Entries[i]
			if !e.Undefined {
				p.undefinedEverywhere = false
				p.name = e.Name
			}
			if f == 0 {
				p.gas, p.numPop, p.maxStack = e.ConstantGas, e.NumPop, e.MaxStack
			} else {
				if e.ConstantGas != p.gas {
					p.gasInv = false
				}
				if e.NumPop != p.numPop || e.MaxStack != p.maxStack {
					p.stackInv = false
				}
			}
			execSyms = append(execSyms, e.Execute)
			dynSyms = append(dynSyms, e.DynamicGas)
			memSyms = append(memSyms, e.MemorySize)
		}
		if p.undefinedEverywhere {
			plans = append(plans, p)
			continue
		}

		p.exec, p.execSymbol = classify(execSyms, directName)
		if p.exec != callDirect {
			p.exec = callEntry
		}
		p.dyn, p.dynSymbol = classify(dynSyms, directName)
		p.mem, p.memSymbol = classify(memSyms, directName)
		plans = append(plans, p)
	}
	return plans
}

// classify decides how the emitted case reaches a function-valued field,
// given its symbol in every fork table.
func classify(symbols []string, directName func(string) string) (callMode, string) {
	allNil, someNil := true, false
	invariant := true
	for _, s := range symbols {
		if s == "" {
			someNil = true
		} else {
			allNil = false
		}
		if s != symbols[0] {
			invariant = false
		}
	}
	switch {
	case allNil:
		return callNone, ""
	case someNil:
		return callGuard, ""
	case invariant:
		if name := directName(symbols[0]); name != "" {
			return callDirect, name
		}
		return callEntry, ""
	default:
		return callEntry, ""
	}
}

type emitter struct {
	buf bytes.Buffer
}

func (e *emitter) p(format string, args ...any) {
	fmt.Fprintf(&e.buf, format, args...)
}

const errReturn = "return nil, callContext.Gas(), mdgas.MdGasUsage{}, "

func (e *emitter) emitDebugBlock() {
	// Experiment: per-case OnOpcode/OnGasChange emission removed to measure
	// the register-pressure cost of the hook machinery in the case bodies.
}

func (e *emitter) emitTraceBlock() {
	e.p("if trace {\n")
	e.p("traceInstruction(evm, jt[op], op, pc, callGas, cost, callContext)\n")
	e.p("}\n")
}

func (e *emitter) emitCase(p opPlan) {
	e.p("case OpCode(0x%02X): // %s\n", p.op, p.name)
	if p.needsEntry() {
		e.p("operation := jt[op]\n")
	}

	if p.gasInv {
		e.p("cost = %d\n", p.gas)
	} else {
		e.p("cost = operation.constantGas\n")
	}

	if p.stackInv {
		if p.numPop > 0 {
			e.p("if sLen := stack.len(); sLen < %d {\n", p.numPop)
			e.p(errReturn+"&ErrStackUnderflow{stackLen: sLen, required: %d}\n", p.numPop)
			if p.maxStack < 1024 {
				e.p("} else if sLen > %d {\n", p.maxStack)
				e.p(errReturn+"&ErrStackOverflow{stackLen: sLen, limit: %d}\n", p.maxStack)
			}
			e.p("}\n")
		} else if p.maxStack < 1024 {
			e.p("if sLen := stack.len(); sLen > %d {\n", p.maxStack)
			e.p(errReturn+"&ErrStackOverflow{stackLen: sLen, limit: %d}\n", p.maxStack)
			e.p("}\n")
		}
	} else {
		e.p("if sLen := stack.len(); sLen < operation.numPop {\n")
		e.p(errReturn + "&ErrStackUnderflow{stackLen: sLen, required: operation.numPop}\n")
		e.p("} else if sLen > operation.maxStack {\n")
		e.p(errReturn + "&ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}\n")
		e.p("}\n")
	}

	if p.gasInv {
		if p.gas > 0 {
			e.p("if callContext.gas < %d {\n", p.gas)
			e.p(errReturn + "ErrOutOfGas\n")
			e.p("}\n")
			e.p("callContext.gas -= %d\n", p.gas)
		}
	} else {
		e.p("if callContext.gas < cost {\n")
		e.p(errReturn + "ErrOutOfGas\n")
		e.p("}\n")
		e.p("callContext.gas -= cost\n")
	}

	if p.dyn != callNone {
		e.p("var memorySize uint64\n")
		switch p.mem {
		case callNone:
		case callDirect:
			e.p("if memSize, overflow := %s(callContext); overflow {\n", p.memSymbol)
			e.p(errReturn + "ErrGasUintOverflow\n")
			e.p("} else if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {\n")
			e.p(errReturn + "ErrGasUintOverflow\n")
			e.p("}\n")
		default:
			e.p("if operation.memorySize != nil {\n")
			e.p("if memSize, overflow := operation.memorySize(callContext); overflow {\n")
			e.p(errReturn + "ErrGasUintOverflow\n")
			e.p("} else if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {\n")
			e.p(errReturn + "ErrGasUintOverflow\n")
			e.p("}\n")
			e.p("}\n")
		}

		if p.dyn == callGuard {
			e.p("if operation.dynamicGas != nil {\n")
		}
		e.p("evm.callGasTemp = 0\n")
		e.p("var dynamicCost mdgas.MdGas\n")
		if p.dyn == callDirect {
			e.p("dynamicCost, err = %s(evm, callContext, callContext.Gas(), memorySize)\n", p.dynSymbol)
		} else {
			e.p("dynamicCost, err = operation.dynamicGas(evm, callContext, callContext.Gas(), memorySize)\n")
		}
		e.p("if err != nil {\n")
		e.p("if !errors.Is(err, ErrOutOfGas) {\n")
		e.p("err = fmt.Errorf(\"%%w: %%w\", ErrOutOfGas, err)\n")
		e.p("}\n")
		e.p(errReturn + "err\n")
		e.p("}\n")
		e.p("if anyTrace {\n")
		e.p("cost += dynamicCost.Regular\n")
		if p.gasInv {
			e.p("callGas = %d + dynamicCost.Regular - evm.CallGasTemp()\n", p.gas)
		} else {
			e.p("callGas = operation.constantGas + dynamicCost.Regular - evm.CallGasTemp()\n")
		}
		e.p("if dbg.TraceDynamicGas && dynamicCost.Regular > 0 {\n")
		e.p("traceDynamicGasPrint(evm, op, callGas, cost)\n")
		e.p("}\n")
		e.p("}\n")
		e.p("if callContext.gas < dynamicCost.Regular {\n")
		e.p(errReturn + "ErrOutOfGas\n")
		e.p("}\n")
		e.p("callContext.gas -= dynamicCost.Regular\n")
		e.p("if dynamicCost.State > 0 {\n")
		e.p("if ok := callContext.useMdGas(dynamicCost.State, mdgas.StateGas, nil, tracing.GasChangeIgnored); !ok {\n")
		e.p(errReturn + "ErrOutOfGas\n")
		e.p("}\n")
		e.p("}\n")
		if p.dyn == callGuard {
			e.p("}\n")
		}
	}

	e.emitDebugBlock()
	if p.dyn != callNone && p.mem != callNone {
		e.p("if memorySize > 0 {\n")
		e.p("callContext.Memory.Resize(memorySize)\n")
		e.p("}\n")
	}
	e.emitTraceBlock()
	if p.exec == callDirect {
		e.p("pc, res, err = %s(pc, evm, callContext)\n", p.execSymbol)
	} else {
		e.p("pc, res, err = operation.execute(pc, evm, callContext)\n")
	}
}

func (e *emitter) emitDefaultCase() {
	e.p("default:\n")
	e.p("cost = 0\n")
	e.emitDebugBlock()
	e.emitTraceBlock()
	e.p("pc, res, err = opUndefined(pc, evm, callContext)\n")
}

const fileHeader = `// Code generated by go run -tags gendispatch ./execution/vm/gen. DO NOT EDIT.

package vm

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
)

`

const runPrologue = `// runGenerated is the generated switch-dispatch twin of EVM.Run, valid for
// every canonical fork table: fields identical across all tables are baked as
// literals, fork-varying ones are read from the active table. Run delegates
// here only for tables in genTables, so ExtraEips-patched copies keep the
// generic loop.
func (evm *EVM) runGenerated(contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	if len(contract.Code) == 0 {
		return nil, gas, mdgas.MdGasUsage{}, nil
	}
	evm.returnData = nil

	var (
		op          OpCode
		callContext = getCallContext(contract, input, gas)
		pc          = uint64(0)
		cost        uint64
		pcCopy      uint64
		gasCopy     uint64
		callGas     uint64
		logged      bool
		res         []byte
		tracer      = evm.config.Tracer
		debug       = tracer != nil && (tracer.OnOpcode != nil || tracer.OnGasChange != nil || tracer.OnFault != nil)
		trace       = dbg.TraceInstructions && evm.intraBlockState.Trace()
	)

	restoreReadonly := readOnly && !evm.readOnly
	if restoreReadonly {
		evm.readOnly = true
	}
	evm.depth++
	defer func() {
		gasUsed.StateSpill = callContext.stateGasSpill
		gasUsed.State = int64(gas.State) - int64(callContext.stateGas) + int64(callContext.stateGasSpill)
		callContext.put()
		if restoreReadonly {
			evm.readOnly = false
		}
		evm.depth--
	}()

	if debug {
		defer func() {
			if err == nil {
				return
			}
			if !logged && tracer.OnOpcode != nil {
				tracer.OnOpcode(pcCopy, byte(op), gasCopy, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
			}
			if logged && tracer.OnFault != nil {
				tracer.OnFault(pcCopy, byte(op), gasCopy, cost, callContext, evm.depth, VMErrorFromErr(err))
			}
		}()
	}

	anyTrace := dbg.TraceDynamicGas || debug || trace
	stack := &callContext.Stack

	jt := evm.jt
	_ = jt[0]

	for {
		callContext.cacheGen++
		if debug {
			logged, pcCopy, gasCopy = false, pc, callContext.gas
		}
		op = contract.GetOp(pc)
		switch op {
`

const runEpilogue = `		}
		if err != nil {
			break
		}
		pc++
	}

	if errors.Is(err, errStopToken) {
		err = nil
	}

	return res, callContext.Gas(), mdgas.MdGasUsage{}, err
}

func init() {
	runGeneratedFn = (*EVM).runGenerated
}
`

func main() {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "cannot resolve generator source path")
		os.Exit(1)
	}
	vmDir := filepath.Dir(filepath.Dir(sourceFile))

	funcs := packageFuncs(vmDir)
	forks := vm.DumpAllDispatch()
	plans := analyze(forks, funcs, "")

	e := &emitter{}
	e.p("%s", fileHeader)
	e.p("%s", runPrologue)
	baked, entryBased := 0, 0
	for _, p := range plans {
		if p.undefinedEverywhere {
			continue
		}
		e.emitCase(p)
		if p.needsEntry() {
			entryBased++
		} else {
			baked++
		}
	}
	e.emitDefaultCase()
	e.p("%s", runEpilogue)

	formatted, err := format.Source(e.buf.Bytes())
	if err != nil {
		debugPath := filepath.Join(vmDir, "run_dispatch_gen_debug.txt")
		_ = os.WriteFile(debugPath, e.buf.Bytes(), 0644)
		fmt.Fprintf(os.Stderr, "format error: %v\nwrote unformatted output to %s\n", err, debugPath)
		os.Exit(1)
	}
	outPath := filepath.Join(vmDir, "run_dispatch_gen.go")
	if err := os.WriteFile(outPath, formatted, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "cannot write %s: %v\n", outPath, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d bytes; %d fully-baked cases, %d entry-reading cases)\n", outPath, len(formatted), baked, entryBased)
}
