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

// Dispatch-loop generator: emits run_amsterdam_gen.go, a copy of the
// interpreter loop where the jump-table load and per-operation metadata reads
// are replaced by a switch with compile-time constants and direct calls.
//
// The live amsterdam table (obtained via vm.DumpAmsterdamDispatch, so the
// fork-constructor chain is never re-implemented here) supplies the metadata.
// Table slots holding named functions become direct calls; slots holding
// closures (makePush/makeDup/makeSwap/makeLog products and gas-func factories)
// are re-exposed as generated package vars initialised from the table itself,
// which keeps their semantics identical by construction.
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
// package, parsed from source. Used to verify that every symbol the dump
// reports as "named" really is a directly callable package function.
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

type emitter struct {
	buf   bytes.Buffer
	vars  []string // generated closure-var declarations
	funcs map[string]bool
	fork  string // e.g. "Amsterdam" — suffix for generated identifiers
	table string // e.g. "amsterdamInstructionSet"
}

func (e *emitter) p(format string, args ...any) {
	fmt.Fprintf(&e.buf, format, args...)
}

// callable resolves a table symbol to a Go expression the generated code can
// call: the bare function name when it is a named package function, otherwise
// a generated package var initialised from the live table slot.
func (e *emitter) callable(symbol, kind string, op byte, opName string) string {
	name := strings.TrimPrefix(symbol, symbolPrefix)
	if e.funcs[name] && !strings.Contains(name, ".") {
		return name
	}
	varName := fmt.Sprintf("%s%s%s", kind, sanitize(opName), e.fork)
	e.vars = append(e.vars, fmt.Sprintf("%s = %s[OpCode(0x%02X)].%s", varName, e.table, op, kindField(kind)))
	return varName
}

func kindField(kind string) string {
	switch kind {
	case "exec":
		return "execute"
	case "dyn":
		return "dynamicGas"
	case "mem":
		return "memorySize"
	}
	panic("unknown kind " + kind)
}

func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

const errReturn = "return nil, callContext.Gas(), mdgas.MdGasUsage{}, "

func (e *emitter) emitDebugBlock() {
	e.p("if debug {\n")
	e.p("if tracer.OnGasChange != nil {\n")
	e.p("tracer.OnGasChange(gasCopy, gasCopy-cost, tracing.GasChangeCallOpCode)\n")
	e.p("}\n")
	e.p("if tracer.OnOpcode != nil {\n")
	e.p("tracer.OnOpcode(pc, byte(op), gasCopy, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))\n")
	e.p("logged = true\n")
	e.p("}\n")
	e.p("}\n")
}

func (e *emitter) emitTraceBlock() {
	e.p("if trace {\n")
	e.p("traceInstructionPrint(evm, op, pc, callGas, cost, callContext)\n")
	e.p("}\n")
}

func (e *emitter) emitCase(entry vm.DispatchEntry) {
	e.p("case OpCode(0x%02X): // %s\n", entry.Op, entry.Name)
	e.p("cost = %d\n", entry.ConstantGas)
	if entry.NumPop > 0 {
		e.p("if sLen := callContext.Stack.len(); sLen < %d {\n", entry.NumPop)
		e.p(errReturn+"&ErrStackUnderflow{stackLen: sLen, required: %d}\n", entry.NumPop)
		if entry.MaxStack < 1024 {
			e.p("} else if sLen > %d {\n", entry.MaxStack)
			e.p(errReturn+"&ErrStackOverflow{stackLen: sLen, limit: %d}\n", entry.MaxStack)
		}
		e.p("}\n")
	} else if entry.MaxStack < 1024 {
		e.p("if sLen := callContext.Stack.len(); sLen > %d {\n", entry.MaxStack)
		e.p(errReturn+"&ErrStackOverflow{stackLen: sLen, limit: %d}\n", entry.MaxStack)
		e.p("}\n")
	}
	if entry.ConstantGas > 0 {
		e.p("if callContext.gas < %d {\n", entry.ConstantGas)
		e.p(errReturn + "ErrOutOfGas\n")
		e.p("}\n")
		e.p("callContext.gas -= %d\n", entry.ConstantGas)
	}

	hasDyn := entry.DynamicGas != ""
	hasMem := entry.MemorySize != ""
	if hasDyn {
		memorySizeExpr := "0"
		if hasMem {
			memFn := e.callable(entry.MemorySize, "mem", entry.Op, entry.Name)
			e.p("var memorySize uint64\n")
			e.p("if memSize, overflow := %s(callContext); overflow {\n", memFn)
			e.p(errReturn + "ErrGasUintOverflow\n")
			e.p("} else if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {\n")
			e.p(errReturn + "ErrGasUintOverflow\n")
			e.p("}\n")
			memorySizeExpr = "memorySize"
		}
		dynFn := e.callable(entry.DynamicGas, "dyn", entry.Op, entry.Name)
		e.p("evm.callGasTemp = 0\n")
		e.p("var dynamicCost mdgas.MdGas\n")
		e.p("dynamicCost, err = %s(evm, callContext, callContext.Gas(), %s)\n", dynFn, memorySizeExpr)
		e.p("if err != nil {\n")
		e.p("if !errors.Is(err, ErrOutOfGas) {\n")
		e.p("err = fmt.Errorf(\"%%w: %%w\", ErrOutOfGas, err)\n")
		e.p("}\n")
		e.p(errReturn + "err\n")
		e.p("}\n")
		e.p("if anyTrace {\n")
		e.p("cost += dynamicCost.Regular\n")
		e.p("callGas = %d + dynamicCost.Regular - evm.CallGasTemp()\n", entry.ConstantGas)
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
	}

	e.emitDebugBlock()
	if hasMem {
		e.p("if memorySize > 0 {\n")
		e.p("callContext.Memory.Resize(memorySize)\n")
		e.p("}\n")
	}
	e.emitTraceBlock()
	execFn := e.callable(entry.Execute, "exec", entry.Op, entry.Name)
	e.p("pc, res, err = %s(pc, evm, callContext)\n", execFn)
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

const runPrologue = `// run%[1]s is the generated switch-dispatch twin of EVM.Run for the
// unmodified %[1]s jump table. Run delegates here on pointer identity, so
// ExtraEips (which copy the table) and unlisted forks keep the generic loop.
func (evm *EVM) run%[1]s(contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
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

`

func main() {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "cannot resolve generator source path")
		os.Exit(1)
	}
	vmDir := filepath.Dir(filepath.Dir(sourceFile))

	funcs := packageFuncs(vmDir)
	forks := []struct {
		Name    string
		Table   string
		Entries []vm.DispatchEntry
	}{
		{"Amsterdam", "amsterdamInstructionSet", vm.DumpAmsterdamDispatch()},
		{"Cancun", "cancunInstructionSet", vm.DumpCancunDispatch()},
	}

	e := &emitter{funcs: funcs}
	e.p("%s", fileHeader)
	totalVars := 0
	var inits []string
	for _, f := range forks {
		ce := &emitter{funcs: funcs, fork: f.Name, table: f.Table}
		for _, entry := range f.Entries {
			if entry.Undefined {
				continue
			}
			ce.emitCase(entry)
		}
		ce.emitDefaultCase()

		if len(ce.vars) > 0 {
			e.p("// Closure-valued %s table slots, re-exposed as vars so the generated\n", f.Name)
			e.p("// loop calls the exact same function values the table holds.\n")
			e.p("var (\n")
			for _, v := range ce.vars {
				e.p("%s\n", v)
			}
			e.p(")\n\n")
		}
		e.p(runPrologue, f.Name)
		e.buf.Write(ce.buf.Bytes())
		e.p("%s", runEpilogue)
		totalVars += len(ce.vars)
		inits = append(inits, fmt.Sprintf("run%sGen = (*EVM).run%s", f.Name, f.Name))
	}
	e.p("func init() {\n")
	for _, line := range inits {
		e.p("%s\n", line)
	}
	e.p("}\n")

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
	fmt.Fprintf(os.Stderr, "wrote %s (%d bytes, %d closure vars)\n", outPath, len(formatted), totalVars)
}
