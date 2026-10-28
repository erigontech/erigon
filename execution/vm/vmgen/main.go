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

// vmgen writes run in vm_run_gen.go from runTraced in interpreter.go: the
// same loop with anyTrace false and the fast-path switch, whose cases inline
// the fastOps' and coldOps' execute funcs from instructions.go and eips.go. It also writes
// fast_path_gen_test.go. With -check it reports stale files instead of writing them.
package main

import (
	"bytes"
	"cmp"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"log"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
)

// fastOp is one opcode the untraced loop runs inline, without the jump table.
// Every entry must charge only constant gas, plus copy gas per word for copies,
// and must not fail once its checks pass. TestFastPathMatchesJumpTables pins
// the constants against all forks, and TestRunMatchesRunTraced pins the inlined
// bodies against the jump-table ops.
type fastOp struct {
	name       string
	execute    string // jump-table execute func, inlined as the case body; "" for the makeDup and makePush closures
	gas        string
	pop, push  int
	memorySize string // set for memory ops, which take the fast path only when memory need not grow
	dynamicGas string // the jump-table gas func of a memory op
	allocated  string // a memory op's condition that memory need not grow
	copyLen    string // the stack item a copy charges params.CopyGas per word of
	jump       bool   // a taken jump also charges the JUMPDEST it lands on and steps over it
	fork       bool   // the op is not in every fork, so the case checks that jt has it
	index      string // for a DUP or SWAP that shares its case with its neighbours: the op's distance from DUP1 or SWAP1
	minStack   string // the stack height such a shared case needs, in terms of index
}

func fastOps() []fastOp {
	op := func(name, execute, gas string, pop, push int) fastOp {
		return fastOp{name: name, execute: execute, gas: gas, pop: pop, push: push}
	}
	ops := []fastOp{
		op("PUSH1", "opPush1", "GasFastestStep", 0, 1),
		op("PUSH2", "opPush2", "GasFastestStep", 0, 1),
		op("ADD", "opAdd", "GasFastestStep", 2, 1),
		op("POP", "opPop", "GasQuickStep", 1, 0),
		op("JUMPDEST", "opJumpdest", "params.JumpdestGas", 0, 0),
		{name: "JUMP", execute: "opJump", gas: "GasMidStep", pop: 1, jump: true},
		{name: "JUMPI", execute: "opJumpi", gas: "GasSlowStep", pop: 2, jump: true},
		op("SUB", "opSub", "GasFastestStep", 2, 1),
		op("MUL", "opMul", "GasFastStep", 2, 1),
		op("DIV", "opDiv", "GasFastStep", 2, 1),
		op("LT", "opLt", "GasFastestStep", 2, 1),
		op("GT", "opGt", "GasFastestStep", 2, 1),
		op("EQ", "opEq", "GasFastestStep", 2, 1),
		op("AND", "opAnd", "GasFastestStep", 2, 1),
		op("ISZERO", "opIszero", "GasFastestStep", 1, 1),
		op("CALLVALUE", "opCallValue", "GasQuickStep", 0, 1),
		{name: "MLOAD", execute: "opMload", gas: "GasFastestStep", pop: 1, push: 1, memorySize: "memoryMLoad", dynamicGas: "pureMemoryGascost", allocated: allocated32},
		{name: "MSTORE", execute: "opMstore", gas: "GasFastestStep", pop: 2, memorySize: "memoryMStore", dynamicGas: "pureMemoryGascost", allocated: allocated32},
		// MCOPY is in the jump table only from Cancun on.
		{name: "MCOPY", execute: "opMcopy", gas: "GasFastestStep", pop: 3, memorySize: "memoryMcopy", dynamicGas: "gasMcopy", allocated: "jt[MCOPY].memorySize != nil && callContext.Memory.allocatedCopy(stack.peek(), stack.back(1), stack.back(2))", copyLen: "stack.back(2)"},
	}
	for n := 3; n <= 32; n++ {
		ops = append(ops, op(fmt.Sprintf("PUSH%d", n), "", "GasFastestStep", 0, 1))
	}
	for n := 1; n <= 8; n++ {
		ops = append(ops, op(fmt.Sprintf("DUP%d", n), "", "GasFastestStep", n, n+1))
	}
	for n := 1; n <= 4; n++ {
		ops = append(ops, op(fmt.Sprintf("SWAP%d", n), fmt.Sprintf("opSwap%d", n), "GasFastestStep", n+1, n+1))
	}
	return ops
}

// coldOps are cheap constant-gas ops that run inline in a second switch after
// the fastOps one. A separate switch keeps the hot ops' compare tree as shallow
// as it was; the DUPs and SWAPs share a case each, or Go compiles it to a jump table.
func coldOps() []fastOp {
	op := func(name, execute string) fastOp {
		return fastOp{name: name, execute: execute, gas: "GasQuickStep", push: 1}
	}
	forked := func(o fastOp) fastOp {
		o.fork = true
		return o
	}
	alu := func(name, execute string, pop int) fastOp {
		return fastOp{name: name, execute: execute, gas: "GasFastestStep", pop: pop, push: 1}
	}
	ops := []fastOp{
		alu("SLT", "opSlt", 2),
		alu("SGT", "opSgt", 2),
		alu("OR", "opOr", 2),
		alu("XOR", "opXor", 2),
		alu("NOT", "opNot", 1),
		alu("BYTE", "opByte", 2),
		forked(alu("SHL", "opSHL", 2)),
		forked(alu("SHR", "opSHR", 2)),
		forked(alu("SAR", "opSAR", 2)),
		op("ADDRESS", "opAddress"),
		op("ORIGIN", "opOrigin"),
		op("CALLER", "opCaller"),
		op("CALLDATASIZE", "opCallDataSize"),
		op("CODESIZE", "opCodeSize"),
		op("GASPRICE", "opGasprice"),
		forked(op("RETURNDATASIZE", "opReturnDataSize")),
		op("COINBASE", "opCoinbase"),
		op("TIMESTAMP", "opTimestamp"),
		op("NUMBER", "opNumber"),
		op("DIFFICULTY", "opDifficulty"),
		op("GASLIMIT", "opGasLimit"),
		forked(op("CHAINID", "opChainID")),
		forked(op("BASEFEE", "opBaseFee")),
		forked(op("BLOBBASEFEE", "opBlobBaseFee")),
		forked(op("PUSH0", "opPush0")),
		op("PC", "opPc"),
		op("MSIZE", "opMsize"),
		op("GAS", "opGas"),
		copyOp("CALLDATACOPY", "opCallDataCopy", "memoryCallDataCopy", "gasCallDataCopy"),
		copyOp("CODECOPY", "opCodeCopy", "memoryCodeCopy", "gasCodeCopy"),
		copyOp("RETURNDATACOPY", "opReturnDataCopy", "memoryReturnDataCopy", "gasReturnDataCopy"),
		forked(alu("BLOBHASH", "opBlobHash", 1)),
		forked(fastOp{name: "DUPN", execute: "opDupN", gas: "GasFastestStep", push: 1}),
		forked(fastOp{name: "SWAPN", execute: "opSwapN", gas: "GasFastestStep"}),
		forked(fastOp{name: "EXCHANGE", execute: "opExchange", gas: "GasFastestStep"}),
	}
	for n := 9; n <= 16; n++ {
		ops = append(ops, fastOp{name: fmt.Sprintf("DUP%d", n), gas: "GasFastestStep", pop: n, push: n + 1, index: "int(op-DUP1)", minStack: "int(op-DUP1)+1"})
	}
	for n := 5; n <= 16; n++ {
		ops = append(ops, fastOp{name: fmt.Sprintf("SWAP%d", n), execute: fmt.Sprintf("opSwap%d", n), gas: "GasFastestStep", pop: n + 1, push: n + 1, index: "int(op-SWAP1)", minStack: "int(op-SWAP1)+2"})
	}
	return ops
}

const allocated32 = "callContext.Memory.allocated32(stack.peek())"

// copyOp is a copy into memory from outside it, which writes length bytes at the stack top.
func copyOp(name, execute, memorySize, dynamicGas string) fastOp {
	allocated := "callContext.Memory.allocatedCopy(stack.peek(), stack.peek(), stack.back(2))"
	if name == "RETURNDATACOPY" {
		// RETURNDATACOPY is in the jump table only from Byzantium on.
		allocated = "jt[RETURNDATACOPY].memorySize != nil && " + allocated
	}
	return fastOp{name: name, execute: execute, gas: "GasFastestStep", pop: 3, memorySize: memorySize, dynamicGas: dynamicGas, allocated: allocated, copyLen: "stack.back(2)"}
}

const switchHere = "// execution/vm/vmgen inserts the fastOps switch here.\n"

// runLocals are run's variables an inlined body may use or the inliner writes.
var runLocals = []string{"pc", "evm", "callContext", "res", "err", "gasLeft", "sLen", "stack", "contract", "op"}

// inlineBody returns o's execute func body as statements of run's loop: its
// parameters renamed to run's pc, evm and callContext, and each return turned
// into a step to the next op or a break out of the loop.
func inlineBody(srcs map[string][]byte, o fastOp) string {
	fset := token.NewFileSet()
	var decls []ast.Decl
	for name, src := range srcs {
		f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
		if err != nil {
			log.Fatal(err)
		}
		decls = append(decls, f.Decls...)
	}
	var typ *ast.FuncType
	var body *ast.BlockStmt
	rename := map[string]string{}
	for _, d := range decls {
		fn, ok := d.(*ast.FuncDecl)
		switch {
		case !ok:
		case fn.Name.Name == o.execute:
			typ, body = fn.Type, fn.Body
		case o.execute == "" && strings.HasPrefix(o.name, "DUP") && fn.Name.Name == "makeDup":
			// The DUP closure reads depth, which makeDup derives from the DUP number.
			n, _ := strconv.Atoi(strings.TrimPrefix(o.name, "DUP"))
			rename["depth"] = cmp.Or(o.index, strconv.Itoa(n-1))
			typ, body = closure(fn)
		case o.execute == "" && strings.HasPrefix(o.name, "PUSH") && fn.Name.Name == "makePush":
			// One body serves every PUSH size, so all of them share one case.
			rename["size"], rename["pushByteSize"] = "uint64(op-PUSH0)", "int(op-PUSH0)"
			typ, body = closure(fn)
		}
	}
	if body == nil {
		log.Fatalf("%s: execute func %q not found", o.name, o.execute)
	}
	i := 0
	for _, field := range typ.Params.List {
		for _, name := range field.Names {
			rename[name.Name] = runLocals[i]
			i++
		}
	}
	fields := map[*ast.Ident]bool{}
	for n := range ast.Preorder(body) {
		switch n := n.(type) {
		case *ast.FuncLit:
			log.Fatalf("%s: a closure in the body would take the inlined returns", o.name)
		case *ast.SelectorExpr:
			fields[n.Sel] = true
		case *ast.AssignStmt:
			for _, l := range n.Lhs {
				if id, ok := l.(*ast.Ident); ok && n.Tok == token.DEFINE && slices.Contains(runLocals, id.Name) {
					log.Fatalf("%s: the body declares %s, which shadows run's", o.name, id.Name)
				}
			}
		}
	}
	for n := range ast.Preorder(body) {
		if id, ok := n.(*ast.Ident); ok && !fields[id] && rename[id.Name] != "" {
			id.Name = rename[id.Name]
		}
	}
	astutil.Apply(body, nil, func(c *astutil.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.SelectorExpr:
			// run keeps the gas in gasLeft.
			if x, ok := n.X.(*ast.Ident); ok && x.Name == "callContext" && n.Sel.Name == "gas" {
				c.Replace(ast.NewIdent("gasLeft"))
			}
		case *ast.BasicLit:
			// A shared SWAP case swaps by the op's distance from SWAP1, not by its own constant.
			if o.index != "" && strings.HasPrefix(o.name, "SWAP") && n.Value == strings.TrimPrefix(o.name, "SWAP") {
				c.Replace(ast.NewIdent(o.index + "+1"))
			}
		}
		return true
	})
	saveAroundCalls(body)
	return inlineReturns(text(fset, body), o)
}

// outOfLine are the funcs run's inlined bodies call that Go does not inline.
var outOfLine = []string{"Mul", "Div", "SetBytes", "ILsh", "validJumpdest", "Copy", "addressWord", "Slt", "Sgt", "Byte", "Lsh", "Rsh", "SRsh", "SetFromData"}

// saveAroundCalls stores gasLeft and pc in callContext before each statement of body
// that calls an outOfLine func, and loads them back after it. Neither is then live
// across a call, so Go does not spill them at the top of run's loop, on every op.
func saveAroundCalls(body *ast.BlockStmt) {
	save := mustStmt("callContext.gas, callContext.savedPC = gasLeft, pc")
	load := mustStmt("gasLeft, pc = callContext.gas, callContext.savedPC")
	ast.Inspect(body, func(n ast.Node) bool {
		block, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		var list []ast.Stmt
		for _, s := range block.List {
			switch st := s.(type) {
			case *ast.ExprStmt, *ast.AssignStmt:
				if callsOutOfLine(s) {
					list = append(list, save, s, load)
					continue
				}
			case *ast.IfStmt:
				// A call in the condition: the branches must reach the load after the if.
				if st.Init == nil && callsOutOfLine(st.Cond) && !returns(st) {
					list = append(list, save, s, load)
					continue
				}
			}
			list = append(list, s)
		}
		block.List = list
		return true
	})
}

func returns(n ast.Node) (found bool) {
	ast.Inspect(n, func(n ast.Node) bool {
		if _, ok := n.(*ast.ReturnStmt); ok {
			found = true
		}
		return !found
	})
	return found
}

func callsOutOfLine(s ast.Node) (found bool) {
	ast.Inspect(s, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && (slices.Contains(outOfLine, sel.Sel.Name) || sel.Sel.Name == "Set" && isMemory(sel.X)) {
				found = true
			}
		}
		return !found
	})
	return found
}

// isMemory reports whether x is a Memory field: uint256's inlined Set has the same name as Memory.Set.
func isMemory(x ast.Expr) bool {
	sel, ok := x.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "Memory"
}

// mustStmt parses src as a statement without positions, so the printer lays it
// out on its own line wherever it lands.
func mustStmt(src string) ast.Stmt {
	f, err := parser.ParseFile(token.NewFileSet(), "", "package p\nfunc _() {\n"+src+"\n}", 0)
	if err != nil {
		log.Fatal(err)
	}
	s := f.Decls[0].(*ast.FuncDecl).Body.List[0]
	ast.Inspect(s, func(n ast.Node) bool {
		if n == nil {
			return false
		}
		v := reflect.ValueOf(n).Elem()
		for _, f := range v.Fields() {
			if f.Type() == reflect.TypeFor[token.Pos]() {
				f.SetInt(int64(token.NoPos))
			}
		}
		return true
	})
	return s
}

// closure returns the type and body of the func literal that fn returns.
func closure(fn *ast.FuncDecl) (*ast.FuncType, *ast.BlockStmt) {
	for n := range ast.Preorder(fn.Body) {
		if lit, ok := n.(*ast.FuncLit); ok {
			return lit.Type, lit.Body
		}
	}
	return nil, nil
}

// inlineReturns replaces each `return pc, res, err` in the printed body block:
// a return without an error moves to the next op, one with an error leaves the
// loop. It returns the block's statements without the braces.
func inlineReturns(body string, o fastOp) string {
	const prefix = "package p\nfunc _() "
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", prefix+body, 0)
	if err != nil {
		log.Fatal(err)
	}
	src := func(n ast.Node) string { return body[int(n.Pos())-1-len(prefix) : int(n.End())-1-len(prefix)] }
	var rets []*ast.ReturnStmt
	for n := range ast.Preorder(f) {
		if r, ok := n.(*ast.ReturnStmt); ok {
			rets = append(rets, r)
		}
	}
	for _, r := range slices.Backward(rets) {
		next, res, err := src(r.Results[0]), src(r.Results[1]), src(r.Results[2])
		var step string
		switch {
		case err != "nil":
			step = "res, err = " + res + ", " + err + "\nbreak run"
		case res != "nil":
			log.Fatalf("%s: a successful return with data cannot stay in the loop", o.name)
		case next == "pc":
			step = "pc++\ncontinue run"
		case o.jump:
			// A valid destination holds a JUMPDEST: charge it here and step over it.
			// min instead of an if: the if costs every jump a taken branch.
			step = "pc = " + next + "\nskip := min(gasLeft, params.JumpdestGas) / params.JumpdestGas\ngasLeft -= skip * params.JumpdestGas\npc += skip + 1\ncontinue run"
		default:
			step = "pc = " + next + "\npc++\ncontinue run"
		}
		body = body[:int(r.Pos())-1-len(prefix)] + step + body[int(r.End())-1-len(prefix):]
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(body, "{"), "}"))
}

func text(fset *token.FileSet, n any) string {
	var b bytes.Buffer
	if err := format.Node(&b, fset, n); err != nil {
		log.Fatal(err)
	}
	return b.String()
}

func fastSwitch(srcs map[string][]byte, ops []fastOp) string {
	var names, codes []string
	for i := range ops {
		o := &ops[i]
		var cond []string
		if o.fork {
			cond = append(cond, "jt["+o.name+"].constantGas != 0")
		}
		switch {
		case o.minStack != "":
			cond = append(cond, "sLen >= "+o.minStack)
		case o.pop > 0:
			cond = append(cond, fmt.Sprintf("sLen >= %d", o.pop))
		}
		switch o.push - o.pop {
		case 1:
			cond = append(cond, "sLen < stackLimit")
		case 0, -1, -2, -3:
		default:
			log.Fatalf("%s: stack growth %d needs its own bound", o.name, o.push-o.pop)
		}
		if o.allocated != "" {
			cond = append(cond, o.allocated)
		}
		gas := o.gas
		if o.copyLen != "" {
			gas += " + params.CopyGas*ToWordSize(" + o.copyLen + ".Uint64())"
		}
		cond = append(cond, "gasLeft >= "+gas)
		code := fmt.Sprintf("if %s {\ngasLeft -= %s\n%s\n}\n", strings.Join(cond, " && "), gas, inlineBody(srcs, *o))
		// Ops with the same code share a case: each case deepens run's compare tree.
		if n := len(codes); n > 0 && codes[n-1] == code {
			names[n-1] += ", " + o.name
			continue
		}
		names, codes = append(names, o.name), append(codes, code)
	}
	var b strings.Builder
	b.WriteString("switch op {\n")
	for i := range names {
		fmt.Fprintf(&b, "case %s:\n%s", names[i], codes[i])
	}
	b.WriteString("}\n")
	return b.String()
}

// coldSwitch is fastSwitch behind a lookup in isColdOp, so a generic op skips
// its compare tree with one branch.
func coldSwitch(srcs map[string][]byte, ops []fastOp) string {
	return "if isColdOp[op] {\n" + fastSwitch(srcs, ops) + "}\n"
}

// coldTable declares isColdOp, which is true for the ops of coldSwitch.
func coldTable(ops []fastOp) string {
	var b strings.Builder
	b.WriteString("\n// isColdOp marks the ops of run's second fast-path switch.\nvar isColdOp = [256]bool{\n")
	for i := range ops {
		b.WriteString(ops[i].name + ": true,\n")
	}
	b.WriteString("}\n")
	return b.String()
}

// untraced returns runTraced as run in a file of its own, with anyTrace set
// to false, without the code this makes dead, with fast in place of the
// switchHere comment, and with decls after it.
func untraced(traced []byte, fast, decls string) []byte {
	if !bytes.Contains(traced, []byte(switchHere)) {
		log.Fatal("interpreter.go: the fast-path switch comment is missing")
	}
	src := dropDeadCode(bytes.Replace(traced, []byte(switchHere), []byte(fast), 1))
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "interpreter.go", src, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}
	// interpreter.go holds more than runTraced: keep only it and the imports it uses.
	f.Decls = slices.DeleteFunc(f.Decls, func(d ast.Decl) bool {
		if d, ok := d.(*ast.FuncDecl); ok {
			return d.Name.Name != "runTraced"
		}
		return d.(*ast.GenDecl).Tok != token.IMPORT
	})
	for _, s := range slices.Clone(f.Imports) {
		if p, _ := strconv.Unquote(s.Path.Value); !astutil.UsesImport(f, p) {
			astutil.DeleteImport(fset, f, p)
		}
	}
	// The fast path charges params.JumpdestGas; runTraced itself does not use params.
	astutil.AddImport(fset, f, "github.com/erigontech/erigon/execution/protocol/params")
	var b bytes.Buffer
	b.WriteString("// Code generated by execution/vm/vmgen from interpreter.go. DO NOT EDIT.\n\npackage vm\n\n")
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.GenDecl:
			b.WriteString(text(fset, d) + "\n\n")
		case *ast.FuncDecl:
			d.Doc, d.Name.Name = nil, "run"
			for n := range ast.Preorder(d) {
				if id, ok := n.(*ast.Ident); ok && id.Name == "anyTrace" {
					id.Name = "false"
				}
			}
			b.WriteString("// run is runTraced without the tracing code and with the fast path.\n" + text(fset, &printer.CommentedNode{Node: d, Comments: f.Comments}) + "\n")
		}
	}
	b.WriteString(decls)
	out, err := format.Source(b.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	return out
}

// dropDeadCode returns src without runTraced's statements that run with
// anyTrace false never executes, the locals only they use, and their comments.
// It edits the text, not the AST, so the printer leaves no gaps in their place.
func dropDeadCode(src []byte) []byte {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "interpreter.go", src, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}
	var body *ast.BlockStmt
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Name.Name == "runTraced" {
			body = fn.Body
		}
	}
	var dead []ast.Node
	isDead := func(p token.Pos) bool {
		return slices.ContainsFunc(dead, func(n ast.Node) bool { return n.Pos() <= p && p < n.End() })
	}
	for n := range ast.Preorder(body) {
		if s, ok := n.(*ast.IfStmt); ok && s.Init == nil && s.Else == nil && traceOnly(s.Cond) && !isDead(s.Pos()) {
			dead = append(dead, s)
		}
	}
	uses := map[string]int{}
	for n := range ast.Preorder(body) {
		if id, ok := n.(*ast.Ident); ok && !isDead(id.Pos()) {
			uses[id.Name]++
		}
	}
	for n := range ast.Preorder(body) {
		if s, ok := n.(*ast.ValueSpec); ok && !isDead(s.Pos()) &&
			!slices.ContainsFunc(s.Names, func(id *ast.Ident) bool { return uses[id.Name] > 1 }) {
			dead = append(dead, s)
		}
	}
	cmap := ast.NewCommentMap(fset, f, f.Comments)
	slices.SortFunc(dead, func(a, b ast.Node) int { return cmp.Compare(b.Pos(), a.Pos()) })
	for _, n := range dead {
		from, to := fset.Position(n.Pos()).Offset, fset.Position(n.End()).Offset
		for _, g := range cmap[n] {
			from, to = min(from, fset.Position(g.Pos()).Offset), max(to, fset.Position(g.End()).Offset)
		}
		from = bytes.LastIndexByte(src[:from], '\n') + 1
		to += bytes.IndexByte(src[to:], '\n') + 1
		src = append(src[:from:from], src[to:]...)
	}
	return src
}

// traceOnly reports whether cond is anyTrace or anyTrace && x, so is false in run.
func traceOnly(cond ast.Expr) bool {
	switch c := cond.(type) {
	case *ast.Ident:
		return c.Name == "anyTrace"
	case *ast.BinaryExpr:
		return c.Op == token.LAND && traceOnly(c.X)
	}
	return false
}

func testTable(ops []fastOp) []byte {
	var b strings.Builder
	b.WriteString("// Code generated by execution/vm/vmgen. DO NOT EDIT.\n\npackage vm\n\n")
	b.WriteString("import \"github.com/erigontech/erigon/execution/protocol/params\"\n\n")
	b.WriteString("var fastPathOps = map[OpCode]fastPathWant{\n")
	for i := range ops {
		o := &ops[i]
		execute, memorySize, dynamicGas := cmp.Or(o.execute, "nil"), cmp.Or(o.memorySize, "nil"), cmp.Or(o.dynamicGas, "nil")
		fmt.Fprintf(&b, "%s: {%s, %s, %d, %d, %s, %s},\n", o.name, execute, o.gas, o.pop, o.push, memorySize, dynamicGas)
	}
	b.WriteString("}\n")
	out, err := format.Source([]byte(b.String()))
	if err != nil {
		log.Fatal(err)
	}
	return out
}

// read returns the file with LF line endings: Git checks it out with CRLF on Windows.
func read(name string) []byte {
	b, err := os.ReadFile(name)
	if err != nil {
		log.Fatal(err)
	}
	return bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n"))
}

func main() {
	check := len(os.Args) > 1 && os.Args[1] == "-check"
	srcs := map[string][]byte{"instructions.go": read("instructions.go"), "eips.go": read("eips.go")}
	switches := "sLen := stack.len()\n" + fastSwitch(srcs, fastOps()) + coldSwitch(srcs, coldOps())
	ops := append(fastOps(), coldOps()...)
	files := []struct {
		name string
		data []byte
	}{
		{"vm_run_gen.go", untraced(read("interpreter.go"), switches, coldTable(coldOps()))},
		{"fast_path_gen_test.go", testTable(ops)},
	}
	var stale []string
	for _, f := range files {
		if !check {
			if err := os.WriteFile(f.name, f.data, 0o644); err != nil {
				log.Fatal(err)
			}
			continue
		}
		// Compare after formatting with this toolchain: gofmt output varies across Go versions.
		got, err := os.ReadFile(f.name)
		if err == nil {
			got, err = format.Source(got)
		}
		if err != nil || !bytes.Equal(got, f.data) {
			stale = append(stale, f.name)
		}
	}
	if len(stale) > 0 {
		log.Fatalf("stale %v: run go generate ./execution/vm", stale)
	}
}
