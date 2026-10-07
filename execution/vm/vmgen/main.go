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
// the fastOps' execute funcs from instructions.go. Next to run it writes a copy
// without the trace of each func that takes one. It also writes
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
	"go/types"
	"log"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
)

// fastOp is one opcode the untraced loop runs inline, without the jump table.
// Every entry must charge only constant gas and must not fail once its checks
// pass. TestFastPathMatchesJumpTables pins the constants against all forks, and
// TestRunMatchesRunTraced pins the inlined bodies against the jump-table ops.
type fastOp struct {
	name       string
	execute    string // jump-table execute func, inlined as the case body; "" for the makeDup and makePush closures
	gas        string
	pop, push  int
	memorySize string // set for memory ops, which take the fast path only when memory need not grow
	jump       bool   // a taken jump also charges the JUMPDEST it lands on and steps over it
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
		{name: "MLOAD", execute: "opMload", gas: "GasFastestStep", pop: 1, push: 1, memorySize: "memoryMLoad"},
		{name: "MSTORE", execute: "opMstore", gas: "GasFastestStep", pop: 2, memorySize: "memoryMStore"},
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

const switchHere = "// execution/vm/vmgen inserts the fastOps switch here.\n"

// runLocals are run's variables an inlined body may use or the inliner writes.
var runLocals = []string{"pc", "evm", "callContext", "res", "err", "gasLeft", "sLen", "stack", "contract", "op"}

// inlineBody returns o's execute func body as statements of run's loop: its
// parameters renamed to run's pc, evm and callContext, and each return turned
// into a step to the next op or a break out of the loop.
func inlineBody(instructions []byte, o fastOp) string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "instructions.go", instructions, parser.SkipObjectResolution)
	if err != nil {
		log.Fatal(err)
	}
	var typ *ast.FuncType
	var body *ast.BlockStmt
	rename := map[string]string{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		switch {
		case !ok:
		case fn.Name.Name == o.execute:
			typ, body = fn.Type, fn.Body
		case o.execute == "" && strings.HasPrefix(o.name, "DUP") && fn.Name.Name == "makeDup":
			// The DUP closure reads depth, which makeDup derives from the DUP number.
			n, _ := strconv.Atoi(strings.TrimPrefix(o.name, "DUP"))
			rename["depth"] = strconv.Itoa(n - 1)
			typ, body = closure(fn)
		case o.execute == "" && strings.HasPrefix(o.name, "PUSH") && fn.Name.Name == "makePush":
			// One body serves every PUSH size, so all of them share one case.
			rename["size"], rename["pushByteSize"] = "uint64(op-PUSH0)", "int(op-PUSH0)"
			typ, body = closure(fn)
		}
	}
	if body == nil {
		log.Fatalf("%s: execute func %q not found in instructions.go", o.name, o.execute)
	}
	toRunLocals(o.name, typ, body, rename, "")
	return inlineReturns(text(fset, body), o)
}

// toRunLocals renames, in body, the params of typ to run's locals and the other
// names to their rename entries. With gas set, callContext.gas becomes gas, where
// run keeps the gas; without it, a body that reads the gas is an error.
func toRunLocals(name string, typ *ast.FuncType, body *ast.BlockStmt, rename map[string]string, gas string) {
	i := 0
	for _, field := range typ.Params.List {
		for _, n := range field.Names {
			rename[n.Name] = runLocals[i]
			i++
		}
	}
	fields := map[*ast.Ident]bool{}
	for n := range ast.Preorder(body) {
		switch n := n.(type) {
		case *ast.FuncLit:
			log.Fatalf("%s: a closure in the body would take the inlined returns", name)
		case *ast.SelectorExpr:
			fields[n.Sel] = true
		case *ast.AssignStmt:
			for _, l := range n.Lhs {
				if id, ok := l.(*ast.Ident); ok && n.Tok == token.DEFINE && (slices.Contains(runLocals, id.Name) || id.Name == "o") {
					log.Fatalf("%s: the body declares %s, which shadows run's", name, id.Name)
				}
			}
		}
	}
	astutil.Apply(body, func(c *astutil.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.SelectorExpr:
			if x, ok := n.X.(*ast.Ident); ok && rename[x.Name] == "callContext" && n.Sel.Name == "gas" {
				if gas == "" {
					log.Fatalf("%s: the body reads gas, which run keeps in gasLeft", name)
				}
				c.Replace(ast.NewIdent(gas))
				return false
			}
		case *ast.Ident:
			if !fields[n] && rename[n.Name] != "" {
				n.Name = rename[n.Name]
			}
		}
		return true
	}, nil)
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
		if len(r.Results) == 1 {
			// A tail call to another op func, which keeps the gas in callContext.
			step := "callContext.gas = gasLeft\npc, res, err = " + src(r.Results[0]) + "\ngasLeft = callContext.gas\nif err != nil {\nbreak run\n}\npc++\ncontinue run"
			body = body[:int(r.Pos())-1-len(prefix)] + step + body[int(r.End())-1-len(prefix):]
			continue
		}
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

func fastSwitch(instructions []byte, ops []fastOp) string {
	var names, codes []string
	for _, o := range ops {
		var cond []string
		if o.pop > 0 {
			cond = append(cond, fmt.Sprintf("sLen >= %d", o.pop))
		}
		switch o.push - o.pop {
		case 1:
			cond = append(cond, "sLen < stackLimit")
		case 0, -1, -2:
		default:
			log.Fatalf("%s: stack growth %d needs its own bound", o.name, o.push-o.pop)
		}
		if o.memorySize != "" {
			cond = append(cond, "callContext.Memory.allocated32(stack.peek())")
		}
		cond = append(cond, "gasLeft >= "+o.gas)
		code := fmt.Sprintf("if %s {\ngasLeft -= %s\n%s\n}\n", strings.Join(cond, " && "), o.gas, inlineBody(instructions, o))
		// Ops with the same code share a case: each case deepens run's compare tree.
		if n := len(codes); n > 0 && codes[n-1] == code {
			names[n-1] += ", " + o.name
			continue
		}
		names, codes = append(names, o.name), append(codes, code)
	}
	var b strings.Builder
	b.WriteString("sLen := stack.len()\nswitch op {\n")
	for i := range names {
		fmt.Fprintf(&b, "case %s:\n%s", names[i], codes[i])
	}
	b.WriteString("}\n")
	return b.String()
}

// untraced returns runTraced as run in a file of its own, with anyTrace set
// to false, without the code this makes dead, with fast in place of the
// switchHere comment and with direct after the generation step.
func untraced(traced []byte, fast, direct string) []byte {
	if !bytes.Contains(traced, []byte(switchHere)) {
		log.Fatal("interpreter.go: the fast-path switch comment is missing")
	}
	src := dropDeadCode(bytes.Replace(traced, []byte(switchHere), []byte(fast), 1))
	if bytes.Count(src, []byte(cacheGenStep)) != 1 {
		log.Fatalf("interpreter.go: want one %q", cacheGenStep)
	}
	src = bytes.Replace(src, []byte(cacheGenStep), []byte(cacheGenStep+direct), 1)
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

func testTable(ops []fastOp, gasOps [][2]string) []byte {
	var b strings.Builder
	b.WriteString("// Code generated by execution/vm/vmgen. DO NOT EDIT.\n\npackage vm\n\n")
	b.WriteString("import \"github.com/erigontech/erigon/execution/protocol/params\"\n\n")
	b.WriteString("var fastPathOps = map[OpCode]fastPathWant{\n")
	for _, o := range ops {
		execute, memorySize := cmp.Or(o.execute, "nil"), cmp.Or(o.memorySize, "nil")
		fmt.Fprintf(&b, "%s: {%s, %s, %d, %d, %s},\n", o.name, execute, o.gas, o.pop, o.push, memorySize)
	}
	b.WriteString("}\n\nvar gasExecuteOps = map[OpCode]gasExecuteFunc{\n")
	for _, o := range gasOps {
		fmt.Fprintf(&b, "%s: %s,\n", o[0], o[1])
	}
	b.WriteString("}\n")
	out, err := format.Source([]byte(b.String()))
	if err != nil {
		log.Fatal(err)
	}
	return out
}

// cacheGenStep is where run calls the gasExecute ops: after the per-op generation
// moves, which the ops' memos read.
const cacheGenStep = "callContext.cacheGen++\n"

// gasExecuteOps returns the ops the jump tables give a gasExecute, with the func:
// run inlines the func's copy without the trace. TestFastPathMatchesJumpTables
// fails for a table whose gasExecute is not here.
func gasExecuteOps() [][2]string {
	return [][2]string{{"SLOAD", "opSloadEIP2929"}}
}

// directCalls returns run's switch over the gasExecute ops, which runs their copies
// without the trace, inlined, after the generic path's stack and constant-gas checks.
// An op that fails a check goes on to the generic path, which reports it. It takes
// the inlined copies out of copies.
func directCalls(ops [][2]string, copies map[string]string) string {
	var b strings.Builder
	b.WriteString("switch op {\n")
	for _, o := range ops {
		fmt.Fprintf(&b, `case %s:
	o := &jt[%s]
	if o.gasExecute == nil || uint(stack.len()-o.numPop) > uint(o.maxStack-o.numPop) || gasLeft < o.constantGas {
		break
	}
	gasLeft -= o.constantGas
	%s
`, o[0], o[0], inlineCopy(copies, o[1]+"Run"))
		delete(copies, o[1]+"Run")
	}
	b.WriteString("}\n")
	return b.String()
}

// inlineCopy returns the body of the copy name as statements of run's loop: its
// parameters renamed to run's pc, evm and callContext, its gas to gasLeft, and each
// return turned into a step to the next op or a break out of the loop.
func inlineCopy(copies map[string]string, name string) string {
	code, ok := copies[name]
	if !ok {
		log.Fatalf("%s: not among the copies without the trace", name)
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", "package p\n"+code, parser.SkipObjectResolution)
	if err != nil {
		log.Fatal(err)
	}
	fn := f.Decls[0].(*ast.FuncDecl)
	toRunLocals(name, fn.Type, fn.Body, map[string]string{}, "gasLeft")
	return inlineReturns(text(fset, fn.Body), fastOp{name: name})
}

// traceFree returns, by name, for each func of operations_acl.go that takes a t *opTrace,
// its copy for run: named with a Run suffix, without t and its `if t != nil`
// statements. Only the copies run inlines are used; the rest are dropped.
func traceFree() map[string]string {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "operations_acl.go", read("operations_acl.go"), parser.SkipObjectResolution)
	if err != nil {
		log.Fatal(err)
	}
	var funcs []*ast.FuncDecl
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && traceParam(fn) >= 0 {
			funcs = append(funcs, fn)
		}
	}
	copies := map[string]string{}
	for _, fn := range funcs {
		name := fn.Name.Name
		fn.Type.Params.List = slices.Delete(fn.Type.Params.List, traceParam(fn), traceParam(fn)+1)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.BlockStmt:
				n.List = slices.DeleteFunc(n.List, isTraceIf)
			case *ast.CaseClause:
				n.Body = slices.DeleteFunc(n.Body, isTraceIf)
			case *ast.CallExpr:
				// A call that outlived the trace statements passes t on: it goes
				// to the traced func itself, which skips its own trace on nil.
				for i, a := range n.Args {
					if isT(a) {
						n.Args[i] = ast.NewIdent("nil")
					}
				}
			}
			return true
		})
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if e, ok := n.(ast.Expr); ok && isT(e) {
				log.Fatalf("%s: t is used outside an `if t != nil` statement", name)
			}
			return true
		})
		fn.Doc, fn.Name.Name = nil, name+"Run"
		// The copy has no comments, so its blank lines are where the trace was.
		code := regexp.MustCompile(`\n\s*\n`).ReplaceAllString(text(fset, fn), "\n")
		copies[name+"Run"] = fmt.Sprintf("\n// %sRun is %s without the trace.\n%s\n", name, name, code)
	}
	return copies
}

// traceParam returns the index of fn's t *opTrace parameter, or -1.
func traceParam(fn *ast.FuncDecl) int {
	return slices.IndexFunc(fn.Type.Params.List, func(p *ast.Field) bool {
		star, ok := p.Type.(*ast.StarExpr)
		if !ok || types.ExprString(star.X) != "opTrace" {
			return false
		}
		if len(p.Names) != 1 || p.Names[0].Name != "t" {
			log.Fatalf("%s: the *opTrace parameter must be t of its own", fn.Name.Name)
		}
		return true
	})
}

func isT(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "t"
}

// isTraceIf reports whether s is `if t != nil { ... }`.
func isTraceIf(s ast.Stmt) bool {
	is, ok := s.(*ast.IfStmt)
	if !ok || is.Init != nil || is.Else != nil {
		return false
	}
	c, ok := is.Cond.(*ast.BinaryExpr)
	return ok && c.Op == token.NEQ && isT(c.X) && types.ExprString(c.Y) == "nil"
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
	ops := fastOps()
	gasOps, copies := gasExecuteOps(), traceFree()
	direct := directCalls(gasOps, copies)
	files := []struct {
		name string
		data []byte
	}{
		{"vm_run_gen.go", untraced(read("interpreter.go"), fastSwitch(read("instructions.go"), ops), direct)},
		{"fast_path_gen_test.go", testTable(ops, gasOps)},
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
