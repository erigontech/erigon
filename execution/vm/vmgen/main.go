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
// the fastOps' execute funcs from instructions.go. It also writes
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
			if x, ok := n.X.(*ast.Ident); ok && rename[x.Name] == "callContext" && n.Sel.Name == "gas" {
				log.Fatalf("%s: the body reads gas, which run keeps in gasLeft", o.name)
			}
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
	saveAroundCalls(body)
	return inlineReturns(text(fset, body), o)
}

// outOfLine are the funcs run's inlined bodies call that Go does not inline.
var outOfLine = []string{"Mul", "Div", "SetBytes", "ILsh", "validJumpdest"}

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
			switch s.(type) {
			case *ast.ExprStmt, *ast.AssignStmt:
				if callsOutOfLine(s) {
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

func callsOutOfLine(s ast.Stmt) (found bool) {
	ast.Inspect(s, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && slices.Contains(outOfLine, sel.Sel.Name) {
				found = true
			}
		}
		return !found
	})
	return found
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

// fastLoop returns runTraced in a file of its own, with fast in place of the
// switchHere comment: as run, with anyTrace set to false and without the code
// this makes dead, or, if hooked, as runHooked, which keeps the tracing code and
// turns on only the !anyTrace guards of the fast path.
func fastLoop(traced []byte, fast string, ops []fastOp, hooked bool) []byte {
	if !bytes.Contains(traced, []byte(switchHere)) {
		log.Fatal("interpreter.go: the fast-path switch comment is missing")
	}
	src := bytes.Replace(traced, []byte(switchHere), []byte(fast), 1)
	name, doc := "runHooked", "// runHooked is runTraced with the fast path, whose ops emit no opcode hook.\n"
	if !hooked {
		src = dropDeadCode(src)
		name, doc = "run", "// run is runTraced without the tracing code and with the fast path.\n"
	}
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
			d.Doc, d.Name.Name = nil, name
			guards := map[*ast.Ident]bool{}
			for n := range ast.Preorder(d) {
				if u, ok := n.(*ast.UnaryExpr); ok && u.Op == token.NOT {
					if id, ok := u.X.(*ast.Ident); ok {
						guards[id] = true
					}
				}
			}
			for n := range ast.Preorder(d) {
				if id, ok := n.(*ast.Ident); ok && id.Name == "anyTrace" && (!hooked || guards[id]) {
					id.Name = "false"
				}
			}
			b.WriteString(doc + text(fset, &printer.CommentedNode{Node: d, Comments: f.Comments}) + "\n")
		}
	}
	if hooked {
		mask := []string{"byte(STOP)"}
		for _, o := range ops {
			mask = append(mask, "byte("+o.name+")")
		}
		b.WriteString("\n// fastPathMask holds the ops runHooked runs without the opcode hook: the fast-path\n" +
			"// ops and the STOP past the end of the code.\n" +
			"var fastPathMask = tracing.NewOpcodeMask(" + strings.Join(mask, ", ") + ")\n")
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

func testTable(ops []fastOp) []byte {
	var b strings.Builder
	b.WriteString("// Code generated by execution/vm/vmgen. DO NOT EDIT.\n\npackage vm\n\n")
	b.WriteString("import \"github.com/erigontech/erigon/execution/protocol/params\"\n\n")
	b.WriteString("var fastPathOps = map[OpCode]fastPathWant{\n")
	for _, o := range ops {
		execute, memorySize := cmp.Or(o.execute, "nil"), cmp.Or(o.memorySize, "nil")
		fmt.Fprintf(&b, "%s: {%s, %s, %d, %d, %s},\n", o.name, execute, o.gas, o.pop, o.push, memorySize)
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
	ops := fastOps()
	fast := fastSwitch(read("instructions.go"), ops)
	files := []struct {
		name string
		data []byte
	}{
		{"vm_run_gen.go", fastLoop(read("interpreter.go"), fast, ops, false)},
		{"vm_run_hooked_gen.go", fastLoop(read("interpreter.go"), fast, ops, true)},
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
