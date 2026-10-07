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
	return inlineReturns(text(fset, body), o)
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

// untraced returns runTraced as run in a file of its own, with anyTrace set
// to false and fast in place of the switchHere comment.
func untraced(traced []byte, fast string) []byte {
	if !bytes.Contains(traced, []byte(switchHere)) {
		log.Fatal("interpreter.go: the fast-path switch comment is missing")
	}
	src := bytes.Replace(traced, []byte(switchHere), []byte(fast), 1)
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
	files := []struct {
		name string
		data []byte
	}{
		{"vm_run_gen.go", untraced(read("interpreter.go"), fastSwitch(read("instructions.go"), ops))},
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
