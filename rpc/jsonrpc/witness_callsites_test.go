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

package jsonrpc

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const witnessTrieImport = "github.com/erigontech/erigon/execution/commitment/trie"

type witnessTrieSelector struct {
	file   string
	scope  string
	symbol string
	assert bool
}

func TestWitnessTrieCallsites(t *testing.T) {
	expected := map[witnessTrieSelector]int{
		{file: "eth_call.go", scope: "getWitness", symbol: "BuildTrieFromWitness", assert: true}:        1,
		{file: "eth_call.go", scope: "assertProofVerifies", symbol: "VerifyAccountProof", assert: true}: 1,
		{file: "eth_call.go", scope: "assertProofVerifies", symbol: "VerifyStorageProof", assert: true}: 1,
		{file: "debug_execution_witness.go", scope: "witnessStateless", symbol: "Trie"}:                 1,
		{file: "debug_execution_witness.go", scope: "newWitnessStateless", symbol: "RLPDecode"}:         1,
		{file: "commitment_context.go", scope: "WitnessNodes", symbol: "WitnessNodesForKeysByHash"}:     1,
	}
	files := []string{
		"eth_call.go",
		"debug_execution_witness.go",
		"witness_cache_builder.go",
		"../../execution/commitment/commitmentdb/commitment_context.go",
	}
	observed := make(map[witnessTrieSelector]int)
	for _, filename := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		importName, ok := witnessTrieImportName(parsed)
		if !ok {
			continue
		}
		file := path.Base(filename)
		ast.PreorderStack(parsed, nil, func(node ast.Node, stack []ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver, ok := selector.X.(*ast.Ident)
			if !ok || receiver.Name != importName || receiver.Obj != nil {
				return true
			}
			observed[witnessTrieSelector{file: file, scope: witnessTrieScope(stack), symbol: selector.Sel.Name, assert: witnessTrieUnderAssert(stack)}]++
			return true
		})
	}
	require.Equal(t, expected, observed)
}

func witnessTrieScope(stack []ast.Node) string {
	for _, declaration := range slices.Backward(stack) {
		switch declaration := declaration.(type) {
		case *ast.TypeSpec:
			return declaration.Name.Name
		case *ast.FuncDecl:
			return declaration.Name.Name
		}
	}
	return ""
}

func witnessTrieUnderAssert(stack []ast.Node) bool {
	for i, node := range stack {
		switch node := node.(type) {
		case *ast.IfStmt:
			if witnessAssertEnabled(node.Cond) && i+1 < len(stack) && stack[i+1] == node.Body {
				return true
			}
		case *ast.FuncDecl:
			if node.Body == nil || len(node.Body.List) == 0 {
				continue
			}
			guard, ok := node.Body.List[0].(*ast.IfStmt)
			if !ok {
				continue
			}
			if negated, ok := guard.Cond.(*ast.UnaryExpr); ok && negated.Op == token.NOT && witnessAssertEnabled(negated.X) {
				return true
			}
		}
	}
	return false
}

func witnessAssertEnabled(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "dbg" && selector.Sel.Name == "AssertEnabled"
}

func witnessTrieImportName(file *ast.File) (string, bool) {
	for _, declaration := range file.Imports {
		importPath, err := strconv.Unquote(declaration.Path.Value)
		if err != nil || importPath != witnessTrieImport {
			continue
		}
		if declaration.Name != nil {
			return declaration.Name.Name, true
		}
		return path.Base(importPath), true
	}
	return "", false
}
