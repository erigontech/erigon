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
}

func TestWitnessTrieCallsites(t *testing.T) {
	expected := map[witnessTrieSelector]int{
		{file: "eth_call.go", scope: "getWitness", symbol: "BuildTrieFromWitness"}:                  1,
		{file: "eth_call.go", scope: "assertProofVerifies", symbol: "VerifyAccountProof"}:           1,
		{file: "eth_call.go", scope: "assertProofVerifies", symbol: "VerifyStorageProof"}:           1,
		{file: "debug_execution_witness.go", scope: "witnessStateless", symbol: "Trie"}:             1,
		{file: "debug_execution_witness.go", scope: "newWitnessStateless", symbol: "RLPDecode"}:     1,
		{file: "commitment_context.go", scope: "WitnessNodes", symbol: "WitnessNodesForKeysByHash"}: 1,
	}
	files := []string{
		"eth_call.go",
		"debug_execution_witness.go",
		"witness_cache_builder.go",
		"../../execution/commitment/commitmentdb/commitment_context.go",
	}
	observed := make(map[witnessTrieSelector]int)
	for _, filename := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.SkipObjectResolution)
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
			if !ok || receiver.Name != importName {
				return true
			}
			scope, ok := witnessTrieScope(file, stack)
			if ok {
				observed[witnessTrieSelector{file: file, scope: scope, symbol: selector.Sel.Name}]++
			}
			return true
		})
	}
	require.Equal(t, expected, observed)
}

func witnessTrieScope(filename string, stack []ast.Node) (string, bool) {
	for _, declaration := range slices.Backward(stack) {
		switch declaration := declaration.(type) {
		case *ast.TypeSpec:
			if filename == "debug_execution_witness.go" && declaration.Name.Name == "witnessStateless" {
				return declaration.Name.Name, true
			}
		case *ast.FuncDecl:
			if witnessTrieFunction(filename, declaration) {
				return declaration.Name.Name, true
			}
		}
	}
	return "", false
}

func witnessTrieFunction(filename string, declaration *ast.FuncDecl) bool {
	switch filename {
	case "eth_call.go":
		return declaration.Name.Name == "getWitness" || declaration.Name.Name == "assertProofVerifies"
	case "debug_execution_witness.go":
		if declaration.Name.Name == "newWitnessStateless" {
			return true
		}
		return declaration.Recv != nil && len(declaration.Recv.List) == 1 && witnessStatelessType(declaration.Recv.List[0].Type)
	case "witness_cache_builder.go":
		return true
	case "commitment_context.go":
		return declaration.Name.Name == "WitnessNodes"
	default:
		return false
	}
}

func witnessStatelessType(expression ast.Expr) bool {
	if star, ok := expression.(*ast.StarExpr); ok {
		expression = star.X
	}
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "witnessStateless"
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
