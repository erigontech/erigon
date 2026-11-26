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
	"strconv"
	"testing"
)

const witnessTrieImport = "github.com/erigontech/erigon/execution/commitment/trie"

type witnessTrieSelector struct {
	file   string
	scope  string
	symbol string
}

var witnessTrieSelectorAllowlist = []witnessTrieSelector{
	{file: "eth_call.go", scope: "getWitness", symbol: "BuildTrieFromWitness"},
	{file: "eth_call.go", scope: "assertProofVerifies", symbol: "VerifyAccountProof"},
	{file: "eth_call.go", scope: "assertProofVerifies", symbol: "VerifyStorageProof"},
	{file: "debug_execution_witness.go", scope: "witnessStateless", symbol: "Trie"},
	{file: "debug_execution_witness.go", scope: "newWitnessStateless", symbol: "RLPDecode"},
	{file: "commitment_context.go", scope: "WitnessNodes", symbol: "WitnessNodesForKeysByHash"},
}

func TestWitnessTrieCallsites(t *testing.T) {
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
		collectWitnessTrieSelectors(parsed, path.Base(filename), importName, observed)
	}

	allowed := make(map[witnessTrieSelector]struct{}, len(witnessTrieSelectorAllowlist))
	for _, selector := range witnessTrieSelectorAllowlist {
		allowed[selector] = struct{}{}
	}
	for selector, count := range observed {
		if _, ok := allowed[selector]; !ok {
			t.Errorf("unexpected trie selector %s.%s in %s.%s", witnessTrieImport, selector.symbol, selector.file, selector.scope)
			continue
		}
		if count != 1 {
			t.Errorf("trie selector %s.%s in %s.%s found %d times, want once", witnessTrieImport, selector.symbol, selector.file, selector.scope, count)
		}
	}
	for _, selector := range witnessTrieSelectorAllowlist {
		if observed[selector] != 1 {
			t.Errorf("allowlisted trie selector %s.%s in %s.%s found %d times, want once", witnessTrieImport, selector.symbol, selector.file, selector.scope, observed[selector])
		}
	}
}

func collectWitnessTrieSelectors(file *ast.File, filename, importName string, observed map[witnessTrieSelector]int) {
	ast.Inspect(file, func(node ast.Node) bool {
		switch declaration := node.(type) {
		case *ast.FuncDecl:
			if witnessTrieFunction(filename, declaration) {
				ast.Walk(&witnessTrieSelectorVisitor{
					file:       filename,
					importName: importName,
					observed:   observed,
				}, declaration)
				return false
			}
		case *ast.TypeSpec:
			if filename == "debug_execution_witness.go" && declaration.Name.Name == "witnessStateless" {
				ast.Walk(&witnessTrieSelectorVisitor{
					file:       filename,
					importName: importName,
					observed:   observed,
				}, declaration)
				return false
			}
		}
		return true
	})
}

func witnessTrieFunction(filename string, declaration *ast.FuncDecl) bool {
	switch filename {
	case "eth_call.go":
		return declaration.Name.Name == "getWitness" || declaration.Name.Name == "assertProofVerifies"
	case "debug_execution_witness.go":
		if declaration.Name.Name == "newWitnessStateless" {
			return true
		}
		return declaration.Recv != nil && declaration.Recv.List != nil && len(declaration.Recv.List) == 1 && witnessStatelessType(declaration.Recv.List[0].Type)
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

type witnessTrieSelectorVisitor struct {
	file       string
	importName string
	scope      string
	observed   map[witnessTrieSelector]int
}

func (v *witnessTrieSelectorVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		return nil
	}
	scope := v.scope
	switch n := node.(type) {
	case *ast.FuncDecl:
		scope = n.Name.Name
	case *ast.FuncLit:
		scope = "<func literal>"
	case *ast.TypeSpec:
		scope = n.Name.Name
	}
	if selector, ok := node.(*ast.SelectorExpr); ok {
		if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == v.importName {
			key := witnessTrieSelector{file: v.file, scope: scope, symbol: selector.Sel.Name}
			v.observed[key]++
		}
	}
	return &witnessTrieSelectorVisitor{
		file:       v.file,
		importName: v.importName,
		scope:      scope,
		observed:   v.observed,
	}
}
