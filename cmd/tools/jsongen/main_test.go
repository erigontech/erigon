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

package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite the golden file")

// testdata/sample carries one field per branch, so this fails when any of them changes shape:
// omitempty per kind, a pointer written as null, an embedded struct flattened, a json tag with
// no name, and the forms a field may declare.
func TestGenerateSample(t *testing.T) {
	const golden = "testdata/sample_golden.go.txt"
	out := filepath.Join(t.TempDir(), "gen_sample_json.go")

	require.NoError(t, run("Sample", "testdata/sample", out, "writeComputedJSON"))
	got, err := os.ReadFile(out)
	require.NoError(t, err)

	if *update {
		require.NoError(t, os.WriteFile(golden, got, 0o644))
		return
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

// A field the generator cannot place must stop it, rather than produce a file that does not
// compile or quietly omits the field.
func TestGenerateRejects(t *testing.T) {
	for name, typeName := range map[string]string{
		"no ethjson tag":    "MissingForm",
		"form vs type":      "WrongForm",
		"duplicate name":    "DuplicateName",
		"tagged embedded":   "TaggedEmbedded",
		"embedded pointer":  "PointerEmbedded",
		"pointer to slice":  "PointerToSlice",
		"narrow quantity":   "NarrowQuantity",
		"not a hash slice":  "NotHashSlice",
		"unknown option":    "UnknownOption",
		"omitempty objects": "OmitemptyObjects",
		"non-error result":  "WrongResult",
	} {
		t.Run(name, func(t *testing.T) {
			err := run(typeName, "testdata/bad", filepath.Join(t.TempDir(), "out.go"), "")
			require.Error(t, err)
		})
	}
}

// The output it wrote last time is in the package it type-checks, so a field renamed since
// then must not be able to lock the generator out of rewriting it.
func TestGenerateOverStaleOutput(t *testing.T) {
	// Inside the module, or the go tool has no go.mod to resolve the package against.
	dir := "testdata/stale"
	require.NoError(t, os.CopyFS(dir, os.DirFS("testdata/sample")))
	t.Cleanup(func() { os.RemoveAll(dir) })
	// The sibling matters as much as the output: go generate runs one directive per type, and
	// the first run must not fail on a file a later run would rewrite.
	for name, recv := range map[string]string{"gen_sample_json.go": "Sample", "gen_inner_json.go": "Inner"} {
		stale := marker + " DO NOT EDIT.\n\npackage sample\n\nfunc (x *" + recv +
			") MarshalFastJSONTo(s *jsonstream.StackStream) error {\n\t_ = x.SinceRenamed\n\treturn nil\n}\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(stale), 0o644))
	}

	require.NoError(t, run("Sample", dir, "gen_sample_json.go", "writeComputedJSON"))
	got, err := os.ReadFile(filepath.Join(dir, "gen_sample_json.go"))
	require.NoError(t, err)
	require.NotContains(t, string(got), "x.SinceRenamed")
}
