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

	"github.com/erigontech/erigon/common/dir"
)

var update = flag.Bool("update", false, "rewrite the golden file")

// testdata/sample carries one field per branch, so this fails when any of them changes shape:
// omitempty per kind, a pointer written as null, an embedded struct flattened, a promoted name
// reached through its own selector, a json tag with no name, and the forms a field may declare.
func TestGenerateSample(t *testing.T) {
	golden, err := filepath.Abs("testdata/sample_golden.go.txt")
	require.NoError(t, err)
	out := filepath.Join(t.TempDir(), "gen_sample_json.go")

	t.Chdir("testdata/sample")
	require.NoError(t, run("Sample", out, "writeComputedJSON"))
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

// A field the generator cannot place must stop it, rather than produce a file that quietly
// omits the field or writes what encoding/json would not. A form that does not suit the field's
// type is not here: the ethjson writers are typed, so the generated file fails to compile.
func TestGenerateRejects(t *testing.T) {
	for name, typeName := range map[string]string{
		"no ethjson tag":    "MissingForm",
		"unknown form":      "UnknownForm",
		"duplicate name":    "DuplicateName",
		"tagged embedded":   "TaggedEmbedded",
		"embedded pointer":  "PointerEmbedded",
		"unknown option":    "UnknownOption",
		"omitempty objects": "OmitemptyObjects",
	} {
		t.Run(name, func(t *testing.T) {
			t.Chdir("testdata/bad")
			require.Error(t, run(typeName, filepath.Join(t.TempDir(), "out.go"), ""))
		})
	}
}

// The output it wrote last time is in the package it type-checks, so a field renamed since then
// must not be able to lock the generator out of rewriting it — nor may a sibling belonging to
// another type, since go generate runs one directive per type and the first failure stops the
// package before the run that would fix it.
func TestGenerateOverStaleOutput(t *testing.T) {
	// Inside the module, or the go tool has no go.mod to resolve the package against.
	const pkg = "testdata/stale"
	require.NoError(t, os.CopyFS(pkg, os.DirFS("testdata/sample")))
	t.Cleanup(func() { _ = dir.RemoveAll(pkg) })
	for name, recv := range map[string]string{"gen_sample_json.go": "Sample", "gen_left_json.go": "Left"} {
		stale := marker + " DO NOT EDIT.\n\npackage sample\n\n" +
			"import \"github.com/erigontech/erigon/rpc/jsonstream\"\n\nfunc (x *" + recv +
			") MarshalFastJSONTo(s *jsonstream.StackStream) error {\n\t_ = x.SinceRenamed\n\treturn nil\n}\n"
		require.NoError(t, os.WriteFile(filepath.Join(pkg, name), []byte(stale), 0o644))
	}

	t.Chdir(pkg)
	require.NoError(t, run("Sample", "gen_sample_json.go", "writeComputedJSON"))
	got, err := os.ReadFile("gen_sample_json.go")
	require.NoError(t, err)
	require.NotContains(t, string(got), "x.SinceRenamed")
}

// An error outside a generated file says the tags being read are not the ones that will build,
// so it must stop the run instead of being skipped along with the stale output.
func TestGenerateRefusesBrokenPackage(t *testing.T) {
	const pkg = "testdata/broken"
	require.NoError(t, os.CopyFS(pkg, os.DirFS("testdata/sample")))
	t.Cleanup(func() { _ = dir.RemoveAll(pkg) })
	// Two shapes: a name the old substring filter also rejected, and one it did not — a bare
	// mention of the method this run writes, which only the full-phrase match refuses.
	require.NoError(t, os.WriteFile(filepath.Join(pkg, "typo.go"),
		[]byte("package sample\n\nvar _ = undefinedHere\nvar _ = "+method+"\n"), 0o644))

	t.Chdir(pkg)
	require.ErrorContains(t, run("Sample", filepath.Join(t.TempDir(), "out.go"), "writeComputedJSON"), "undefinedHere")
}

// A Windows path carries a colon of its own, and CI runs there.
func TestPositionFile(t *testing.T) {
	for pos, want := range map[string]string{
		`C:\repo\gen_x.go:12:3`: `C:\repo\gen_x.go`,
		"/repo/gen_x.go:12:3":   "/repo/gen_x.go",
		"/repo/gen_x.go:12":     "/repo/gen_x.go",
		"gen_x.go":              "gen_x.go",
		"-":                     "-",
	} {
		require.Equal(t, want, positionFile(pos), pos)
	}
}
