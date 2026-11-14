// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the license, or
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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunBlockTestDecodesOnlySelectedCases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blocks.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"excluded": {"blocks": 42},
		"selected": {"network": "unsupported-test-fork"}
	}`), 0o600))
	filter, err := compileTestFilter("^selected$", nil)
	require.NoError(t, err)
	results, err := runBlockTestsParallel([]string{path}, 3, filter)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "selected", results[0].Name)
	require.False(t, results[0].Pass)
	require.Contains(t, results[0].Error, "unsupported-test-fork")

	filter, err = compileTestFilter("^excluded$", nil)
	require.NoError(t, err)
	_, err = runBlockTest(path, filter)
	require.Error(t, err)
}

func TestRunBlockTestsParallelKeepsFileAndCaseOrder(t *testing.T) {
	var files []string
	for _, name := range []string{"second", "first"} {
		path := filepath.Join(t.TempDir(), name+".json")
		require.NoError(t, os.WriteFile(path, []byte(`{
			"z": {"network": "unsupported-z"},
			"a": {"network": "unsupported-a"}
		}`), 0o600))
		files = append(files, path)
	}
	filter, err := compileTestFilter("", nil)
	require.NoError(t, err)
	serial, err := runBlockTestsParallel(files, 1, filter)
	require.NoError(t, err)
	require.Len(t, serial, 4)
	parallel, err := runBlockTestsParallel(files, 3, filter)
	require.NoError(t, err)
	require.Equal(t, serial, parallel)

	path := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"bad": {"blocks": 42}}`), 0o600))
	_, err = runBlockTestsParallel(append(files, path), 3, filter)
	require.Error(t, err)
	_, err = runBlockTestsParallel([]string{path + ".missing"}, 3, filter)
	require.ErrorIs(t, err, os.ErrNotExist)
}
