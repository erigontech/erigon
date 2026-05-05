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

package validation

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func TestAllFilesPresent_AcceptsExistingFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.kv"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.kvi"), []byte("y"), 0o644))

	files := []*snapshot.FileEntry{
		{Name: "a.kv", Local: true},
		{Name: "a.kvi", Local: true},
	}
	err := AllFilesPresent{SnapDir: dir}.ValidateStep(context.Background(), files)
	require.NoError(t, err)
}

func TestAllFilesPresent_RejectsMissingFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.kv"), []byte("x"), 0o644))
	// a.kvi missing.

	files := []*snapshot.FileEntry{
		{Name: "a.kv", Local: true},
		{Name: "a.kvi", Local: true},
	}
	err := AllFilesPresent{SnapDir: dir}.ValidateStep(context.Background(), files)
	require.Error(t, err)
	require.Contains(t, err.Error(), "a.kvi")
	require.Contains(t, err.Error(), "missing on disk")
}

func TestAllFilesPresent_SkipsNonLocalFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.kv"), []byte("x"), 0o644))

	files := []*snapshot.FileEntry{
		{Name: "a.kv", Local: true},
		{Name: "peer-only.kvi", Local: false}, // peer-advertised, not yet downloaded
	}
	err := AllFilesPresent{SnapDir: dir}.ValidateStep(context.Background(), files)
	require.NoError(t, err,
		"non-local files are skipped; they're not expected to be on disk yet")
}

func TestAllFilesPresent_RejectsEmptySnapDir(t *testing.T) {
	err := AllFilesPresent{SnapDir: ""}.ValidateStep(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty SnapDir")
}

type stubStepValidator struct {
	name string
	err  error
}

func (s stubStepValidator) Name() string { return s.name }
func (s stubStepValidator) ValidateStep(_ context.Context, _ []*snapshot.FileEntry) error {
	return s.err
}

func TestStepChain_AcceptsEmptyChain(t *testing.T) {
	require.NoError(t, StepChain{}.Validate(context.Background(), nil))
}

func TestStepChain_FailsFastOnFirstError(t *testing.T) {
	called := []string{}
	chain := StepChain{
		stubStepValidator{name: "first", err: nil},
		// Use a closure-backed validator to track ordering.
		recordingValidator{name: "second", record: &called, err: nil},
		stubStepValidator{name: "third", err: assertError("rejected by third")},
		recordingValidator{name: "fourth", record: &called},
	}
	err := chain.Validate(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "third")
	require.Contains(t, err.Error(), "rejected by third")
	require.Equal(t, []string{"second"}, called,
		"chain stops at the first failure; fourth is never invoked")
}

type recordingValidator struct {
	name   string
	record *[]string
	err    error
}

func (r recordingValidator) Name() string { return r.name }
func (r recordingValidator) ValidateStep(_ context.Context, _ []*snapshot.FileEntry) error {
	*r.record = append(*r.record, r.name)
	return r.err
}

type assertError string

func (a assertError) Error() string { return string(a) }
