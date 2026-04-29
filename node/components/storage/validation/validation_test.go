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
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// stubValidator is a test-only validator that returns whatever its
// configured err is, and records call count for chain-ordering tests.
type stubValidator struct {
	name  string
	err   error
	calls int
}

func (s *stubValidator) Name() string { return s.name }
func (s *stubValidator) Validate(*snapshot.FileEntry, ContentSource) error {
	s.calls++
	return s.err
}

func TestChain_EmptyChainAcceptsEverything(t *testing.T) {
	var chain Chain
	require.NoError(t, chain.Validate(&snapshot.FileEntry{Name: "x"}, nil))
}

func TestChain_RunsInOrderStopsOnFirstFailure(t *testing.T) {
	a := &stubValidator{name: "a"}
	b := &stubValidator{name: "b", err: errors.New("nope")}
	c := &stubValidator{name: "c"}
	chain := Chain{a, b, c}

	err := chain.Validate(&snapshot.FileEntry{Name: "x"}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "b: nope")
	require.Equal(t, 1, a.calls)
	require.Equal(t, 1, b.calls)
	require.Equal(t, 0, c.calls, "validators after first failure must not run")
}

func TestNameNotEmpty(t *testing.T) {
	v := NameNotEmpty{}
	require.NoError(t, v.Validate(&snapshot.FileEntry{Name: "x"}, nil))

	err := v.Validate(&snapshot.FileEntry{Name: ""}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty Name")

	require.Error(t, v.Validate(nil, nil))
}

func TestRangeOrdering(t *testing.T) {
	v := RangeOrdering{}

	// Step-less file (zero, zero) — accepted.
	require.NoError(t, v.Validate(&snapshot.FileEntry{Name: "salt"}, nil))

	// Valid half-open range.
	require.NoError(t, v.Validate(&snapshot.FileEntry{Name: "x", FromStep: 0, ToStep: 1024}, nil))
	require.NoError(t, v.Validate(&snapshot.FileEntry{Name: "x", FromStep: 1024, ToStep: 2048}, nil))

	// Inverted.
	err := v.Validate(&snapshot.FileEntry{Name: "x", FromStep: 2048, ToStep: 1024}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be strictly less than")

	// Empty non-zero range [N, N) is rejected — producer bug.
	err = v.Validate(&snapshot.FileEntry{Name: "x", FromStep: 1024, ToStep: 1024}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be strictly less than")
}

func TestDefaultStage1Chain(t *testing.T) {
	chain := DefaultStage1Chain()
	require.NotEmpty(t, chain)

	// Pass: a sane file.
	good := &snapshot.FileEntry{Name: "v1.0-accounts.0-1024.kv", FromStep: 0, ToStep: 1024}
	require.NoError(t, chain.Validate(good, nil))

	// Fail: empty name takes out NameNotEmpty.
	bad := &snapshot.FileEntry{Name: "", FromStep: 0, ToStep: 1024}
	err := chain.Validate(bad, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "name_not_empty")
}

func TestBytesContent_RoundTrip(t *testing.T) {
	src := BytesContent("hello-validation")
	rc, err := src.Open()
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "hello-validation", string(got))
}

func TestFileContent_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	require.NoError(t, os.WriteFile(path, []byte("on-disk"), 0o600))

	src := FileContent{Path: path}
	rc, err := src.Open()
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "on-disk", string(got))
}

func TestFileContent_EmptyPathIsAnError(t *testing.T) {
	_, err := FileContent{}.Open()
	require.Error(t, err)
}

// TestChain_ValidatorThatReadsContent confirms the chain plumbs the
// ContentSource through to validators that need it. Stage-2-style
// shape — a validator that reads the bytes is exactly how
// hash-match, well-formed-for-kind, etc. will be implemented.
func TestChain_ValidatorThatReadsContent(t *testing.T) {
	expectsBytes := byteAssertingValidator{want: []byte("payload")}
	chain := Chain{expectsBytes}
	require.NoError(t, chain.Validate(&snapshot.FileEntry{Name: "x"}, BytesContent("payload")))

	err := chain.Validate(&snapshot.FileEntry{Name: "x"}, BytesContent("wrong"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "byte_check: content mismatch")
}

type byteAssertingValidator struct{ want []byte }

func (byteAssertingValidator) Name() string { return "byte_check" }
func (b byteAssertingValidator) Validate(_ *snapshot.FileEntry, src ContentSource) error {
	if src == nil {
		return fmt.Errorf("validator needs content but got nil source")
	}
	rc, err := src.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, b.want) {
		return fmt.Errorf("content mismatch: got %q, want %q", got, b.want)
	}
	return nil
}
