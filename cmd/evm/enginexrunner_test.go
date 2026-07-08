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
	"os"
	"path/filepath"
	"testing"
)

func TestDirSizeBytes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), make([]byte, 1000), 0o600); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b"), make([]byte, 2000), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := dirSizeBytes(root)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3000 {
		t.Fatalf("dirSizeBytes = %d, want 3000", got)
	}

	// A missing root reports 0, not an error, so the size check is a no-op
	// before any tester has been created.
	got, err = dirSizeBytes(filepath.Join(root, "does-not-exist"))
	if err != nil {
		t.Fatalf("missing dir should not error, got %v", err)
	}
	if got != 0 {
		t.Fatalf("dirSizeBytes(missing) = %d, want 0", got)
	}
}

func TestResetTesterIfOversizedDisabled(t *testing.T) {
	// maxBytes <= 0 disables the check and must not touch the runner (nil here).
	reset, err := resetTesterIfOversized(nil, engineXGroupKey{}, 0)
	if err != nil || reset {
		t.Fatalf("disabled check: got (reset=%v, err=%v), want (false, nil)", reset, err)
	}
}
