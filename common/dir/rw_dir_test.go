// Copyright 2021 The Erigon Authors
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

package dir

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_CreateTemp(t *testing.T) {
	dir := t.TempDir()
	ogfile := filepath.Join(dir, "hello_world")
	tmpfile, err := CreateTemp(ogfile)
	if err != nil {
		t.Fatal(err)
	}
	defer tmpfile.Close()
	dir1 := filepath.Dir(tmpfile.Name())
	dir2 := filepath.Dir(ogfile)
	require.True(t, dir1 == dir2)

	base1 := filepath.Base(tmpfile.Name())
	base2 := filepath.Base(ogfile)
	require.True(t, strings.HasPrefix(base1, base2))
}

func Test_LogDirOnENOSPC(t *testing.T) {
	dir := t.TempDir()

	// create some temp files with known sizes
	for i := 0; i < 3; i++ {
		f, err := os.CreateTemp(dir, "erigon-sortable-buf-")
		require.NoError(t, err)
		_, err = f.Write(make([]byte, (i+1)*1024))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	// non-ENOSPC error: should not panic or log anything
	LogDirOnENOSPC(fmt.Errorf("some other error"), dir)

	// ENOSPC error: should log file listing, filesystem info, then panic
	enospc := fmt.Errorf("write failed: %w", syscall.ENOSPC)
	require.Panics(t, func() { LogDirOnENOSPC(enospc, dir) })

	// wrapped ENOSPC: should also panic
	wrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", syscall.ENOSPC))
	require.Panics(t, func() { LogDirOnENOSPC(wrapped, dir) })

	// bad directory: should log warning, not panic (returns early before panic)
	LogDirOnENOSPC(enospc, filepath.Join(dir, "nonexistent"))
}

func Test_CreateTempWithExt(t *testing.T) {
	dir := t.TempDir()
	ogfile := filepath.Join(dir, "hello_world")

	_, err := CreateTempWithExtension(ogfile, "existence")
	require.Error(t, err)

	tmpfile, err := CreateTempWithExtension(ogfile, "existence.tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpfile.Close()
	dir1 := filepath.Dir(tmpfile.Name())
	dir2 := filepath.Dir(ogfile)
	require.True(t, dir1 == dir2)

	base1 := filepath.Base(tmpfile.Name())
	base2 := filepath.Base(ogfile)
	require.True(t, strings.HasPrefix(base1, base2))
}
