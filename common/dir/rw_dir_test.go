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
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func TestWriteFileAtomic(t *testing.T) {
	dirPath := t.TempDir()
	path := filepath.Join(dirPath, "settings.json")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))

	require.NoError(t, WriteFileAtomic(path, []byte("new"), 0o644))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), data)
	info, err := os.Stat(path)
	require.NoError(t, err)
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
	}
	entries, err := os.ReadDir(dirPath)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Base(path), entries[0].Name())
}
