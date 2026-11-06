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
	"errors"
	"os"
	"path/filepath"
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

func TestOpenFilesCountsADeletedFileUntilClose(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "f.seg")
	require.NoError(t, os.WriteFile(p, []byte{1}, 0o600))
	open0, _, err := OpenFiles(d)
	if errors.Is(err, errors.ErrUnsupported) {
		return // no /proc/self/fd on this platform
	}
	require.NoError(t, err)

	f, err := os.Open(p)
	require.NoError(t, err)
	open1, deleted, err := OpenFiles(d)
	require.NoError(t, err)
	require.Equal(t, open0+1, open1)
	require.Zero(t, deleted)

	require.NoError(t, os.Remove(p))
	_, deleted, err = OpenFiles(d)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	_, deleted, err = OpenFiles(filepath.Join(d, "other"))
	require.NoError(t, err)
	require.Zero(t, deleted, "files outside dir are not counted")

	require.NoError(t, f.Close())
	open2, deleted, err := OpenFiles(d)
	require.NoError(t, err)
	require.Equal(t, open0, open2)
	require.Zero(t, deleted)
}
