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

//go:build linux || darwin

package diskutils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A relative symlink target is relative to the directory holding the symlink,
// not to the process working directory, so the returned path has to be absolute.
func TestSmlinkForDirPathResolvesRelativeTargetAgainstSymlinkDir(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "disks", "node1"), 0o755))

	link := filepath.Join(root, "current")
	require.NoError(t, os.Symlink(filepath.Join("disks", "node1"), link))

	// Run from an unrelated directory: the answer must not depend on it.
	t.Chdir(t.TempDir())

	require.Equal(t, filepath.Join(root, "disks", "node1"), SmlinkForDirPath(link))
}

func TestSmlinkForDirPathPreservesRelativeTarget(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "node1"), 0o755))

	link := filepath.Join(root, "sub", "current")
	require.NoError(t, os.MkdirAll(filepath.Dir(link), 0o755))
	require.NoError(t, os.Symlink(filepath.Join("..", "node1"), link))

	want := filepath.Join(root, "sub") + string(filepath.Separator) + ".." + string(filepath.Separator) + "node1"
	require.Equal(t, want, SmlinkForDirPath(link))
}

func TestSmlinkForDirPathPreservesIntermediateSymlinkOrder(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	outside := filepath.Join(base, "outside")
	require.NoError(t, os.MkdirAll(root, 0o755))
	require.NoError(t, os.MkdirAll(outside, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(base, "node1"), 0o755))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "alias")))

	link := filepath.Join(root, "current")
	rawTarget := "alias" + string(filepath.Separator) + ".." + string(filepath.Separator) + "node1"
	require.NoError(t, os.Symlink(rawTarget, link))

	want := filepath.Join(root, "alias") + string(filepath.Separator) + ".." + string(filepath.Separator) + "node1"
	got := SmlinkForDirPath(link)
	require.Equal(t, want, got)
	_, err := os.Stat(got)
	require.NoError(t, err)
}

func TestSmlinkForDirPathKeepsAbsoluteTarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "node1")
	require.NoError(t, os.MkdirAll(target, 0o755))

	link := filepath.Join(root, "current")
	require.NoError(t, os.Symlink(target, link))

	require.Equal(t, target, SmlinkForDirPath(link))
}

func TestSmlinkForDirPathReturnsNonSymlinkUnchanged(t *testing.T) {
	dir := t.TempDir()

	require.Equal(t, dir, SmlinkForDirPath(dir))
}

func TestSmlinkForDirPathReturnsMissingPathUnchanged(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")

	require.Equal(t, missing, SmlinkForDirPath(missing))
}
