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

//go:build linux

package diskutils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMountPointForDirPathResolvesRelativeSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "node1")
	require.NoError(t, os.MkdirAll(target, 0o755))

	link := filepath.Join(root, "current")
	require.NoError(t, os.Symlink("node1", link))

	require.Equal(t, MountPointForDirPath(target), MountPointForDirPath(link))
	require.NotEmpty(t, MountPointForDirPath(link))
}
