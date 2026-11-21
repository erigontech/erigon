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

//go:build !windows

package backup

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type ownedFileInfo struct {
	os.FileInfo
	owner *syscall.Stat_t
}

func (f ownedFileInfo) Sys() any { return f.owner }

func TestRestoreOwnerDoesNotFollowSymlinks(t *testing.T) {
	t.Parallel()
	link := filepath.Join(t.TempDir(), "dangling")
	require.NoError(t, os.Symlink("missing", link))
	src := ownedFileInfo{owner: &syscall.Stat_t{Uid: uint32(os.Getuid()) + 1, Gid: uint32(os.Getgid()) + 1}}

	if err := restoreOwner(src, link); err != nil {
		require.ErrorIs(t, err, os.ErrPermission, "only root may change the owner; following the link gives ErrNotExist")
	}
}
