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

//go:build unix

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
)

// Fixtures from a pipe are read once: a pipe cannot be read again or at an offset.
func TestLoadEngineXGroupsReadsAPipeOnce(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "fixtures.json")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))
	go func() {
		_ = os.WriteFile(fifo, []byte(`{"one": {"network":"Prague","preHash":"0x01","lastblockhash":"0x000000000000000000000000000000000000000000000000000000000000000a"}}`), 0o600)
	}()
	groups, total, err := loadEngineXGroups(fifo, regexp.MustCompile(""))
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.NoError(t, dir.RemoveFile(fifo))
	for _, tests := range groups {
		def, err := tests[0].definition()
		require.NoError(t, err)
		require.Equal(t, common.HexToHash("0x0a"), *def.LastBlockHash)
	}
}

// A piped input is a file, not a directory: collectFiles returns it as given. On Linux its
// /proc/self/fd link points at "pipe:[N]", which filepath.EvalSymlinks can not resolve.
func TestCollectFilesKeepsAPipe(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()
	path := fmt.Sprintf("/dev/fd/%d", r.Fd())
	if _, err := os.Stat("/proc/self/fd"); err == nil {
		path = fmt.Sprintf("/proc/self/fd/%d", r.Fd())
	}
	files, err := collectFiles(path)
	require.NoError(t, err)
	require.Equal(t, []string{path}, files)
}
