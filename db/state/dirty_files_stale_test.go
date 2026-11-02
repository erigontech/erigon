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

package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

func buildSegFile(t *testing.T, dir, name string, words []string) string {
	t.Helper()
	fPath := filepath.Join(dir, name)
	comp, err := seg.NewCompressor(t.Context(), t.Name(), fPath, dir, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	comp.DisableFsync()
	for _, w := range words {
		require.NoError(t, comp.AddWord([]byte(w)))
	}
	require.NoError(t, comp.Compress())
	comp.Close()
	return fPath
}

// isStaleOnDisk reports whether the FilesItem's decompressor is holding
// an mmap on a file that has been replaced on disk since it was opened.
// Same-name different-inode (retire cutover, downloader .part → final
// rename, in-place overwrite) is invisible to openDirtyFiles today: the
// item.decompressor != nil check skips reopening, so the old mmap keeps
// serving torn/zero pages until process restart. Detecting via
// size+mtime comparison lets openDirtyFiles close and reopen.
func TestFilesItem_IsStaleOnDisk(t *testing.T) {
	tmp := t.TempDir()
	fPath := buildSegFile(t, tmp, "v1-foo.0-1.kv", []string{"word1", "word2"})

	dec, err := seg.NewDecompressor(fPath)
	require.NoError(t, err)
	defer dec.Close()

	item := &FilesItem{startTxNum: 0, endTxNum: 10}
	item.decompressor = dec

	require.False(t, item.isStaleOnDisk(),
		"freshly-opened file must not be stale")

	// Replace the file at the same path with different content — the
	// exact pattern the downloader's .part → final rename produces
	// when it re-fetches a file we previously served, or that retire
	// produces when it rewrites a subsumed sub-chunk.
	otherPath := buildSegFile(t, tmp, "v1-foo.0-1-other.kv", []string{"word1", "word2", "word3"})
	// Force a different mtime so the check trips even on filesystems
	// with 1-second mtime resolution.
	future := time.Now().Add(2 * time.Second)
	require.NoError(t, os.Chtimes(otherPath, future, future))
	require.NoError(t, os.Rename(otherPath, fPath))

	require.True(t, item.isStaleOnDisk(),
		"file replaced at same path must be detected as stale (size or mtime changed)")
}

// A nil decompressor is never stale — the openDirtyFiles loop's
// existing branch opens it fresh, no reopen needed.
func TestFilesItem_IsStaleOnDisk_NilDecompressor(t *testing.T) {
	item := &FilesItem{startTxNum: 0, endTxNum: 10}
	require.False(t, item.isStaleOnDisk())
}

// A missing file is not stale — the caller's existing invalidFileItems
// path handles ENOENT via version.MatchVersionedFile. Returning true
// here would double-invalidate.
func TestFilesItem_IsStaleOnDisk_FileGone(t *testing.T) {
	tmp := t.TempDir()
	fPath := buildSegFile(t, tmp, "v1-foo.0-1.kv", []string{"word1"})

	dec, err := seg.NewDecompressor(fPath)
	require.NoError(t, err)
	defer dec.Close()

	item := &FilesItem{startTxNum: 0, endTxNum: 10}
	item.decompressor = dec

	require.NoError(t, dir.RemoveFile(fPath))
	require.False(t, item.isStaleOnDisk(),
		"missing file falls through to existing invalidFileItems path — not stale")
}
