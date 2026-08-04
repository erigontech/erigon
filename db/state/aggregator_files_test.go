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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

// TestMergeResult_FilePaths_HistoryOnlyMergeSurvives pins the
// invariant that a per-domain merge round producing history + idx but
// no new values file still appears in the FilePaths notification.
// Pre-fix, the loop keyed the history/idx append on `mf.d[id] != nil`,
// so history-only outputs were silently dropped and
// NotifyOnFilesChange panicked with "empty names" whenever the merge
// planner picked the history-only branch on every domain — the exact
// state a mode-C v4-overlap layout produces (values merge blocked by
// v4 straddling, history/idx unaffected).
func TestMergeResult_FilePaths_HistoryOnlyMergeSurvives(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	makeItem := func(name string, from, to uint64) *FilesItem {
		path := filepath.Join(tmp, name)
		comp, err := seg.NewCompressor(t.Context(), t.Name(), path, tmp, seg.DefaultCfg, log.LvlDebug, logger)
		require.NoError(t, err)
		comp.DisableFsync()
		require.NoError(t, comp.AddWord([]byte("k")))
		require.NoError(t, comp.Compress())
		comp.Close()
		dec, err := seg.NewDecompressor(path)
		require.NoError(t, err)
		t.Cleanup(func() { dec.Close() })
		return &FilesItem{startTxNum: from, endTxNum: to, decompressor: dec}
	}

	histItem := makeItem("v1.0-accounts.0-1.v", 0, 100)
	idxItem := makeItem("v1.0-accounts.0-1.ef", 0, 100)

	var mf MergeResult
	mf.dHist[kv.AccountsDomain] = histItem
	mf.dIdx[kv.AccountsDomain] = idxItem
	// mf.d[kv.AccountsDomain] left nil — this is the history-only shape.

	paths := mf.FilePaths(tmp)
	require.NotEmpty(t, paths,
		"history-only merge must contribute its history + idx paths; "+
			"pre-fix the outer `if d == nil { continue }` silently dropped them "+
			"and NotifyOnFilesChange panicked on empty names")
	require.Len(t, paths, 2, "expect one .v path + one .ef path")
	require.Contains(t, paths, "v1.0-accounts.0-1.v")
	require.Contains(t, paths, "v1.0-accounts.0-1.ef")
}
