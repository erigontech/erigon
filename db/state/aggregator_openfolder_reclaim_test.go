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

package state_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
)

// A reader that pinned the visible-file generation before Aggregator.OpenFolder
// runs must keep reading the files it still references, even if OpenFolder finds
// some of them removed from disk. The buggy path closed those files in place
// (nil-ing FilesItem.decompressor), crashing the reader that still held them.
func TestAggregatorOpenFolderKeepsReaderFilesAlive(t *testing.T) {
	_, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: 16})

	kvPath, ok := newestDomainFile(t, agg.Dirs().SnapDomain, "accounts")
	require.True(t, ok, "expected at least one accounts .kv file")

	// Reader pins the current generation, which still references kvPath.
	at := agg.BeginFilesRo()
	defer at.Close()

	// An external actor removes the newest file from disk (e.g. after an unwind
	// or merge cleanup), then the folder is reopened while `at` is still live.
	require.NoError(t, dir.RemoveFile(kvPath))
	require.NoError(t, agg.OpenFolder())

	require.NotPanics(t, func() {
		_, _, _, _, err := at.DebugGetLatestFromFiles(kv.AccountsDomain, make([]byte, 20), 0)
		require.NoError(t, err)
	})
}

// newestDomainFile returns the path of the domain .kv file with the greatest end
// step (the visible tip) for the given domain, matching `<ver>-<name>.<from>-<to>.kv`.
func newestDomainFile(t *testing.T, dir, name string) (string, bool) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	bestEnd, bestPath := -1, ""
	for _, e := range entries {
		fn := e.Name()
		if !strings.HasSuffix(fn, ".kv") || !strings.Contains(fn, "-"+name+".") {
			continue
		}
		body := strings.TrimSuffix(fn, ".kv")
		rng := body[strings.LastIndex(body, ".")+1:]
		_, toStep, ok := strings.Cut(rng, "-")
		if !ok {
			continue
		}
		end, err := strconv.Atoi(toStep)
		if err != nil {
			continue
		}
		if end > bestEnd {
			bestEnd, bestPath = end, filepath.Join(dir, fn)
		}
	}
	return bestPath, bestPath != ""
}
