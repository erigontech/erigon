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

package downloader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTrimPostCutSiblings_RemovesOnlyPostCut pins the fork-transition
// post-swap cleanup: sibling files (accessor/*.vi/.efi, history/*.v,
// idx/*.ef) whose step range exceeds cutStep+1 must be removed;
// pre-cut siblings must survive. Also confirms the .torrent sidecar
// gets dropped alongside its primary.
func TestTrimPostCutSiblings_RemovesOnlyPostCut(t *testing.T) {
	dir := t.TempDir()

	// Layout: cutStep=299 (so files with ToStep <= 300 keep, ToStep > 300 trim).
	files := map[string]bool{ // path relative to snap dir → expect kept
		"accessor/v1.1-code.288-296.vi":         true, // pre-cut
		"accessor/v1.1-code.288-296.vi.torrent": true,
		"accessor/v1.1-code.299-300.vi":         true,  // straddles the boundary but To==300==cutStep+1 → kept
		"accessor/v1.1-code.300-304.vi":         false, // post-cut → trim
		"accessor/v1.1-code.300-304.vi.torrent": false,
		"accessor/v2.1-storage.299-300.efi":     true,
		"accessor/v2.1-storage.300-304.efi":     false,
		"history/v2.0-storage.288-296.v":        true,
		"history/v2.0-storage.300-304.v":        false,
		"idx/v2.0-storage.288-296.ef":           true,
		"idx/v2.0-storage.300-304.ef":           false,
		"domain/v3.0-receipt.288-296.kv":        true, // domain dir untouched by this helper
	}
	for path := range files {
		full := filepath.Join(dir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, []byte("x"), 0o644))
	}

	removed, err := TrimPostCutSiblings(dir, 299)
	require.NoError(t, err)
	// 4 post-cut primaries (accessor: 2, history: 1, idx: 1); torrent sidecars aren't counted.
	require.Equal(t, 4, removed)

	for path, expectKept := range files {
		_, err := os.Stat(filepath.Join(dir, path))
		if expectKept {
			require.NoError(t, err, "expected %s to survive", path)
		} else {
			require.True(t, os.IsNotExist(err), "expected %s to be removed, got err=%v", path, err)
		}
	}
}

// TestTrimPostCutSiblings_NoOpOnMissingSnapDir covers the defensive
// early return — validators that fire without a snap dir wired must
// not fail.
func TestTrimPostCutSiblings_NoOpOnMissingSnapDir(t *testing.T) {
	removed, err := TrimPostCutSiblings("", 299)
	require.NoError(t, err)
	require.Zero(t, removed)

	removed, err = TrimPostCutSiblings(filepath.Join(t.TempDir(), "does-not-exist"), 299)
	require.NoError(t, err)
	require.Zero(t, removed)
}
