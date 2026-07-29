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

// TestTrimPostCutSiblings_RemovesPostCutAndStraddlers pins the
// fork-transition post-swap cleanup: sibling files (accessor/*.vi/.efi,
// history/*.v, idx/*.ef) whose step range extends into or past cutStep
// must be removed — both entirely-post-cut files AND boundary
// straddlers. The fork-datadir validator (ValidateForkDatadir via
// classifyRange) rejects both, so both must go for a clean fork boot.
// Pre-cut siblings survive; .torrent sidecars are dropped alongside
// their primary.
func TestTrimPostCutSiblings_RemovesPostCutAndStraddlers(t *testing.T) {
	dir := t.TempDir()

	// Layout: cutStep=299 — the step containing the cut txNum. Any
	// file with To > 299 covers step 299 or later and contains
	// at-cut or post-cut data.
	files := map[string]bool{ // path relative to snap dir → expect kept
		"accessor/v1.1-code.288-296.vi":         true, // pre-cut (To=296 <= cutStep)
		"accessor/v1.1-code.288-296.vi.torrent": true,
		"accessor/v1.1-code.299-300.vi":         false, // straddles boundary step (To=300 > cutStep=299) → trim
		"accessor/v1.1-code.299-300.vi.torrent": false,
		"accessor/v1.1-code.300-304.vi":         false, // entirely post-cut → trim
		"accessor/v1.1-code.300-304.vi.torrent": false,
		"accessor/v2.1-storage.299-300.efi":     false, // straddler → trim
		"accessor/v2.1-storage.300-304.efi":     false,
		"history/v2.0-storage.288-296.v":        true,
		"history/v2.0-storage.299-300.v":        false, // straddler → trim
		"history/v2.0-storage.300-304.v":        false,
		"idx/v2.0-storage.288-296.ef":           true,
		"idx/v2.0-storage.299-300.ef":           false, // straddler → trim
		"idx/v2.0-storage.300-304.ef":           false,
		// domain/ hosts primary .kv (untouched — regen owns those)
		// alongside accessor siblings (.bt, .kvei, .kvi) that this
		// helper trims by step-range rule.
		"domain/v3.0-receipt.288-296.kv":       true,  // pre-cut primary → keep
		"domain/v3.0-receipt.299-300.kv":       true,  // straddling primary → keep (regen truncates)
		"domain/v3.0-receipt.300-304.kv":       true,  // post-cut primary → keep (regen's job)
		"domain/v1.1-accounts.288-296.bt":      true,  // pre-cut accessor → keep
		"domain/v1.1-accounts.299-300.bt":      false, // straddling accessor → trim
		"domain/v1.1-accounts.300-304.bt":      false, // post-cut accessor → trim
		"domain/v1.1-accounts.288-296.kvei":    true,
		"domain/v1.1-accounts.299-300.kvei":    false, // straddler .kvei → trim
		"domain/v1.1-accounts.300-304.kvei":    false, // post-cut .kvei → trim
	}
	for path := range files {
		full := filepath.Join(dir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
		require.NoError(t, os.WriteFile(full, []byte("x"), 0o644))
	}

	removed, err := TrimPostCutSiblings(dir, 299)
	require.NoError(t, err)
	// 12 primaries removed: accessor 4 + history 2 + idx 2 +
	// domain-side 4 (.bt x2 + .kvei x2). Torrent sidecars aren't
	// counted in the return.
	require.Equal(t, 12, removed)

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
