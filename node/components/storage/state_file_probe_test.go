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

package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// buildDomainPair writes a valid .kv+bt+kvei triple under dir/domain/
// and returns their basenames. Used to seed the probe tests with
// realistic on-disk shapes.
func buildDomainPair(t *testing.T, dir, kvBase string) (kvName, btName string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "domain"), 0o755))

	// generateKV lives in the btindex test package — inline the same
	// pattern using a plain compressor for a small fixture.
	dataPath := filepath.Join(dir, "domain", kvBase+".kv")
	compressor, err := seg.NewCompressor(t.Context(), "test", dataPath, t.TempDir(), seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	for i := 0; i < 64; i++ {
		key := []byte{byte(i)}
		val := []byte{byte(i), byte(i)}
		require.NoError(t, compressor.AddWord(key))
		require.NoError(t, compressor.AddWord(val))
	}
	require.NoError(t, compressor.Compress())
	compressor.Close()

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(t, err)
	r := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
	btPath := filepath.Join(dir, "domain", kvBase+".bt")
	kveiPath := filepath.Join(dir, "domain", kvBase+".kvei")
	require.NoError(t, btindex.BuildBtreeIndexWithDecompressor(btPath, kveiPath, r, background.NewProgressSet(), t.TempDir(), 1, log.New(), true, statecfg.AccessorBTree|statecfg.AccessorExistence))
	decomp.Close()

	return kvBase + ".kv", kvBase + ".bt"
}

// TestSafeProbeDomainFileAgainstBt_HealthyPair pins the pass-through
// case: a valid .kv+.bt pair returns nil from the probe.
func TestSafeProbeDomainFileAgainstBt_HealthyPair(t *testing.T) {
	dir := t.TempDir()
	buildDomainPair(t, dir, "v3.0-accounts.0-1")
	kvPath := filepath.Join(dir, "domain", "v3.0-accounts.0-1.kv")
	btPath := filepath.Join(dir, "domain", "v3.0-accounts.0-1.bt")

	require.NoError(t, safeProbeDomainFileAgainstBt(kvPath, btPath))
}

// TestSafeProbeDomainFileAgainstBt_KvTruncated pins the corruption-
// detect case: truncate the .kv below u, expect
// ErrDomainFileSegInvalid so the caller quarantines the pair.
func TestSafeProbeDomainFileAgainstBt_KvTruncated(t *testing.T) {
	dir := t.TempDir()
	buildDomainPair(t, dir, "v3.0-storage.0-1")
	kvPath := filepath.Join(dir, "domain", "v3.0-storage.0-1.kv")
	btPath := filepath.Join(dir, "domain", "v3.0-storage.0-1.bt")

	u, err := btindex.ReadKvDataSizeFromBt(btPath)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(kvPath, int64(u)/2))

	err = safeProbeDomainFileAgainstBt(kvPath, btPath)
	require.Error(t, err)
	require.True(t, errors.Is(err, integrity.ErrDomainFileSegInvalid), "expected ErrDomainFileSegInvalid, got %v", err)
}
