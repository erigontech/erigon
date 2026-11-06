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

	"github.com/erigontech/erigon/db/snaptype"
)

func writeSnapFile(t *testing.T, d *Downloader, name string) string {
	t.Helper()
	path := d.filePathForName(snapshotName(name))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
	return path
}

// An accessor's offsets only mean anything against the bytes it was built from.
// Discarding a primary for re-download while its accessors stay leaves them
// indexing a generation that is gone, and the replacement is then read through
// them. Their own version prefix is independent of the primary's, so the set is
// matched on coordinate and extension rather than named.
func TestInvalidateDataDiscardsThePrimarysAccessors(t *testing.T) {
	require := require.New(t)
	d, _, name, path := newLocalSnapshotTest(t)
	accessor := writeSnapFile(t, d, "domain/v1.2-accounts.0-1024.kvi")
	bt := writeSnapFile(t, d, "domain/v2.0-accounts.0-1024.bt")

	_, download := prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), name)
	require.True(download)

	require.NoFileExists(path)
	require.NoFileExists(accessor, "the .kvi indexes bytes that are gone")
	require.NoFileExists(bt, "the .bt indexes bytes that are gone")
}

// A coordinate groups more than one set: a domain's .kv, its history .v and its
// index .ef all carry the same (type, from, to). Only the accessors built from
// the discarded primary's own bytes go with it — the others are independent
// primaries that nothing has invalidated.
func TestInvalidateDataKeepsIndependentPrimariesOfTheSameCoordinate(t *testing.T) {
	require := require.New(t)
	d, _, name, path := newLocalSnapshotTest(t)
	history := writeSnapFile(t, d, "history/v2.1-accounts.0-1024.v")
	idx := writeSnapFile(t, d, "idx/v3.1-accounts.0-1024.ef")
	historyAccessor := writeSnapFile(t, d, "accessor/v1.2-accounts.0-1024.vi")

	_, download := prepareLocalDataForDownload(t, d, snaptype.Hex2InfoHash("aa"), name)
	require.True(download)

	require.NoFileExists(path)
	require.FileExists(history, "the history .v is its own primary")
	require.FileExists(idx, "the index .ef is its own primary")
	require.FileExists(historyAccessor, "the .vi was built from the .v, not the .kv")
}
