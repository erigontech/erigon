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

package snapshotsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

func TestDumpCaplinStateUsesRegisteredTypeFilename(t *testing.T) {
	name, err := caplinStateFileName(kv.PendingDepositsDump, 0, 50_000)
	require.NoError(t, err)
	require.Equal(t, "v1.1-000000-000050-PendingDepositsDump.seg", name)

	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapCaplin, 0o755))
	require.NoError(t, os.MkdirAll(dirs.Tmp, 0o755))
	require.NoError(t, dumpCaplinState(context.Background(), kv.PendingDepositsDump, rootGetter(nil), 0, 50_000, 50_000, 0, dirs, 1, log.LvlDebug, log.New(), true))

	entries, err := filepath.Glob(filepath.Join(dirs.SnapCaplin, "*.seg"))
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(dirs.SnapCaplin, name)}, entries)
}
