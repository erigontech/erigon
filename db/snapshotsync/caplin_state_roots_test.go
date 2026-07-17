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

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

func rootGetter(emptyAt map[uint64]bool) KeyValueGetter {
	return func(id uint64) ([]byte, []byte, error) {
		if emptyAt[id] {
			return nil, nil, nil
		}
		return nil, make([]byte, length.Hash), nil
	}
}

func segFileCount(t *testing.T, dir string) int {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	require.NoError(t, err)
	return len(m)
}

// A missing block/state-root entry in the range must abort the dump and freeze
// nothing (an empty word would permanently shadow the DB), while genuinely
// sparse tables keep dumping empties.
func TestDumpCaplinStateRefusesEmptyMandatoryRoots(t *testing.T) {

	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapCaplin, 0o755))
	require.NoError(t, os.MkdirAll(dirs.Tmp, 0o755))
	const from, to, per = uint64(0), uint64(8), uint64(8)

	err := dumpCaplinState(ctx, kv.BlockRoot, rootGetter(map[uint64]bool{5: true}), from, to, per, 0, dirs, 1, log.LvlDebug, logger, true)
	require.ErrorIs(t, err, errIncompleteStateRange)
	require.Equal(t, 0, segFileCount(t, dirs.SnapCaplin))

	require.NoError(t, dumpCaplinState(ctx, kv.RandaoMixes, rootGetter(map[uint64]bool{5: true}), from, to, per, 0, dirs, 1, log.LvlDebug, logger, true))
	require.Equal(t, 1, segFileCount(t, dirs.SnapCaplin))
}

// A dense table returning a non-empty root of the wrong length is corruption,
// not an incomplete range: it must surface as a hard error (never
// errIncompleteStateRange, which would silently retry forever) and freeze nothing.
func TestDumpCaplinStateRejectsCorruptMandatoryRoot(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapCaplin, 0o755))
	require.NoError(t, os.MkdirAll(dirs.Tmp, 0o755))
	const from, to, per = uint64(0), uint64(8), uint64(8)

	corrupt := func(id uint64) ([]byte, []byte, error) {
		if id == 5 {
			return nil, make([]byte, 5), nil
		}
		return nil, make([]byte, length.Hash), nil
	}
	err := dumpCaplinState(ctx, kv.BlockRoot, corrupt, from, to, per, 0, dirs, 1, log.LvlDebug, logger, true)
	require.Error(t, err)
	require.NotErrorIs(t, err, errIncompleteStateRange)
	require.Contains(t, err.Error(), "corrupt")
	require.Equal(t, 0, segFileCount(t, dirs.SnapCaplin))
}
