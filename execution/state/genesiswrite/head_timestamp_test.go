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

package genesiswrite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// On a snapshot-synced datadir the head header is in the block files and not in
// kv.Headers, so reading only the database dates the head at time 0.
func TestHeadTimestampFromBlockFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	headers := []*types.Header{
		{Number: *uint256.NewInt(0), Time: 1000, Difficulty: *uint256.NewInt(1), Extra: []byte{}},
		{Number: *uint256.NewInt(1), Time: 2000, Difficulty: *uint256.NewInt(1), Extra: []byte{}},
	}
	writeHeaderSegment(t, dirs, headers)

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	head := headers[1]
	require.Nil(t, rawdb.ReadHeader(tx, head.Hash(), 1), "the database must not hold the head header")

	headTime, err := headTimestamp(tx, head.Hash(), 1, "", dirs, logger)
	require.NoError(t, err)
	require.Equal(t, head.Time, headTime)
}

// The snapshot lookup resolves by height, so a stale head marker would otherwise be dated
// with whatever block sits at that number.
func TestHeadTimestampRejectsHashMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	logger := log.New()
	dirs := datadir.New(t.TempDir())
	headers := []*types.Header{
		{Number: *uint256.NewInt(0), Time: 1000, Difficulty: *uint256.NewInt(1), Extra: []byte{}},
		{Number: *uint256.NewInt(1), Time: 2000, Difficulty: *uint256.NewInt(1), Extra: []byte{}},
	}
	writeHeaderSegment(t, dirs, headers)

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = headTimestamp(tx, common.Hash{9}, 1, "", dirs, logger)
	require.Error(t, err, "a header at the right height but the wrong hash must not date the head")
}

// A head header that neither source has leaves the active timestamp forks unknown.
func TestHeadTimestampUnresolvable(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = headTimestamp(tx, common.Hash{1}, 1, "", dirs, log.New())
	require.Error(t, err)
}

func writeHeaderSegment(t *testing.T, dirs datadir.Dirs, headers []*types.Header) {
	t.Helper()

	ctx := context.Background()
	logger := log.New()
	from, to := headers[0].Number.Uint64(), headers[len(headers)-1].Number.Uint64()+1
	name := snaptype.SegmentFileName(version.V1_0, from, to, snaptype2.Enums.Headers)

	c, err := seg.NewCompressor(ctx, "test", filepath.Join(dirs.Snap, name), dirs.Tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	for _, h := range headers {
		enc, err := rlp.EncodeToBytes(h)
		require.NoError(t, err)
		hash := h.Hash()
		require.NoError(t, c.AddWord(append([]byte{hash[0]}, enc...)))
	}
	require.NoError(t, c.Compress())
	c.Close()

	_, err = snaptype.LoadSalt(dirs.Snap, true, logger)
	require.NoError(t, err)

	info, _, ok := snaptype.ParseFileName(dirs.Snap, name)
	require.True(t, ok)
	require.NoError(t, snaptype2.Headers.BuildIndexes(ctx, info, nil, nil, dirs.Tmp, nil, log.LvlDebug, logger))
}
