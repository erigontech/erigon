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

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
)

func TestPBTKVIProgressCountsKeysOnceAfterCollision(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "input.kv")
	c, err := seg.NewCompressor(t.Context(), t.Name(), path, tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	const count = 8
	for i := range count {
		require.NoError(t, c.AddUncompressedWord([]byte{byte(i)}))
		require.NoError(t, c.AddUncompressedWord([]byte{byte(i + 1)}))
	}
	require.NoError(t, c.Compress())
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	p := newPbtRangeProgress(tmp, "range 0-1").batch(".kvi", count)
	cfg := recsplit.RecSplitArgs{KeyCount: count, BucketSize: recsplit.DefaultBucketSize, LeafSize: recsplit.DefaultLeafSize, TmpDir: tmp, IndexFile: filepath.Join(tmp, "input.kvi"), Workers: 2, NoFsync: true}
	require.NoError(t, buildHashMapAccessor(t.Context(), d, seg.CompressNone, cfg.IndexFile, false, cfg, background.NewProgressSet(), log.New(), p, func(rs *recsplit.RecSplit) { rs.ForceCollisionOnce() }))
	p.Done()
	require.Equal(t, p.progress.total, p.progress.done.Load(), "successful .kvi build must finish at done == total after a collision retry; done=%d total=%d", p.progress.done.Load(), p.progress.total)
}
