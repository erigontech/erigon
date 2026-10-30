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
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/stretchr/testify/require"
)

func TestPBTProgressReportsRuntimeState(t *testing.T) {
	previousHandler := log.Root().GetHandler()
	var logs bytes.Buffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })
	progress := NewPBTProgress(PBTProgressConfig{Phase: "test", Total: 2, TempDir: t.TempDir(), Interval: time.Nanosecond})
	progress.Add(1)
	progress.Done()
	line := logs.String()
	require.Contains(t, line, "phase=test")
	require.Contains(t, line, "done=1")
	require.Contains(t, line, "total=2")
	require.Contains(t, line, "eta=")
	require.Contains(t, line, "rate=")
	require.Contains(t, line, "rss_anon=")
	require.Contains(t, line, "go_heap=")
	require.Contains(t, line, "temp_bytes=")
}

func TestPBTPartitionWriteProgressCountsEachOpOnce(t *testing.T) {
	previous := log.Root().GetHandler()
	previousInterval := PBTProgressInterval
	var logs bytes.Buffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	PBTProgressInterval = time.Nanosecond
	t.Cleanup(func() {
		log.Root().SetHandler(previous)
		PBTProgressInterval = previousInterval
	})
	const totalOps = 5000
	address := bytes.Repeat([]byte{1}, 20)
	ops := make([]pbt.Op, totalOps)
	for i := range ops {
		slot := make([]byte, 32)
		binary.BigEndian.PutUint64(slot[24:], uint64(i))
		var value [32]byte
		binary.BigEndian.PutUint64(value[24:], uint64(i))
		ops[i] = pbt.Op{Key: eip8297.TreeKeyStorage(address, slot), Value: value}
	}
	ops = pbinSortedOps(ops)
	batches, err := pbinRebuildBatches(ops, t.TempDir(), 1, 1<<20)
	require.NoError(t, err)
	var count int
	for _, batch := range batches {
		count += len(batch)
	}
	require.Equal(t, len(ops), count)
	require.Contains(t, logs.String(), `phase="pbin-ops file write" done=4096 `, "sampled progress must report the threshold")
	require.Contains(t, logs.String(), `phase="pbin-ops file write" done=5000 `, "all written operations must report done=5000")
}

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
