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
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

type pbinRebuildReaderStub struct {
	domain kv.Domain
	value  []byte
}

func (r pbinRebuildReaderStub) WithHistory() bool { return false }

func (r pbinRebuildReaderStub) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r pbinRebuildReaderStub) Read(domain kv.Domain, _ []byte, _ uint64) ([]byte, kv.Step, error) {
	if domain == r.domain {
		return r.value, 0, nil
	}
	return nil, 0, nil
}

func (r pbinRebuildReaderStub) Clone(kv.TemporalTx) commitmentdb.StateReader { return r }

func (r pbinRebuildReaderStub) CloneForWorker(context.Context, kv.TemporalTx) commitmentdb.StateReader {
	return r
}

func TestPBinRebuildStateReaderKeepsCommitmentOverlaySeparate(t *testing.T) {
	reader := newPBinRebuildStateReader(
		pbinRebuildReaderStub{domain: kv.CommitmentDomain, value: []byte{1}},
		pbinRebuildReaderStub{domain: kv.AccountsDomain, value: []byte{2}},
		kv.CommitmentDomain,
	)
	commitmentValue, _, err := reader.Read(kv.CommitmentDomain, nil, 1)
	require.NoError(t, err)
	plainValue, _, err := reader.Read(kv.AccountsDomain, nil, 1)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, commitmentValue)
	require.Equal(t, []byte{2}, plainValue)
}

type pbinRebuildContextStub struct {
	records map[string][]byte
	reads   int
	discard bool
}

func (c *pbinRebuildContextStub) Branch(key []byte) ([]byte, kv.Step, error) {
	c.reads++
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *pbinRebuildContextStub) PutBranch(key, data, _ []byte) error {
	if c.discard {
		return nil
	}
	c.records[string(key)] = bytes.Clone(data)
	return nil
}

func (c *pbinRebuildContextStub) Account([]byte) (*commitment.Update, error) { return nil, nil }

func (c *pbinRebuildContextStub) Storage([]byte) (*commitment.Update, error) { return nil, nil }

func TestPBinRebuildOverlayReadsPendingRowsBeforeFiles(t *testing.T) {
	inner := &pbinRebuildContextStub{records: make(map[string][]byte)}
	overlay := newPBinRebuildOverlay().withInner(inner)
	require.NoError(t, overlay.PutBranch([]byte("row"), []byte{1}, nil))
	got, _, err := overlay.Branch([]byte("row"))
	require.NoError(t, err)
	require.Equal(t, []byte{1}, got)
	require.Zero(t, inner.reads)
	_, _, err = overlay.Branch([]byte("other"))
	require.NoError(t, err)
	require.Equal(t, 1, inner.reads)
}

func TestPBinRebuildBatchesReadOnlyRightEdgeRows(t *testing.T) {
	inner := &pbinRebuildContextStub{records: map[string][]byte{
		"right-edge": {2},
	}}
	overlay := newPBinRebuildOverlay().withInner(inner)
	require.NoError(t, overlay.PutBranch([]byte("finished"), []byte{1}, nil))

	readsBefore := inner.reads
	finished, _, err := overlay.Branch([]byte("finished"))
	require.NoError(t, err)
	require.Equal(t, []byte{1}, finished)
	rightEdge, _, err := overlay.Branch([]byte("right-edge"))
	require.NoError(t, err)
	require.Equal(t, []byte{2}, rightEdge)
	require.Equal(t, 1, inner.reads-readsBefore)
}

func TestPBinRebuildCheckpointResumesAfterLargestTreeKey(t *testing.T) {
	overlay := newPBinRebuildOverlay()
	overlay.writes["row"] = pbinRebuildWrite{data: []byte{1}, prev: []byte{2}}
	path := t.TempDir() + "/checkpoint"
	lastKey := []byte{3, 4}
	require.NoError(t, writePBinRebuildCheckpoint(path, lastKey, overlay))
	checkpoint, err := readPBinRebuildCheckpoint(path)
	require.NoError(t, err)
	require.Equal(t, lastKey, checkpoint.LastKey)
	require.Equal(t, overlay.writes["row"].data, checkpoint.Writes["row"].Data)

	ops := []pbt.Op{{Key: []byte{3, 3}, Value: [32]byte{1}}, {Key: []byte{3, 5}, Value: [32]byte{2}}}
	var resumed []pbt.Op
	err = pbinForEachRebuildBatchAfter(ops, t.TempDir(), 2, 1024, checkpoint.LastKey, func(batch []pbt.Op, _ bool) error {
		resumed = append(resumed, batch...)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, resumed, 1)
	require.True(t, bytes.Equal(ops[1].Key, resumed[0].Key))
}

func TestPBinRebuildOverlayHeapStaysBoundedByRightEdge(t *testing.T) {
	const (
		maxOperations = 1000
		maxBytes      = 1 << 20
	)
	run := func(count int) uint64 {
		inner := &pbinRebuildContextStub{records: make(map[string][]byte), discard: true}
		overlay := newPBinRebuildOverlay().withInner(inner)
		runtime.GC()
		var baseline runtime.MemStats
		runtime.ReadMemStats(&baseline)
		var peak uint64
		maxKey := bytes.Repeat([]byte{0xff}, eip8297.StorageKeyLength)
		lastPath := eip8297.PathFromBits(maxKey[:33], 264)
		lastRow, err := pbt.EncodeRowKey(&lastPath)
		require.NoError(t, err)
		for i := 0; i < count; i++ {
			path := eip8297.PathFromBits([]byte{byte(i >> 16), byte(i >> 8), byte(i)}, 24)
			key, err := pbt.EncodeRowKey(&path)
			require.NoError(t, err)
			if i == count-1 {
				key = lastRow
			}
			overlay.writes[string(key)] = pbinRebuildWrite{data: []byte{1}}
			if i%maxOperations == maxOperations-1 || i == count-1 {
				require.NoError(t, overlay.FlushFinished(maxKey))
				runtime.GC()
				var current runtime.MemStats
				runtime.ReadMemStats(&current)
				if current.Alloc > peak {
					peak = current.Alloc
				}
			}
		}
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		require.LessOrEqual(t, len(overlay.writes), 1)
		ceiling := baseline.Alloc + maxBytes + maxOperations*256 + 32<<20
		require.Less(t, peak, ceiling)
		t.Logf("overlay rows: %d; peak live heap after GC: %d bytes; live heap after GC: %d bytes; ceiling: %d bytes", len(overlay.writes), peak, stats.Alloc, ceiling)
		return stats.Alloc
	}

	small := run(200_000)
	large := run(600_000)
	if large > small {
		require.Less(t, large-small, uint64(32<<20))
	} else {
		require.Less(t, small-large, uint64(32<<20))
	}
}
