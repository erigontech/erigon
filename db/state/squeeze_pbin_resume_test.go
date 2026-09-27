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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
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
}

func (c *pbinRebuildContextStub) Branch(key []byte) ([]byte, kv.Step, error) {
	c.reads++
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *pbinRebuildContextStub) PutBranch(key, data, _ []byte) error {
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
