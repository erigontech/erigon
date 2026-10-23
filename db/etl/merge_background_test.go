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

package etl

import (
	"bytes"
	"errors"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type mergeTestEntry struct {
	key   []byte
	value []byte
}

func TestMergeInBackgroundMatchesSerial(t *testing.T) {
	rows := makeMergeTestEntries(12000)
	serial := runMergeLoad(t, rows, false)
	background := runMergeLoad(t, rows, true)
	require.Equal(t, serial, background)
}

func TestMergeInBackgroundEmptyAndSingleProvider(t *testing.T) {
	empty := NewCollector(t.Name()+"-empty", t.TempDir(), NewSortableBuffer(1), log.New()).MergeInBackground(true)
	defer empty.Close()
	var emptyRows []mergeTestEntry
	require.NoError(t, empty.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		emptyRows = append(emptyRows, mergeTestEntry{bytes.Clone(k), bytes.Clone(v)})
		return nil
	}, TransformArgs{}))
	require.Empty(t, emptyRows)

	single := NewCollector(t.Name()+"-single", t.TempDir(), NewSortableBuffer(1), log.New()).MergeInBackground(true)
	defer single.Close()
	require.NoError(t, single.Collect([]byte{2}, []byte{20}))
	require.NoError(t, single.Collect([]byte{1}, []byte{10}))
	var singleRows []mergeTestEntry
	require.NoError(t, single.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		singleRows = append(singleRows, mergeTestEntry{bytes.Clone(k), bytes.Clone(v)})
		return nil
	}, TransformArgs{}))
	require.Equal(t, []mergeTestEntry{{key: []byte{1}, value: []byte{10}}, {key: []byte{2}, value: []byte{20}}}, singleRows)
}

func TestMergeInBackgroundLoadError(t *testing.T) {
	collector := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(1), log.New()).MergeInBackground(true)
	defer collector.Close()
	for i := range 5000 {
		require.NoError(t, collector.Collect([]byte{byte(i >> 8), byte(i)}, []byte{byte(i)}))
	}
	want := errors.New("load failed")
	before := runtime.NumGoroutine()
	err := collector.Load(nil, "", func(_, _ []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		return want
	}, TransformArgs{})
	require.ErrorIs(t, err, want)
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+1 }, 2*time.Second, 10*time.Millisecond)
}

func TestMergeInBackgroundCancellation(t *testing.T) {
	collector := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(1), log.New()).MergeInBackground(true)
	defer collector.Close()
	for i := range 12000 {
		require.NoError(t, collector.Collect([]byte{byte(i >> 8), byte(i)}, []byte{byte(i)}))
	}
	quit := make(chan struct{})
	loads := 0
	before := runtime.NumGoroutine()
	err := collector.Load(nil, "", func(_, _ []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		loads++
		if loads == 1 {
			close(quit)
		}
		return nil
	}, TransformArgs{Quit: quit})
	require.ErrorIs(t, err, common.ErrStopped)
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+1 }, 2*time.Second, 10*time.Millisecond)
}

func makeMergeTestEntries(count int) []mergeTestEntry {
	rng := rand.New(rand.NewSource(42))
	rows := make([]mergeTestEntry, count)
	for i := range rows {
		rows[i] = mergeTestEntry{
			key:   []byte{byte(rng.Intn(64)), byte(rng.Intn(16))},
			value: []byte{byte(i), byte(rng.Intn(256))},
		}
	}
	return rows
}

func runMergeLoad(t *testing.T, rows []mergeTestEntry, background bool) []mergeTestEntry {
	t.Helper()
	onDisk := NewCollector(t.Name()+"-disk", t.TempDir(), NewSortableBuffer(1), log.New())
	inRAM := NewCollector(t.Name()+"-ram", t.TempDir(), NewSortableBuffer(BufferOptimalSize), log.New())
	defer onDisk.Close()
	defer inRAM.Close()
	if background {
		onDisk.MergeInBackground(true)
		inRAM.MergeInBackground(true)
	}
	for i, row := range rows {
		collector := inRAM
		if i%3 != 0 {
			collector = onDisk
		}
		require.NoError(t, collector.Collect(row.key, row.value))
	}
	var got []mergeTestEntry
	require.NoError(t, MergeLoad(t.Name(), []*Collector{onDisk, inRAM}, func(k, v []byte) error {
		got = append(got, mergeTestEntry{bytes.Clone(k), bytes.Clone(v)})
		return nil
	}, TransformArgs{}))
	return got
}
