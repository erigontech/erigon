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
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/stretchr/testify/require"
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

func backgroundMergeCollector(t *testing.T, count int, disk, background bool) (*Collector, string) {
	t.Helper()
	dir := t.TempDir()
	collector := NewCollector(t.Name(), dir, NewSortableBuffer(BufferOptimalSize), log.New()).MergeInBackground(background)
	t.Cleanup(collector.Close)
	for i := range count {
		var row [8]byte
		binary.BigEndian.PutUint64(row[:], uint64(i))
		require.NoError(t, collector.Collect(row[:], row[:]))
	}
	if disk {
		require.NoError(t, collector.Flush())
	}
	return collector, dir
}

func backgroundMergeLoad(collector *Collector, merged bool, load simpleLoadFunc, args TransformArgs) error {
	if merged {
		return MergeLoad("review", []*Collector{collector}, load, args)
	}
	return collector.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		return load(k, v)
	}, args)
}

func TestBackgroundRetainedSlices(t *testing.T) {
	for _, merged := range []bool{false, true} {
		for _, disk := range []bool{false, true} {
			for _, background := range []bool{false, true} {
				name := "serial"
				if background {
					name = "background"
				}
				if disk {
					name += "/disk"
				} else {
					name += "/ram"
				}
				if merged {
					name += "/MergeLoad"
				} else {
					name += "/Load"
				}
				t.Run(name, func(t *testing.T) {
					collector, _ := backgroundMergeCollector(t, 6*mergeBatchEntries, disk, background)
					var firstKey, firstValue []byte
					err := backgroundMergeLoad(collector, merged, func(k, v []byte) error {
						if firstKey == nil {
							firstKey, firstValue = k, v
						}
						return nil
					}, TransformArgs{})
					require.NoError(t, err)
					gotKey, gotValue := binary.BigEndian.Uint64(firstKey), binary.BigEndian.Uint64(firstValue)
					if gotKey != 0 || gotValue != 0 {
						t.Errorf("retained row changed after six batches: key=%d value=%d; want key=0 value=0", gotKey, gotValue)
					}
				})
			}
		}
	}
}

type backgroundFailingProvider struct {
	dataProvider
	reads  int
	failAt int
	err    error
}

func (p *backgroundFailingProvider) Next() ([]byte, []byte, error) {
	p.reads++
	if p.reads == p.failAt {
		return nil, nil, p.err
	}
	return p.dataProvider.Next()
}

func TestBackgroundProviderErrorPrefix(t *testing.T) {
	readErr := errors.New("provider read failed")
	for _, background := range []bool{false, true} {
		name := "serial"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			collector, dir := backgroundMergeCollector(t, 20, true, background)
			collector.dataProviders[0] = &backgroundFailingProvider{dataProvider: collector.dataProviders[0], failAt: 11, err: readErr}
			loads := 0
			err := backgroundMergeLoad(collector, false, func(_, _ []byte) error { loads++; return nil }, TransformArgs{})
			require.ErrorIs(t, err, readErr)
			collector.Close()
			files, listErr := os.ReadDir(dir)
			require.NoError(t, listErr)
			require.Empty(t, files)
			if loads != 10 {
				t.Errorf("valid rows delivered before provider error: got %d; want 10", loads)
			}
		})
	}
}

func TestBackgroundErrorPrecedence(t *testing.T) {
	readErr := errors.New("provider read failed")
	loadErr := errors.New("consumer failed on row 5")
	for _, background := range []bool{false, true} {
		name := "serial"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			collector, _ := backgroundMergeCollector(t, 20, true, background)
			collector.dataProviders[0] = &backgroundFailingProvider{dataProvider: collector.dataProviders[0], failAt: 11, err: readErr}
			loads := 0
			err := backgroundMergeLoad(collector, true, func(_, _ []byte) error {
				loads++
				if loads == 5 {
					return loadErr
				}
				return nil
			}, TransformArgs{})
			if !errors.Is(err, loadErr) {
				t.Errorf("error precedence: got %v after %d loads; want consumer failed on row 5", err, loads)
			}
		})
	}
}

func TestBackgroundCancellationAfterMerge(t *testing.T) {
	for _, merged := range []bool{false, true} {
		for _, background := range []bool{false, true} {
			name := "serial"
			if background {
				name = "background"
			}
			if merged {
				name += "/MergeLoad"
			} else {
				name += "/Load"
			}
			t.Run(name, func(t *testing.T) {
				collector, _ := backgroundMergeCollector(t, 2048, false, background)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				loads := 0
				err := backgroundMergeLoad(collector, merged, func(_, _ []byte) error {
					loads++
					if loads == 1 {
						cancel()
					}
					return nil
				}, TransformArgs{Quit: ctx.Done()})
				if !errors.Is(err, common.ErrStopped) {
					t.Errorf("cancellation after first load: got err=%v loads=%d; want ErrStopped (serial loads=1023)", err, loads)
				}
			})
		}
	}
}

func TestBackgroundCancellationWithDeduplicatedInputs(t *testing.T) {
	for _, background := range []bool{false, true} {
		name := "serial"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			collectors := make([]*Collector, 2048)
			dir := t.TempDir()
			for i := range collectors {
				collector := NewCollector(t.Name(), dir, NewOldestEntryBuffer(BufferOptimalSize), log.New()).MergeInBackground(background)
				t.Cleanup(collector.Close)
				require.NoError(t, collector.Collect([]byte("same-key"), []byte("value")))
				collectors[i] = collector
			}
			quit := make(chan struct{})
			loads := 0
			err := MergeLoad(t.Name(), collectors, func(_, _ []byte) error {
				loads++
				if loads == 1 {
					close(quit)
				}
				return nil
			}, TransformArgs{Quit: quit})
			if !errors.Is(err, common.ErrStopped) {
				t.Errorf("cancellation after first deduplicated load: got err=%v loads=%d; want ErrStopped", err, loads)
			}
		})
	}
}
