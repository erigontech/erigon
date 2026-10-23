package etl

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func reviewMergeCollector(t *testing.T, count int, disk, background bool) (*Collector, string) {
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

func reviewMergeLoad(collector *Collector, merged bool, load simpleLoadFunc, args TransformArgs) error {
	if merged {
		return MergeLoad("review", []*Collector{collector}, load, args)
	}
	return collector.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
		return load(k, v)
	}, args)
}

func TestReviewBackgroundRetainedSlices(t *testing.T) {
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
					collector, _ := reviewMergeCollector(t, 6*mergeBatchEntries, disk, background)
					var firstKey, firstValue []byte
					err := reviewMergeLoad(collector, merged, func(k, v []byte) error {
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

type reviewFailingProvider struct {
	dataProvider
	reads  int
	failAt int
	err    error
}

func (p *reviewFailingProvider) Next() ([]byte, []byte, error) {
	p.reads++
	if p.reads == p.failAt {
		return nil, nil, p.err
	}
	return p.dataProvider.Next()
}

func TestReviewBackgroundProviderErrorPrefix(t *testing.T) {
	readErr := errors.New("provider read failed")
	for _, background := range []bool{false, true} {
		name := "serial"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			collector, dir := reviewMergeCollector(t, 20, true, background)
			collector.dataProviders[0] = &reviewFailingProvider{dataProvider: collector.dataProviders[0], failAt: 11, err: readErr}
			loads := 0
			err := reviewMergeLoad(collector, false, func(_, _ []byte) error { loads++; return nil }, TransformArgs{})
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

func TestReviewBackgroundErrorPrecedence(t *testing.T) {
	readErr := errors.New("provider read failed")
	loadErr := errors.New("consumer failed on row 5")
	for _, background := range []bool{false, true} {
		name := "serial"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			collector, _ := reviewMergeCollector(t, 20, true, background)
			collector.dataProviders[0] = &reviewFailingProvider{dataProvider: collector.dataProviders[0], failAt: 11, err: readErr}
			loads := 0
			err := reviewMergeLoad(collector, true, func(_, _ []byte) error {
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

func TestReviewBackgroundCancellationAfterMerge(t *testing.T) {
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
				collector, _ := reviewMergeCollector(t, 2048, false, background)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				loads := 0
				err := reviewMergeLoad(collector, merged, func(_, _ []byte) error {
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
