package etl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestSpillReaderRecords(t *testing.T) {
	buffer := NewSortableBuffer(32 << 20)
	defer buffer.Reset()
	keys := [][]byte{nil, {}}
	values := [][]byte{{}, nil}
	for i := range 40 {
		key := make([]byte, 32)
		binary.BigEndian.PutUint64(key, uint64(i+1))
		keys = append(keys, key)
		values = append(values, bytes.Repeat([]byte{byte(i), byte(i + 1), byte(i + 2)}, (512<<10)/3+i))
	}
	buffer.Put(keys[0], values[0])
	buffer.Put(keys[1], values[1])
	for i := len(keys) - 1; i >= 2; i-- {
		buffer.Put(keys[i], values[i])
	}
	provider, err := FlushToDisk(t.Name(), buffer, t.TempDir(), log.LvlDebug)
	require.NoError(t, err)
	defer provider.Dispose()
	var gotKeys, gotValues [][]byte
	for range keys {
		key, value, readErr := provider.Next()
		require.NoError(t, readErr)
		gotKeys = append(gotKeys, key)
		gotValues = append(gotValues, value)
	}
	_, _, err = provider.Next()
	require.ErrorIs(t, err, io.EOF)
	for i := range keys {
		require.Equal(t, keys[i], gotKeys[i])
		require.Equal(t, values[i], gotValues[i])
	}
	fileProvider := provider.(*fileDataProvider)
	require.NotNil(t, fileProvider.mmapReader)
	require.Positive(t, fileProvider.discarded)
}

func TestSpillReaderAdviceError(t *testing.T) {
	for _, mode := range []string{"Next", "Load"} {
		t.Run(mode, func(t *testing.T) {
			original := discardSpillRange
			t.Cleanup(func() { discardSpillRange = original })
			calls := 0
			discardSpillRange = func(*os.File, []byte, int, int) error {
				calls++
				return syscall.ENOSYS
			}
			c := NewCollector(t.Name(), t.TempDir(), NewSortableBuffer(32<<20), log.New())
			defer c.Close()
			keys := [][]byte{[]byte("a"), []byte("b")}
			values := [][]byte{bytes.Repeat([]byte{42}, 8<<20), []byte("last")}
			for i := range keys {
				require.NoError(t, c.Collect(keys[i], values[i]))
			}
			require.NoError(t, c.Flush())
			require.Len(t, c.dataProviders, 1)
			if mode == "Next" {
				p := c.dataProviders[0]
				require.NoError(t, p.Wait())
				for i := range keys {
					k, v, err := p.Next()
					require.NoError(t, err)
					require.Equal(t, keys[i], k)
					require.Equal(t, values[i], v)
				}
				_, _, err := p.Next()
				require.ErrorIs(t, err, io.EOF)
			} else {
				seen := 0
				err := c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, _ LoadNextFunc) error {
					require.Less(t, seen, len(keys))
					require.Equal(t, keys[seen], k)
					require.Equal(t, values[seen], v)
					seen++
					return nil
				}, TransformArgs{})
				require.NoError(t, err)
				require.Equal(t, len(keys), seen)
			}
			require.Positive(t, calls)
		})
	}
}

func TestSpillReaderDrainAtEOF(t *testing.T) {
	for _, sizes := range [][]int{
		{13},
		{os.Getpagesize() - 7},
		{(7 << 20) + 13},
		{(12 << 20) + 13},
		{(4 << 20) + 13, (4 << 20) + 13, (4 << 20) + 13},
	} {
		t.Run(fmt.Sprint(sizes), func(t *testing.T) {
			buffer := NewSortableBuffer(32 << 20)
			defer buffer.Reset()
			for i, size := range sizes {
				buffer.Put([]byte{byte(i)}, bytes.Repeat([]byte{42}, size))
			}
			provider, err := FlushToDisk(t.Name(), buffer, t.TempDir(), log.LvlDebug)
			require.NoError(t, err)
			defer provider.Dispose()
			p := provider.(*fileDataProvider)
			original := discardSpillRange
			t.Cleanup(func() { discardSpillRange = original })
			lastEnd, calls := 0, 0
			discardSpillRange = func(file *os.File, data []byte, start, end int) error {
				require.Same(t, p.file, file)
				require.Equal(t, len(p.mmapData), len(data))
				require.Equal(t, lastEnd, start)
				require.Zero(t, start%os.Getpagesize())
				require.Greater(t, end, start)
				require.LessOrEqual(t, end, p.mmapReader.pos)
				lastEnd = end
				calls++
				return nil
			}
			for i, size := range sizes {
				key, value, nextErr := p.Next()
				require.NoError(t, nextErr)
				require.Equal(t, []byte{byte(i)}, key)
				require.Len(t, value, size)
			}
			_, _, err = p.Next()
			require.ErrorIs(t, err, io.EOF)
			require.Equal(t, len(p.mmapData), lastEnd)
			require.Equal(t, lastEnd, p.discarded)
			callsAtEOF := calls
			_, _, err = p.Next()
			require.ErrorIs(t, err, io.EOF)
			require.Equal(t, callsAtEOF, calls)
		})
	}
}
