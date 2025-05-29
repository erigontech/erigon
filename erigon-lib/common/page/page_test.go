package page

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
)

// multyBytesWriter is a writer for [][]byte, similar to bytes.Writer.
type multyBytesWriter struct {
	buffer [][]byte
}

func (w *multyBytesWriter) Write(p []byte) (n int, err error) {
	w.buffer = append(w.buffer, common.Copy(p))
	return len(p), nil
}
func (w *multyBytesWriter) Bytes() [][]byte { return w.buffer }
func (w *multyBytesWriter) Reset()          { w.buffer = nil }

func TestPage(t *testing.T) {
	buf, require := &multyBytesWriter{}, require.New(t)
	sampling := 2
	w := NewWriter(buf, sampling, false)
	for i := 0; i < sampling+1; i++ {
		k, v := fmt.Sprintf("k %d", i), fmt.Sprintf("v %d", i)
		require.NoError(w.Add([]byte(k), []byte(v)))
	}
	require.NoError(w.Flush())
	pages := buf.Bytes()
	pageNum := 0
	p1 := &Reader{}
	p1.Reset(pages[0], false)

	iter := 0
	for i := 0; i < sampling+1; i++ {
		iter++
		expectK, expectV := fmt.Sprintf("k %d", i), fmt.Sprintf("v %d", i)
		v, _ := Get([]byte(expectK), pages[pageNum], nil, false)
		require.Equal(expectV, string(v), i)
		require.True(p1.HasNext())
		k, v := p1.Next()
		require.Equal(expectK, string(k), i)
		require.Equal(expectV, string(v), i)

		if iter%sampling == 0 {
			pageNum++

			require.False(p1.HasNext())
			p1.Reset(pages[pageNum], false)
		}
	}
}

func BenchmarkName(b *testing.B) {
	buf := bytes.NewBuffer(nil)
	w := NewWriter(buf, 16, false)
	for i := 0; i < 16; i++ {
		w.Add([]byte{byte(i)}, []byte{10 + byte(i)})
	}
	bts := buf.Bytes()

	k := []byte{15}

	b.Run("1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Get(k, bts, nil, false)
		}
	})

}
