package page

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
)

func TestPage(t *testing.T) {
	buf, require := bytes.NewBuffer(nil), require.New(t)
	w := NewWriter(buf, 2, false)
	require.NoError(w.Add([]byte{1}, []byte{11}))
	require.NoError(w.Add([]byte{2}, []byte{12}))
	require.NoError(w.Flush())
	bts := common.Copy(buf.Bytes())
	v := Get([]byte{1}, bts, false)
	require.Equal([]byte{11}, v)
	v = Get([]byte{2}, bts, false)
	require.Equal([]byte{12}, v)

	p1 := &Reader{}
	p1.Reset(bts, false)
	require.True(p1.HasNext())
	k, v := p1.Next()
	require.Equal([]byte{1}, k)
	require.Equal([]byte{11}, v)

	require.True(p1.HasNext())
	k, v = p1.Next()
	require.Equal([]byte{2}, k)
	require.Equal([]byte{12}, v)

	require.False(p1.HasNext())

	p1.Reset(bts, false)
	require.True(p1.HasNext())
	k, v = p1.Next()
	require.Equal([]byte{1}, k)
	require.Equal([]byte{11}, v)

	require.True(p1.HasNext())
	k, v = p1.Next()
	require.Equal([]byte{2}, k)
	require.Equal([]byte{12}, v)

	require.False(p1.HasNext())
}

func BenchmarkName(b *testing.B) {
	buf := bytes.NewBuffer(nil)
	w := NewWriter(buf, 2, false)
	w.Add([]byte{1}, []byte{11})
	w.Add([]byte{2}, []byte{12})
	bts := common.Copy(buf.Bytes())

	k := []byte{2}

	b.Run("1", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Get(k, bts, false)
		}
	})

}
