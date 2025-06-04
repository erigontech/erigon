package page

import (
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
func (w *multyBytesWriter) Bytes() [][]byte  { return w.buffer }
func (w *multyBytesWriter) Reset()           { w.buffer = nil }
func (w *multyBytesWriter) Compress() error  { return nil }
func (w *multyBytesWriter) Count() int       { return 0 }
func (w *multyBytesWriter) FileName() string { return "" }

func TestPage(t *testing.T) {
	buf := &multyBytesWriter{}
	w := NewWriter(buf, 2, false)
	for i := 0; i < 3; i++ {
		k, v := fmt.Sprintf("k %d", i), fmt.Sprintf("v %d", i)
		require.NoError(t, w.AddForHistory([]byte(k), []byte(v)))
	}
	require.NoError(t, w.Flush())

	pages := buf.Bytes()
	p1 := &Page{}
	p1.Reset(pages[0], false)

	k, v := p1.Next()
	require.Equal(t, "k 0", string(k))
	require.Equal(t, "v 0", string(v))

	k, v = p1.Next()
	require.Equal(t, "k 1", string(k))
	require.False(t, p1.HasNext())
}

func TestSeek(t *testing.T) {
	buf := &multyBytesWriter{}
	w := NewWriter(buf, 3, false)
	keys := []string{"a", "c", "e"}
	for _, k := range keys {
		w.Add([]byte(k), []byte("val_"+k))
	}
	w.Flush()

	t.Run("basic", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)

		k, _ := reader.Seek([]byte("c"))
		require.Equal(t, "c", string(k))

		k, _ = reader.Seek([]byte("b"))
		require.Equal(t, "c", string(k))

		k, _ = reader.Seek([]byte("z"))
		require.Nil(t, k)
	})

	t.Run("empty", func(t *testing.T) {
		emptyBuf := &multyBytesWriter{}
		emptyW := NewWriter(emptyBuf, 5, false)
		emptyW.Flush()

		reader := &Page{}
		reader.Reset([]byte{0}, false)
		k, v := reader.Seek([]byte("any"))
		require.Nil(t, k)
		require.Nil(t, v)
	})

	t.Run("between_keys", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		k, _ := reader.Seek([]byte("b"))
		require.Equal(t, "c", string(k))
	})

	t.Run("before_all", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		k, _ := reader.Seek([]byte("0"))
		require.Equal(t, "a", string(k))
	})

	t.Run("exact_match", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		k, v := reader.Seek([]byte("c"))
		require.Equal(t, "c", string(k))
		require.Equal(t, "val_c", string(v))
	})
}

func TestCurrent(t *testing.T) {
	buf := &multyBytesWriter{}
	w := NewWriter(buf, 5, false)
	keys := []string{"a", "c", "e"}
	for _, key := range keys {
		w.Add([]byte(key), []byte("val_"+key))
	}
	w.Flush()

	t.Run("basic_operations", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)

		k, v := reader.Current()
		require.Equal(t, "a", string(k))
		require.Equal(t, "val_a", string(v))

		// Multiple calls don't advance
		k2, _ := reader.Current()
		require.Equal(t, "a", string(k2))

		reader.Next()
		k, v = reader.Current()
		require.Equal(t, "c", string(k))
		require.Equal(t, "val_c", string(v))
	})

	t.Run("past_end", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		reader.Next() // a
		reader.Next() // c
		reader.Next() // e
		k, v := reader.Current()
		require.Nil(t, k)
		require.Nil(t, v)
	})

	t.Run("after_seek", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		reader.Seek([]byte("b"))
		k, v := reader.Current()
		require.Equal(t, "e", string(k))
		require.Equal(t, "val_e", string(v))
	})

	t.Run("after_reset", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		reader.Next() // Advance position
		reader.Reset(buf.Bytes()[0], false)
		k, v := reader.Current()
		require.Equal(t, "a", string(k))
		require.Equal(t, "val_a", string(v))
	})
}

func TestLast(t *testing.T) {
	buf := &multyBytesWriter{}
	w := NewWriter(buf, 5, false)
	keys := []string{"a", "b", "c", "d"}
	for _, key := range keys {
		w.Add([]byte(key), []byte("val_"+key))
	}
	w.Flush()

	t.Run("empty", func(t *testing.T) {
		emptyBuf := &multyBytesWriter{}
		emptyW := NewWriter(emptyBuf, 2, false)
		emptyW.Flush()

		if len(emptyBuf.Bytes()) > 0 {
			reader := FromBytes(emptyBuf.Bytes()[0], false)
			k, v := reader.Last()
			require.Nil(t, k)
			require.Nil(t, v)
		}
	})

	t.Run("single_item", func(t *testing.T) {
		singleBuf := &multyBytesWriter{}
		singleW := NewWriter(singleBuf, 2, false)
		singleW.Add([]byte("only"), []byte("val_only"))
		singleW.Flush()

		reader := FromBytes(singleBuf.Bytes()[0], false)
		k, v := reader.Last()
		require.Equal(t, "only", string(k))
		require.Equal(t, "val_only", string(v))
	})

	t.Run("multiple_items", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		k, v := reader.Last()
		require.Equal(t, "d", string(k))
		require.Equal(t, "val_d", string(v))

		// Position should be at end after Last()
		require.False(t, reader.HasNext())
	})

	t.Run("after_operations", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)

		// After Next
		reader.Next()
		k, v := reader.Last()
		require.Equal(t, "d", string(k))
		require.Equal(t, "val_d", string(v))

		// After Seek
		reader.Seek([]byte("b"))
		k, v = reader.Last()
		require.Equal(t, "d", string(k))
		require.Equal(t, "val_d", string(v))
	})
}

func TestEdgeCases(t *testing.T) {
	// Setup common data
	buf := &multyBytesWriter{}
	w := NewWriter(buf, 3, false)
	w.Add([]byte("a"), []byte("val_a"))
	w.Add([]byte("b"), []byte("val_b"))
	w.Add([]byte("exists"), []byte("val_exists"))
	w.Flush()

	t.Run("hasNext_boundary", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		require.True(t, reader.HasNext())

		reader.Next()
		require.True(t, reader.HasNext())

		reader.Next()
		require.True(t, reader.HasNext())

		reader.Next()
		require.False(t, reader.HasNext())
	})

	t.Run("get_nonexistent", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		v := reader.Get([]byte("missing"))
		require.Nil(t, v)

		v = reader.Get([]byte("exists"))
		require.Equal(t, "val_exists", string(v))
	})

	t.Run("first_method", func(t *testing.T) {
		reader := FromBytes(buf.Bytes()[0], false)
		reader.Next() // Advance position

		k, v := reader.First()
		require.Equal(t, "a", string(k))
		require.Equal(t, "val_a", string(v))
	})
}

func BenchmarkPage(b *testing.B) {
	buf := &multyBytesWriter{}
	w := NewWriter(buf, 256, false)
	for i := 0; i < 256; i++ {
		w.Add([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
	}
	w.Flush()
	reader := FromBytes(buf.Bytes()[0], false)

	getKey := []byte("k255")
	seekKey := []byte("k128")

	b.Run("get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			Get(getKey, buf.Bytes()[0], nil, false)
		}
	})

	b.Run("seek", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader.Seek(seekKey)
		}
	})

	b.Run("last", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader.Last()
		}
	})

	b.Run("first", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader.First()
		}
	})
}
