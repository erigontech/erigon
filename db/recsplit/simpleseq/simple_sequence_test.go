package simpleseq

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv/stream"
)

func TestSimpleSequence(t *testing.T) {
	s := NewSimpleSequence(1000, 4)
	s.AddOffset(1001)
	s.AddOffset(1007)
	s.AddOffset(1015)
	s.AddOffset(1027)

	t.Run("basic test", func(t *testing.T) {
		require.Equal(t, uint64(1001), s.Get(0))
		require.Equal(t, uint64(1007), s.Get(1))
		require.Equal(t, uint64(1015), s.Get(2))
		require.Equal(t, uint64(1027), s.Get(3))

		require.Equal(t, uint64(1001), s.Min())
		require.Equal(t, uint64(1027), s.Max())
		require.Equal(t, uint64(4), s.Count())
	})

	t.Run("serialization", func(t *testing.T) {
		b := make([]byte, 0)
		b = s.AppendBytes(b)

		require.Equal(t, hexutil.MustDecodeHex("0x"+
			"00000001"+
			"00000007"+
			"0000000f"+
			"0000001b"), b)
	})

	t.Run("seek", func(t *testing.T) {
		// before baseNum
		v, found := s.Seek(10)
		require.True(t, found)
		require.Equal(t, uint64(1001), v)

		// at baseNum
		v, found = s.Seek(1000)
		require.True(t, found)
		require.Equal(t, uint64(1001), v)

		// at elem
		v, found = s.Seek(1007)
		require.True(t, found)
		require.Equal(t, uint64(1007), v)

		// between elems
		v, found = s.Seek(1014)
		require.True(t, found)
		require.Equal(t, uint64(1015), v)

		// at last
		v, found = s.Seek(1027)
		require.True(t, found)
		require.Equal(t, uint64(1027), v)

		// after last
		v, found = s.Seek(1028)
		require.False(t, found)
		require.Equal(t, uint64(0), v)

		require.True(t, s.Has(1007))
		require.False(t, s.Has(1008))
	})

	t.Run("iterator", func(t *testing.T) {
		it := s.Iterator()
		defer it.Close()

		require.True(t, it.HasNext())
		v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1001), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1007), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1015), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1027), v)

		require.False(t, it.HasNext())
		v, err = it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("iterator seek exact", func(t *testing.T) {
		it := s.Iterator()
		defer it.Close()

		it.Seek(1015)

		require.True(t, it.HasNext())
		v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1015), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1027), v)

		require.False(t, it.HasNext())
		v, err = it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("iterator seek", func(t *testing.T) {
		it := s.Iterator()
		defer it.Close()

		it.Seek(1014)

		require.True(t, it.HasNext())
		v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1015), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1027), v)

		require.False(t, it.HasNext())
		v, err = it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("iterator seek not found", func(t *testing.T) {
		it := s.Iterator()
		defer it.Close()

		it.Seek(1029)
		require.False(t, it.HasNext())
		v, err := it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("iterator seek before base num", func(t *testing.T) {
		it := s.Iterator()
		defer it.Close()

		it.Seek(999)
		require.True(t, it.HasNext())
		v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1001), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1007), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1015), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1027), v)

		require.False(t, it.HasNext())
		v, err = it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("reverse iterator", func(t *testing.T) {
		it := s.ReverseIterator()
		defer it.Close()

		require.True(t, it.HasNext())
		v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1027), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1015), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1007), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1001), v)

		require.False(t, it.HasNext())
		v, err = it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("reverse iterator seek exact", func(t *testing.T) {
		it := s.ReverseIterator()
		defer it.Close()

		it.Seek(1007)

		require.True(t, it.HasNext())
		v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1007), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1001), v)

		require.False(t, it.HasNext())
		v, err = it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("reverse iterator seek", func(t *testing.T) {
		it := s.ReverseIterator()
		defer it.Close()

		it.Seek(1008)

		require.True(t, it.HasNext())
		v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1007), v)

		require.True(t, it.HasNext())
		v, err = it.Next()
		require.NoError(t, err)
		require.Equal(t, uint64(1001), v)

		require.False(t, it.HasNext())
		v, err = it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("reverse iterator seek not found", func(t *testing.T) {
		it := s.ReverseIterator()
		defer it.Close()

		it.Seek(1000)
		require.False(t, it.HasNext())
		v, err := it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})

	t.Run("reverse iterator seek before base num", func(t *testing.T) {
		it := s.ReverseIterator()
		defer it.Close()

		it.Seek(999)
		require.False(t, it.HasNext())
		v, err := it.Next()
		require.ErrorIs(t, err, stream.ErrIteratorExhausted)
		require.Equal(t, uint64(0), v)
	})
}

func TestReadSimpleSequence(t *testing.T) {
	// Build a sequence via NewSimpleSequence, serialize it, then deserialize via
	// ReadSimpleSequence (which goes through Reset). Regression test for a bug
	// where Reset did not update the cached `count` field, causing Count()==0 on
	// deserialized sequences.
	orig := NewSimpleSequence(1000, 4)
	orig.AddOffset(1001)
	orig.AddOffset(1007)
	orig.AddOffset(1015)
	orig.AddOffset(1027)

	raw := orig.AppendBytes(nil)
	s := ReadSimpleSequence(1000, raw)

	require.Equal(t, uint64(4), s.Count())
	require.Equal(t, uint64(1001), s.Min())
	require.Equal(t, uint64(1027), s.Max())

	v, found := s.Seek(1007)
	require.True(t, found)
	require.Equal(t, uint64(1007), v)

	v, found = s.Seek(9999)
	require.False(t, found)
	require.Equal(t, uint64(0), v)
}

func makeSequence(n int) *SimpleSequence {
	base := uint64(1_000_000)
	s := NewSimpleSequence(base, uint64(n))
	for i := 0; i < n; i++ {
		s.AddOffset(base + uint64(i)*7 + 1)
	}
	return s
}

func BenchmarkSimpleSequenceSeek(b *testing.B) {
	for _, size := range []int{1, 2, 4, 16} {
		s := makeSequence(size)
		minV := s.Min()
		maxV := s.Max()
		midV := s.Get(uint64(size / 2))

		b.Run(fmt.Sprintf("n=%d/hit_first", size), func(b *testing.B) {
			for b.Loop() {
				s.Seek(minV)
			}
		})
		b.Run(fmt.Sprintf("n=%d/hit_mid", size), func(b *testing.B) {
			for b.Loop() {
				s.Seek(midV)
			}
		})
		b.Run(fmt.Sprintf("n=%d/hit_last", size), func(b *testing.B) {
			for b.Loop() {
				s.Seek(maxV)
			}
		})
		b.Run(fmt.Sprintf("n=%d/miss", size), func(b *testing.B) {
			for b.Loop() {
				s.Seek(maxV + 1)
			}
		})
	}
}
