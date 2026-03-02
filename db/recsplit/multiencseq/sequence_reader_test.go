package multiencseq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/recsplit/simpleseq"
)

func TestMultiEncSeq(t *testing.T) {

	t.Run("plain elias fano", func(t *testing.T) {
		b := make([]byte, 0)

		// append serialized elias fano
		ef := eliasfano32.NewEliasFano(3, 1027)
		ef.AddOffset(1000)
		ef.AddOffset(1015)
		ef.AddOffset(1027)
		ef.Build()
		b = ef.AppendBytes(b)

		// check deserialization
		s := ReadMultiEncSeq(1000, b)
		require.Equal(t, PlainEliasFano, s.EncodingType())
		requireSequenceChecks(t, s)
		requireRawDataChecks(t, b)
	})

	t.Run("simple encoding", func(t *testing.T) {
		b := make([]byte, 0)

		// type: simple encoding, count: 3
		b = append(b, 0b10000010)

		// append serialized simple sequence
		seq := simpleseq.NewSimpleSequence(1000, 3)
		seq.AddOffset(1000)
		seq.AddOffset(1015)
		seq.AddOffset(1027)
		b = seq.AppendBytes(b)

		// check deserialization
		s := ReadMultiEncSeq(1000, b)
		require.Equal(t, SimpleEncoding, s.EncodingType())
		requireSequenceChecks(t, s)
		requireRawDataChecks(t, b)
	})

	t.Run("rebased elias fano", func(t *testing.T) {
		b := make([]byte, 0)

		// type: rebased elias fano
		b = append(b, 0b10010000)

		// append serialized elias fano (rebased -1000)
		ef := eliasfano32.NewEliasFano(3, 27)
		ef.AddOffset(0)
		ef.AddOffset(15)
		ef.AddOffset(27)
		ef.Build()
		b = ef.AppendBytes(b)

		// check deserialization
		s := ReadMultiEncSeq(1000, b)
		require.Equal(t, RebasedEliasFano, s.EncodingType())
		requireSequenceChecks(t, s)
		requireRawDataChecks(t, b)
	})

	t.Run("reset", func(t *testing.T) {
		b := make([]byte, 0)

		// type: simple encoding, count: 3
		b = append(b, 0b10000010)

		// append serialized simple sequence
		seq := simpleseq.NewSimpleSequence(1000, 3)
		seq.AddOffset(1000)
		seq.AddOffset(1015)
		seq.AddOffset(1027)
		b = seq.AppendBytes(b)

		// check deserialization through reset
		var s SequenceReader
		s.Reset(1000, b)
		require.Equal(t, SimpleEncoding, s.EncodingType())
		requireSequenceChecks(t, &s)
		requireRawDataChecks(t, b)

		// RESET
		b = make([]byte, 0)

		// type: rebased elias fano
		b = append(b, 0b10010000)

		// append serialized elias fano (rebased -1000)
		ef := eliasfano32.NewEliasFano(3, 27)
		ef.AddOffset(0)
		ef.AddOffset(15)
		ef.AddOffset(27)
		ef.Build()
		b = ef.AppendBytes(b)

		// check deserialization
		s.Reset(1000, b)
		require.Equal(t, RebasedEliasFano, s.EncodingType())
		requireSequenceChecks(t, &s)
		requireRawDataChecks(t, b)
	})
}

func TestMerge(t *testing.T) {
	buildSeq := func(baseNum uint64, vals ...uint64) []byte {
		b := NewBuilder(baseNum, uint64(len(vals)), vals[len(vals)-1])
		for _, v := range vals {
			b.AddOffset(v)
		}
		b.Build()
		return b.AppendBytes(nil)
	}

	t.Run("small sequences (simple encoding path)", func(t *testing.T) {
		// 3 + 3 = 6 elements: stays within SIMPLE_SEQUENCE_MAX_THRESHOLD
		s1 := ReadMultiEncSeq(1000, buildSeq(1000, 1001, 1003, 1005))
		s2 := ReadMultiEncSeq(1000, buildSeq(1000, 1007, 1009, 1011))

		var it1, it2 SequenceIterator
		merged, err := s1.Merge(s2, 1000, &it1, &it2)
		require.NoError(t, err)

		out := merged.AppendBytes(nil)
		result := ReadMultiEncSeq(1000, out)
		require.Equal(t, uint64(6), result.Count())
		require.Equal(t, uint64(1001), result.Min())
		require.Equal(t, uint64(1011), result.Max())
		for i, want := range []uint64{1001, 1003, 1005, 1007, 1009, 1011} {
			require.Equal(t, want, result.Get(uint64(i)))
		}
	})

	t.Run("large sequences (rebased EF fast path)", func(t *testing.T) {
		// 10 + 10 = 20 elements: exceeds SIMPLE_SEQUENCE_MAX_THRESHOLD
		vals1 := make([]uint64, 10)
		vals2 := make([]uint64, 10)
		for i := range vals1 {
			vals1[i] = 1000 + uint64(i)*2
			vals2[i] = 1020 + uint64(i)*2
		}
		s1 := ReadMultiEncSeq(1000, buildSeq(1000, vals1...))
		s2 := ReadMultiEncSeq(1000, buildSeq(1000, vals2...))

		var it1, it2 SequenceIterator
		merged, err := s1.Merge(s2, 1000, &it1, &it2)
		require.NoError(t, err)

		out := merged.AppendBytes(nil)
		require.Equal(t, byte(RebasedEliasFano), out[0], "expected rebased EF encoding")

		result := ReadMultiEncSeq(1000, out)
		require.Equal(t, uint64(20), result.Count())
		require.Equal(t, uint64(1000), result.Min())
		require.Equal(t, uint64(1038), result.Max())
		for i := uint64(0); i < 20; i++ {
			require.Equal(t, 1000+i*2, result.Get(i))
		}
	})
}

func BenchmarkMerge(b *testing.B) {
	const baseNum = 1_000_000
	const n = 500 // elements per sequence

	raw1 := func() []byte {
		sb := NewBuilder(baseNum, n, baseNum+n*2-2)
		for i := uint64(0); i < n; i++ {
			sb.AddOffset(baseNum + i*2)
		}
		sb.Build()
		return sb.AppendBytes(nil)
	}()
	raw2 := func() []byte {
		sb := NewBuilder(baseNum, n, baseNum+n*2+n*2-2)
		for i := uint64(0); i < n; i++ {
			sb.AddOffset(baseNum + n*2 + i*2)
		}
		sb.Build()
		return sb.AppendBytes(nil)
	}()

	var s1, s2 SequenceReader
	var it1, it2 SequenceIterator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s1.Reset(baseNum, raw1)
		s2.Reset(baseNum, raw2)
		merged, err := s1.Merge(&s2, baseNum, &it1, &it2)
		if err != nil {
			b.Fatal(err)
		}
		_ = merged.AppendBytes(nil)
	}
}

func requireSequenceChecks(t *testing.T, s *SequenceReader) {
	t.Helper()

	require.Equal(t, uint64(1000), s.Min())
	require.Equal(t, uint64(1027), s.Max())
	require.Equal(t, uint64(3), s.Count())

	require.Equal(t, uint64(1000), s.Get(0))
	require.Equal(t, uint64(1015), s.Get(1))
	require.Equal(t, uint64(1027), s.Get(2))

	// check iterator
	it := s.Iterator(0)
	require.True(t, it.HasNext())
	n, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1027), n)

	require.False(t, it.HasNext())

	// check iterator + seek
	it = s.Iterator(1014)
	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1027), n)

	require.False(t, it.HasNext())

	// check iterator + seek before base num
	it = s.Iterator(999)
	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1027), n)

	require.False(t, it.HasNext())

	// check reverse iterator
	it = s.ReverseIterator(2000)
	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1027), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), n)

	require.False(t, it.HasNext())

	// check reverse iterator + seek
	it = s.ReverseIterator(1016)
	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), n)

	require.False(t, it.HasNext())

	// check reverse iterator + seek before base num
	it = s.ReverseIterator(999)
	require.False(t, it.HasNext())

	require.True(t, s.Has(1015))
	require.False(t, s.Has(1014))
}

func requireRawDataChecks(t *testing.T, b []byte) {
	t.Helper()

	// check fast count
	require.Equal(t, uint64(3), Count(1000, b))

	// check search
	n, found := Seek(1000, b, 1014)
	require.True(t, found)
	require.Equal(t, uint64(1015), n)

	n, found = Seek(1000, b, 1015)
	require.True(t, found)
	require.Equal(t, uint64(1015), n)

	_, found = Seek(1000, b, 1028)
	require.False(t, found)

	// check search before base num
	n, found = Seek(1000, b, 999)
	require.True(t, found)
	require.Equal(t, uint64(1000), n)
}
