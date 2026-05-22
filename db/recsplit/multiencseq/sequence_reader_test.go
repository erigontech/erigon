package multiencseq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/recsplit/simpleseq"
)

func buildTestSeq(baseNum uint64, vals ...uint64) []byte {
	b := NewBuilder(baseNum, uint64(len(vals)), vals[len(vals)-1])
	for _, v := range vals {
		b.AddOffset(v)
	}
	b.Build()
	return b.AppendBytes(nil)
}

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
	t.Run("small sequences (simple encoding path)", func(t *testing.T) {
		// 3 + 3 = 6 elements: stays within SIMPLE_SEQUENCE_MAX_THRESHOLD
		s1 := ReadMultiEncSeq(1000, buildTestSeq(1000, 1001, 1003, 1005))
		s2 := ReadMultiEncSeq(1000, buildTestSeq(1000, 1007, 1009, 1011))

		var merged SequenceBuilder
		err := merged.Merge(s1, s2, 1000)
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
		s1 := ReadMultiEncSeq(1000, buildTestSeq(1000, vals1...))
		s2 := ReadMultiEncSeq(1000, buildTestSeq(1000, vals2...))

		var merged SequenceBuilder
		err := merged.Merge(s1, s2, 1000)
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

func TestMergeSorted(t *testing.T) {
	t.Run("small sequences (simple encoding path)", func(t *testing.T) {
		// 3 + 3 = 6 elements across two files with different baseNums
		raw1 := buildTestSeq(1000, 1001, 1003, 1005)
		raw2 := buildTestSeq(1006, 1007, 1009, 1011)

		var sr SequenceReader
		var merged SequenceBuilder
		err := merged.MergeSorted(&sr, 1000, []uint64{1000, 1006}, [][]byte{raw1, raw2})
		require.NoError(t, err)

		result := ReadMultiEncSeq(1000, merged.AppendBytes(nil))
		require.Equal(t, uint64(6), result.Count())
		require.Equal(t, uint64(1001), result.Min())
		require.Equal(t, uint64(1011), result.Max())
		for i, want := range []uint64{1001, 1003, 1005, 1007, 1009, 1011} {
			require.Equal(t, want, result.Get(uint64(i)))
		}
	})

	t.Run("large sequences (rebased EF path)", func(t *testing.T) {
		// 10 + 10 = 20 elements across two files
		vals1 := make([]uint64, 10)
		vals2 := make([]uint64, 10)
		for i := range vals1 {
			vals1[i] = 1000 + uint64(i)*2
			vals2[i] = 1020 + uint64(i)*2
		}
		raw1 := buildTestSeq(1000, vals1...)
		raw2 := buildTestSeq(1020, vals2...)

		var sr SequenceReader
		var merged SequenceBuilder
		err := merged.MergeSorted(&sr, 1000, []uint64{1000, 1020}, [][]byte{raw1, raw2})
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

	t.Run("three sequences", func(t *testing.T) {
		// 4 + 4 + 4 = 12 elements across three files
		raw1 := buildTestSeq(1000, 1001, 1003, 1005, 1007)
		raw2 := buildTestSeq(1010, 1011, 1013, 1015, 1017)
		raw3 := buildTestSeq(1020, 1021, 1023, 1025, 1027)

		var sr SequenceReader
		var merged SequenceBuilder
		err := merged.MergeSorted(&sr, 1000, []uint64{1000, 1010, 1020}, [][]byte{raw1, raw2, raw3})
		require.NoError(t, err)

		result := ReadMultiEncSeq(1000, merged.AppendBytes(nil))
		require.Equal(t, uint64(12), result.Count())
		require.Equal(t, uint64(1001), result.Min())
		require.Equal(t, uint64(1027), result.Max())
		want := []uint64{1001, 1003, 1005, 1007, 1011, 1013, 1015, 1017, 1021, 1023, 1025, 1027}
		for i, v := range want {
			require.Equal(t, v, result.Get(uint64(i)))
		}
	})

	t.Run("out of order panics", func(t *testing.T) {
		raw1 := buildTestSeq(1000, 1001, 1010)
		raw2 := buildTestSeq(1005, 1005, 1020) // Min=1005 < prevMax=1010

		var sr SequenceReader
		var merged SequenceBuilder
		require.Panics(t, func() {
			_ = merged.MergeSorted(&sr, 1000, []uint64{1000, 1005}, [][]byte{raw1, raw2})
		})
	})

	t.Run("single sequence is a no-op rebase", func(t *testing.T) {
		raw := buildTestSeq(500, 501, 503, 505)

		var sr SequenceReader
		var merged SequenceBuilder
		err := merged.MergeSorted(&sr, 500, []uint64{500}, [][]byte{raw})
		require.NoError(t, err)

		result := ReadMultiEncSeq(500, merged.AppendBytes(nil))
		require.Equal(t, uint64(3), result.Count())
		require.Equal(t, uint64(501), result.Min())
		require.Equal(t, uint64(505), result.Max())
	})
}

func TestMergeEncodingBoundary(t *testing.T) {
	merge := func(baseNum uint64, raw1, raw2 []byte) []byte {
		s1 := ReadMultiEncSeq(baseNum, raw1)
		s2 := ReadMultiEncSeq(baseNum, raw2)
		var merged SequenceBuilder
		if err := merged.Merge(s1, s2, baseNum); err != nil {
			t.Fatal(err)
		}
		return merged.AppendBytes(nil)
	}

	// 8+8=16: must stay simple encoding
	raw16 := merge(1000,
		buildTestSeq(1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008),
		buildTestSeq(1000, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016),
	)
	require.Equal(t, byte(SimpleEncoding)|15, raw16[0], "8+8=16 must use simple encoding")

	// 8+9=17: must flip to rebased EF
	raw17 := merge(1000,
		buildTestSeq(1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008),
		buildTestSeq(1000, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017),
	)
	require.Equal(t, byte(RebasedEliasFano), raw17[0], "8+9=17 must use rebased EF")
}

func TestMergeSeek(t *testing.T) {
	// merge two large sequences so output uses rebased EF
	vals1 := make([]uint64, 10)
	vals2 := make([]uint64, 10)
	for i := range vals1 {
		vals1[i] = 1000 + uint64(i)*2 // 1000,1002,...,1018
		vals2[i] = 1020 + uint64(i)*2 // 1020,1022,...,1038
	}
	s1 := ReadMultiEncSeq(1000, buildTestSeq(1000, vals1...))
	s2 := ReadMultiEncSeq(1000, buildTestSeq(1000, vals2...))
	var merged SequenceBuilder
	err := merged.Merge(s1, s2, 1000)
	require.NoError(t, err)
	result := ReadMultiEncSeq(1000, merged.AppendBytes(nil))

	// Seek to existing value
	n, ok := result.Seek(1010)
	require.True(t, ok)
	require.Equal(t, uint64(1010), n)

	// Seek to gap — returns next
	n, ok = result.Seek(1011)
	require.True(t, ok)
	require.Equal(t, uint64(1012), n)

	// Seek past end
	_, ok = result.Seek(1039)
	require.False(t, ok)

	// Has
	require.True(t, result.Has(1020))
	require.False(t, result.Has(1021))
}

func TestBuilderFreeFunctions(t *testing.T) {
	const baseNum = uint64(5000)
	vals := []uint64{5003, 5007, 5015}
	b := NewBuilder(baseNum, uint64(len(vals)), vals[len(vals)-1])
	for _, v := range vals {
		b.AddOffset(v)
	}
	b.Build()
	raw := b.AppendBytes(nil)

	require.Equal(t, uint64(3), Count(baseNum, raw))

	n, ok := Seek(baseNum, raw, 5006)
	require.True(t, ok)
	require.Equal(t, uint64(5007), n)

	n, ok = Seek(baseNum, raw, 5007)
	require.True(t, ok)
	require.Equal(t, uint64(5007), n)

	_, ok = Seek(baseNum, raw, 5016)
	require.False(t, ok)
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
	var merged SequenceBuilder
	for b.Loop() {
		s1.Reset(baseNum, raw1)
		s2.Reset(baseNum, raw2)
		if err := merged.Merge(&s1, &s2, baseNum); err != nil {
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
