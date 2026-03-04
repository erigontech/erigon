package multiencseq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
)

func TestMultiEncodingSeqBuilder(t *testing.T) {

	t.Run("singleton sequence", func(t *testing.T) {
		builder := NewBuilder(1000, 1, 1005)
		builder.AddOffset(1005)
		builder.Build()

		b := make([]byte, 0)
		b = builder.AppendBytes(b)
		require.Equal(t, hexutil.MustDecodeHex("0x8000000005"), b)
	})

	t.Run("short sequences must use simple encoding", func(t *testing.T) {
		builder := NewBuilder(1000, 16, 1035)
		builder.AddOffset(1005)
		builder.AddOffset(1007)
		builder.AddOffset(1009)
		builder.AddOffset(1011)
		builder.AddOffset(1013)
		builder.AddOffset(1015)
		builder.AddOffset(1017)
		builder.AddOffset(1019)
		builder.AddOffset(1021)
		builder.AddOffset(1023)
		builder.AddOffset(1025)
		builder.AddOffset(1027)
		builder.AddOffset(1029)
		builder.AddOffset(1031)
		builder.AddOffset(1033)
		builder.AddOffset(1035)
		builder.Build()

		b := make([]byte, 0)
		b = builder.AppendBytes(b)
		require.Equal(t, hexutil.MustDecodeHex(
			"0x8F"+
				"00000005"+
				"00000007"+
				"00000009"+
				"0000000B"+
				"0000000D"+
				"0000000F"+
				"00000011"+
				"00000013"+
				"00000015"+
				"00000017"+
				"00000019"+
				"0000001B"+
				"0000001D"+
				"0000001F"+
				"00000021"+
				"00000023"), b)
	})

	t.Run("large sequences must use rebased elias fano", func(t *testing.T) {
		builder := NewBuilder(1000, 17, 1035)
		builder.AddOffset(1005)
		builder.AddOffset(1007)
		builder.AddOffset(1009)
		builder.AddOffset(1011)
		builder.AddOffset(1013)
		builder.AddOffset(1015)
		builder.AddOffset(1017)
		builder.AddOffset(1019)
		builder.AddOffset(1021)
		builder.AddOffset(1023)
		builder.AddOffset(1025)
		builder.AddOffset(1027)
		builder.AddOffset(1029)
		builder.AddOffset(1031)
		builder.AddOffset(1033)
		builder.AddOffset(1035)
		builder.AddOffset(1037)
		builder.Build()

		b := make([]byte, 0)
		b = builder.AppendBytes(b)
		require.Equal(t, b[0], byte(0x90), "encoding type is not 0x90")

		ef, _ := eliasfano32.ReadEliasFano(b[1:])
		require.Equal(t, uint64(17), ef.Count())
		curr := uint64(5)
		for it := ef.Iterator(); it.HasNext(); {
			n, err := it.Next()

			require.NoError(t, err)
			require.Equal(t, curr, n)

			curr += 2
		}
	})
}

func TestBuilderReset(t *testing.T) {
	var b SequenceBuilder

	addAll := func(vals []uint64) {
		for _, v := range vals {
			b.AddOffset(v)
		}
	}
	check := func(t *testing.T, baseNum uint64, vals []uint64) {
		t.Helper()
		raw := b.AppendBytes(nil)
		s := ReadMultiEncSeq(baseNum, raw)
		require.Equal(t, uint64(len(vals)), s.Count())
		for i, want := range vals {
			require.Equal(t, want, s.Get(uint64(i)), "index %d", i)
		}
	}

	small := []uint64{1001, 1003, 1005} // 3 elements → simple encoding
	large := make([]uint64, 17)         // 17 elements → rebased EF
	for i := range large {
		large[i] = 2000 + uint64(i)*2
	}

	t.Run("small then large: rebasedEf allocated on demand", func(t *testing.T) {
		b.Reset(1000, uint64(len(small)), small[len(small)-1])
		addAll(small)
		b.Build()
		require.Equal(t, byte(SimpleEncoding)|byte(len(small)-1), b.AppendBytes(nil)[0])
		check(t, 1000, small)

		b.Reset(2000, uint64(len(large)), large[len(large)-1])
		addAll(large)
		b.Build()
		require.Equal(t, byte(RebasedEliasFano), b.AppendBytes(nil)[0])
		check(t, 2000, large)
	})

	t.Run("large then small: rebasedEf nilled, smallCount zeroed", func(t *testing.T) {
		b.Reset(2000, uint64(len(large)), large[len(large)-1])
		addAll(large)
		b.Build()
		require.Equal(t, byte(RebasedEliasFano), b.AppendBytes(nil)[0])

		b.Reset(1000, uint64(len(small)), small[len(small)-1])
		addAll(small)
		b.Build()
		require.Equal(t, byte(SimpleEncoding)|byte(len(small)-1), b.AppendBytes(nil)[0])
		check(t, 1000, small)
	})

	t.Run("small then small: no stale smallCount", func(t *testing.T) {
		small2 := []uint64{3001, 3003}
		b.Reset(3000, uint64(len(small2)), small2[len(small2)-1])
		addAll(small2)
		b.Build()
		check(t, 3000, small2)

		b.Reset(3000, uint64(len(small2)), small2[len(small2)-1])
		addAll(small2)
		b.Build()
		check(t, 3000, small2)
	})

	t.Run("large then large: rebasedEf reused", func(t *testing.T) {
		large2 := make([]uint64, 17)
		for i := range large2 {
			large2[i] = 5000 + uint64(i)*3
		}
		b.Reset(2000, uint64(len(large)), large[len(large)-1])
		addAll(large)
		b.Build()
		ef1 := b.rebasedEf

		b.Reset(5000, uint64(len(large2)), large2[len(large2)-1])
		addAll(large2)
		b.Build()
		require.Same(t, ef1, b.rebasedEf, "rebasedEf should be reused")
		check(t, 5000, large2)
	})
}

func BenchmarkBuilder(b *testing.B) {
	const baseNum = 1_000_000
	const n = 500

	vals := make([]uint64, n)
	for i := range vals {
		vals[i] = baseNum + uint64(i)*2
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sb := NewBuilder(baseNum, n, vals[n-1])
		for _, v := range vals {
			sb.AddOffset(v)
		}
		sb.Build()
		_ = sb.AppendBytes(nil)
	}
}

// TestBuilderRoundTrip verifies that serialized output can be read back correctly via
// SequenceReader for both encoding paths, including the direct-rebased-EF path (count > 16).
func TestBuilderRoundTrip(t *testing.T) {
	check := func(t *testing.T, baseNum uint64, vals []uint64) {
		t.Helper()
		raw := buildTestSeq(baseNum, vals...)
		s := ReadMultiEncSeq(baseNum, raw)
		require.Equal(t, uint64(len(vals)), s.Count())
		require.Equal(t, vals[0], s.Min())
		require.Equal(t, vals[len(vals)-1], s.Max())
		for i, want := range vals {
			require.Equal(t, want, s.Get(uint64(i)), "index %d", i)
		}
		var it SequenceIterator
		it.Reset(s, 0)
		for i := 0; it.HasNext(); i++ {
			v, err := it.Next()
			require.NoError(t, err)
			require.Equal(t, vals[i], v, "iterator index %d", i)
		}
	}

	t.Run("boundary: 16 elements uses simple encoding", func(t *testing.T) {
		vals := make([]uint64, 16)
		for i := range vals {
			vals[i] = 5000 + uint64(i)*3
		}
		raw := buildTestSeq(5000, vals...)
		require.Equal(t, byte(SimpleEncoding)|15, raw[0])
		check(t, 5000, vals)
	})

	t.Run("boundary: 17 elements uses rebased EF", func(t *testing.T) {
		vals := make([]uint64, 17)
		for i := range vals {
			vals[i] = 5000 + uint64(i)*3
		}
		raw := buildTestSeq(5000, vals...)
		require.Equal(t, byte(RebasedEliasFano), raw[0])
		check(t, 5000, vals)
	})

	t.Run("large sequence with high baseNum", func(t *testing.T) {
		const baseNum = 1_000_000_000
		vals := make([]uint64, 100)
		for i := range vals {
			vals[i] = baseNum + uint64(i)*7
		}
		raw := buildTestSeq(baseNum, vals...)
		require.Equal(t, byte(RebasedEliasFano), raw[0])
		check(t, baseNum, vals)
	})
}
