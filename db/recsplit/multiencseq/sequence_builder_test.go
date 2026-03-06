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
		builder := NewBuilder(1000, 17, 1037)
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

	for b.Loop() {
		sb := NewBuilder(baseNum, n, vals[n-1])
		for _, v := range vals {
			sb.AddOffset(v)
		}
		sb.Build()
		_ = sb.AppendBytes(nil)
	}
}

// TestMergeRealData reproduces a production crash where mergeSeq (arr1) and
// preSeq (arr2) violate the Merge precondition: s1.Max() <= s2.Min().
// Note: arr2 itself is not sorted (last element 107248350 < 109296394),
// indicating upstream data corruption.
func TestMergeRealData(t *testing.T) {
	t.Skip("known-failing: reproduces production merge crash with unsorted data")
	arr1 := []uint64{100012485, 100013434, 100024539, 100024964, 100040999, 100042192, 100053697, 100054203, 100069363, 100069773, 100079507, 100079881, 100099698, 100100397, 100109270, 100109685, 100115007, 100115271, 100129112, 100129516, 100236035, 100236415, 100236421, 100236732, 100236737, 100237054, 100237062, 100237374, 100237379, 100237739, 100237743, 100238034, 100238040, 100238324, 100238329, 100238616, 100251282, 100251881, 100260390, 100260905, 100271956, 100272391, 100278910, 100279261, 100289883, 100290178, 100298280, 100298542, 100308608, 100309057, 100318964, 100319444, 100329099, 100330100, 100333865, 100334110, 100341655, 100341967, 100354776, 100355163, 100368065, 100368392, 100376508, 100377273, 100388900, 100389193, 100396022, 100396293, 100404240, 100404554, 100412531, 100412825, 100421119, 100421355, 100429389, 100429673, 100440138, 100440414, 100448748, 100449041, 100457062, 100457372, 100465032, 100465374, 100476280, 100476783, 100488789, 100489152, 100500444, 100500858, 100509704, 100510042, 100522389, 100522817, 100533105, 100533440, 100543525, 100543985, 100552687, 100553024, 100561964, 100562306, 100571066, 100571408, 100579935, 100580276, 101484164, 101484182, 101484577, 101484588, 101484611, 101770124, 101770135, 101770174, 101770345, 101770363, 101986304, 101986316, 101986339, 101986348, 101987512, 103448853, 103449480, 103449503, 103450889, 103450916, 103450942, 103450957, 103451325, 103451377, 103451396, 103451746, 103454137, 103454573, 103455034, 103455454, 103455507, 103455991, 103456019, 103456407, 103458489, 103458988, 103459020, 103460051, 103460077, 103460479, 103463511, 103464833, 103465692, 103467511, 103467537, 103468838, 103470142, 103470963, 103471045, 103471610, 103471648, 103472317, 103473037, 103473930, 103473975, 103478099, 103479232, 103479976, 103480060, 103480784, 103480801, 103481982, 103482045, 103484036, 103484084, 103484674, 103485760, 103486202, 103486256, 103486752, 103486796, 103487303, 103488135, 103488786, 103488812, 103489404, 103489465, 103490002, 103490021, 103490581, 103491712, 103492254, 103492367, 103492831, 103492874, 103493567, 103494112, 103494521, 103494540, 103495103, 103495141, 103495613, 103497346, 103497778, 103497855, 103498361, 103498378, 103498814, 103499682, 103500196, 103500781, 103501254, 103501282, 103501750, 103501791, 103502304, 103502346, 103502883, 103502934, 103503490, 103503592, 103504237, 103505098, 103505642, 103505718, 103506232, 103506256, 103506854, 103506886, 103507346, 103507863, 103508327, 103508360, 103508820, 103509239, 103509607, 103509641, 103510147, 103510168, 103510811, 103510835, 103511278, 103511778, 103512213, 103512229, 103512609, 103513032, 103513393, 103513459, 103513837, 103513852, 103514280, 103514309, 103514707, 103515565, 103516110, 103516206, 103517058, 103517079, 103517541, 103517658, 103518056, 103518099, 103518521, 103518576, 103519186, 103519206, 103519608, 103519630, 103520017, 103520432, 103520833, 103520869, 103521449, 103522021, 103523502, 103523516, 103523926, 103523975, 103524367, 103524395, 103813611, 103813641, 103814275, 103814332, 103814788, 103814803, 103815284, 103815314, 103967288, 103967310, 103967335, 103967801, 103967829, 103968279, 103968325, 103968839, 103968856, 104898432, 104898461, 104898476, 104898910, 104898942, 104898960, 104899518, 104899557, 104900349, 104900363, 104900903, 104901025, 104901502, 104901684, 104902158, 104902203, 104902751, 104902791, 104903320, 104903332, 104905253, 104908572, 104909223, 104913492, 104914058, 105354869, 105356650, 105356692, 105358675, 106647546, 106647576, 108190102, 108190143, 108391205, 108391223, 108391239, 108391256, 109615141, 109615147, 109615152, 110749727, 110749741, 110749756, 110749769, 110749782, 110749796, 111919761, 111919779, 111919796, 111919810, 113037587, 113037598, 113037612, 113037634, 114812835, 114814071, 115108766, 115552473, 115552484, 115552503, 115976155, 115976182, 116412506, 116412517, 116907762, 116907797, 117337622, 117337635, 117337648, 117780124, 117780153, 118233962, 118233983, 118233998, 118746926, 118746939, 118746951, 119205663, 119205710, 119205741, 119521791, 119923845, 119923855, 119923870, 120355778, 120355785, 120355800, 120355830, 120355841, 120355859, 120818423, 120818450, 120818480, 121253112, 121253131, 121253154, 121253187, 121253201, 121253218, 122934402, 122934428, 122934459, 122934478, 122934510, 122947721, 122947732, 122947748, 123311574, 123311595, 123311611, 123760352, 123760373, 124248074, 124248094, 124248112, 124765779, 124765848}
	arr2 := []uint64{106529195, 106529221, 106529261, 107099718, 107099781, 107099859, 107308287, 107308299, 108030021, 108030049, 108617226, 108617250, 108617273, 109296365, 109296394, 107248350}

	// Build s1 from arr1
	baseNum1 := arr1[0] // use first element as base approximation
	s1Seq := buildTestSeq(baseNum1, arr1...)
	var s1 SequenceReader
	s1.Reset(baseNum1, s1Seq)

	// Build s2 from arr2
	baseNum2 := arr2[0]
	s2Seq := buildTestSeq(baseNum2, arr2...)
	var s2 SequenceReader
	s2.Reset(baseNum2, s2Seq)

	// Merge
	var builder SequenceBuilder
	outBaseNum := min(arr1[0], arr2[0])
	err := builder.Merge(&s1, &s2, outBaseNum)
	require.NoError(t, err)

	// Verify merged result
	raw := builder.AppendBytes(nil)
	merged := ReadMultiEncSeq(outBaseNum, raw)

	// Build expected sorted result
	expected := make([]uint64, 0, len(arr1)+len(arr2))
	expected = append(expected, arr1...)
	expected = append(expected, arr2...)

	require.Equal(t, uint64(len(expected)), merged.Count())

	var it SequenceIterator
	it.Reset(merged, 0)
	i := 0
	for it.HasNext() {
		v, err := it.Next()
		require.NoError(t, err)
		require.Equal(t, expected[i], v, "index %d", i)
		i++
	}
	require.Equal(t, len(expected), i)
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
