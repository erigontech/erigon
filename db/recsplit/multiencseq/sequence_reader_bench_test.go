package multiencseq

import (
	"testing"
)

func BenchmarkMerge(b *testing.B) {
	const baseNum = 1_000_000
	const n = 500 // elements per sequence

	raw1 := func() []byte {
		sb := NewBuilder(baseNum, n, baseNum+n*2-2)
		for i := range uint64(n) {
			sb.AddOffset(baseNum + i*2)
		}
		sb.Build()
		return sb.AppendBytes(nil)
	}()
	raw2 := func() []byte {
		sb := NewBuilder(baseNum, n, baseNum+n*2+n*2-2)
		for i := range uint64(n) {
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
