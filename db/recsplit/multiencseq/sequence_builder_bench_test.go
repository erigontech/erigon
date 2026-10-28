package multiencseq

import (
	"testing"
)

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
