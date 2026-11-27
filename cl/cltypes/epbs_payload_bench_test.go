package cltypes

import "testing"

func BenchmarkExecutionPayloadBidCopiedHashSSZ(b *testing.B) {
	source := executionPayloadBidWithCommitments(6)
	if _, err := source.HashSSZ(); err != nil {
		b.Fatal(err)
	}
	for _, clone := range []bool{false, true} {
		name := "copy"
		target := source.Copy()
		if clone {
			name = "clone"
			target = source.Clone().(*ExecutionPayloadBid)
		}
		b.Run(name, func(b *testing.B) {
			if _, err := target.HashSSZ(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := target.HashSSZ(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
