package types

import (
	"encoding/binary"
	"testing"
)

func BenchmarkBloomHashing(b *testing.B) {
	for _, fresh := range []bool{false, true} {
		name := "repeated"
		if fresh {
			name = "fresh"
		}
		b.Run(name, func(b *testing.B) {
			var bloom Bloom
			var key [32]byte
			var counter uint64
			b.ReportAllocs()
			for b.Loop() {
				if fresh {
					counter++
					binary.BigEndian.PutUint64(key[:8], counter)
				}
				bloom.Add(key[:])
			}
		})
	}
}
